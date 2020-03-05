import logging
import os
import shutil
import threading
import time
from queue import SimpleQueue

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

__all__ = ["Worker"]

logger = logging.getLogger(__name__)


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, mq: SimpleQueue):
        self._mq = mq

    def on_created(self, event):
        logger.debug(f'new event "{os.path.basename(event.src_path)}"')
        self._mq.put(event)


def property_guard(func):
    """Ensure the loop is stopped when modifying the property."""

    def wrapper(self, *args):
        resume = False
        if self.is_running:
            logger.debug("pausing the loop")
            self.stop()
        func(self, *args)
        if resume:
            logger.debug("resuming the loop")
            self.start()

    return wrapper


class Worker(object):
    """
    Worker monitors files in the source directory and moves it to the destination directory.

    Args:
        threshold (int, optional): backlog threshold
        timeout (int, optional): worker terminates itself after timeout, in seconds
    """

    def __init__(self):
        self._src, self._dst = None, None
        self._threshold = -1
        self._timeout = 0

        # observer monitors the source directory
        self._observer = Observer()
        self._mq = SimpleQueue()
        # mover is the actual work horse
        self._flush_queue_event = threading.Event()
        self._stop_mover_event = threading.Event()
        self._mover = threading.Thread(target=self._mover, name="mover")
        # watchdog determine if worker has been idle for too long
        self._t0 = -1  # internal timestamp book-keeping
        self._kill_watchdog_event = threading.Event()
        self._watchdog = threading.Thread(target=self._watchdog, name="watchdog")

    ##

    @property
    def threshold(self) -> int:
        return self._threshold

    @threshold.setter
    @property_guard
    def threshold(self, threshold: int):
        assert threshold > 0, "backlog threshold should >= 0"
        self._threshold = threshold

    @property
    def timeout(self) -> int:
        return self._timeout

    @timeout.setter
    @property_guard
    def timeout(self, timeout: int):
        assert timeout >= 0, "timeout should >= 0"
        self._timeout = timeout

    @property
    def src(self) -> str:
        return self._src

    @src.setter
    @property_guard
    def src(self, path: str):
        self._src = path

    @property
    def dst(self) -> str:
        return self._dst

    @dst.setter
    @property_guard
    def dst(self, path: str):
        self._dst = path

    ##

    @property
    def is_running(self) -> bool:
        return self._mover.is_alive()

    @property
    def is_valid(self) -> bool:
        return self.src is not None and self.dst is not None

    ##

    def start(self):
        """Start the worker activity."""
        assert self.src is not None, "src dir not specified"
        assert self.dst is not None, "dst dir not specified"
        assert self.threshold >= 0, "backlog threshold not specified"

        # push everything to the queue
        logger.info("populating src dir content")
        scan_queue = [self.src]
        while scan_queue:
            src_dir = scan_queue.pop(0)
            logger.debug(f'scanning "{src_dir}"')
            with os.scandir(src_dir) as it:
                for entry in it:
                    if entry.is_dir():
                        event_class = DirCreatedEvent
                        scan_queue.append(entry.path)
                    else:
                        event_class = FileCreatedEvent
                    event = event_class(entry.path)
                    logger.debug(f'new event "{os.path.basename(entry.path)}"')
                    self._mq.put(event)

        # set observer
        self._observer.schedule(FileEventHandler(self._mq), self.src, recursive=True)
        self._observer.start()

        # set mover
        self._flush_queue_event.clear()
        self._stop_mover_event.clear()
        self._mover.start()

        # set watchdog
        self._kill_watchdog_event.clear()
        self._watchdog.start()

    def stop(self):
        """Stop the worker activity."""
        # stop watchdog
        self._kill_watchdog_event.set()
        self._watchdog.join()

        # stop observer
        self._observer.stop()
        self._observer.join()
        self._observer.unschedule_all()

        # stop mover
        self._flush_queue_event.set()
        self._stop_mover_event.set()
        self._mover.join()

        # cleanup directories
        logger.info(f"remove empty directories")
        scan_queue, rm_queue = [self.src], []
        while scan_queue:
            src_dir = scan_queue.pop(0)
            logger.debug(f'scanning "{src_dir}"')
            with os.scandir(src_dir) as it:
                is_empty = True
                for entry in it:
                    if entry.is_dir():
                        scan_queue.append(entry.path)
                    else:
                        is_empty = False

                if not is_empty:
                    logger.error(f'"{src_dir}" is not entirely empty')
                else:
                    rm_queue.append(src_dir)
        # remove all the folders in rm_queue, in backward (deeper)
        # NOTE still keep the src dir
        for src_dir in reversed(rm_queue[1:]):
            os.rmdir(src_dir)

    ##

    def _mover(self):
        logger.debug("mover started")
        while True:
            n_events = 0
            if self._mq.qsize() > self.threshold:
                n_events = self._mq.qsize() - self.threshold
            if self._flush_queue_event.is_set():
                n_events = self._mq.qsize()
                if n_events > 0:
                    logger.info(f"flushing remaining {n_events} event(s) in the queue")
            events = [self._mq.get(block=False) for _ in range(n_events)]

            if events or self._flush_queue_event.is_set():
                # pet the watch dog if:
                #   - we have some events to process
                #   - acknowledge watchdog timeout
                self._flush_queue_event.clear()
                self._pet_watchdog()

            for event in events:
                # generate target path
                rel_path = os.path.relpath(event.src_path, self.src)
                dst_path = os.path.join(self.dst, rel_path)

                filename = os.path.basename(dst_path)
                try:
                    if event.is_directory:
                        # create directory
                        logger.debug(f'mkdir "{filename}"')
                        try:
                            os.makedirs(dst_path)
                        except FileExistsError:
                            logger.warning(f'"{filename}" exists')
                    else:
                        # move file
                        logger.debug(f'mv "{filename}"')
                        try:
                            shutil.move(event.src_path, dst_path)
                        except PermissionError:
                            logger.error(f'"{filename}" blocked, please extend timeout')
                            # requeue the file and attempt later
                            self._mq.put(event)
                except FileNotFoundError:
                    logger.error(f'"{filename}" was moved after being monitored')
                except Exception as err:
                    logger.exception(f'unable to handle "{err.__class__.__name__}"')

            if self._stop_mover_event.is_set():
                if self._mq.empty():
                    break
                # if not empty, run one more time
        logger.debug("mover stopped")

    ##

    def _watchdog(self, update_rate=1):
        """
        The watchdog activity.

        Args:
            update_rate (int, optional): watchdog update interval, in seconds
        """
        self._pet_watchdog()

        logger.debug("watchdog started")
        while True:
            if self._kill_watchdog_event.wait(update_rate):
                break

            dt = time.time() - self._t0
            if self._timeout > 0 and dt > self._timeout:
                logger.debug(f"watchdog timeout, bark")
                self._flush_queue_event.set()
        logger.debug("watchdog stopped")

    def _pet_watchdog(self):
        self._t0 = time.time()
