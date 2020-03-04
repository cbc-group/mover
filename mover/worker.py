import logging
import os
import time
import threading
import queue
from queue import SimpleQueue
from shutil import copytree
from dataclasses import dataclass

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

__all__ = ["Worker"]

logger = logging.getLogger(__name__)


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, mq: SimpleQueue):
        self._mq = mq

    def on_created(self, event):
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


def move_tree(src, dst):
    """
    The standard shutil.copytree causes error when root folder exists.

    Reference:
        How do I copy an entire directory of files into an existing directory using     
            Python? https://stackoverflow.com/a/12514470
    """
    # iterate over items in the top level folder
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            copytree(s, d, copy_function=os.rename)
        else:
            os.rename(s, d)


class Worker(object):
    """
    Worker monitors files in the source directory and moves it to the destination directory.

    Args:
        threshold (int, optional): backlog threshold
        timeout (int, optional): worker terminates itself after timeout, in seconds
    """

    def __init__(self, timeout=300):
        self._src, self._dst = None, None
        self._threshold = -1

        # observer monitors the source directory
        self._observer = Observer()
        self._mq = SimpleQueue()
        # mover is the actual work horse
        self._flush_queue_event = threading.Event()
        self._mover = threading.Thread(target=self._mover, name="mover")
        # watchdog determine if worker has been idle for too long
        self._t0 = -1  # internal timestamp book-keeping
        self._kill_watchdog_event = threading.Event()
        self._watchdog = threading.Thread(
            target=self._watchdog, args=(timeout,), name="watchdog"
        )

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

    ##

    def start(self):
        """Start the worker activity."""
        assert self.src is not None, "src dir not specified"
        assert self.dst is not None, "dst dir not specified"
        assert self.threshold >= 0, "backlog threshold not specified"

        scan_queue = [self.src]
        need_copytree = False
        while scan_queue:
            src_dir = scan_queue.pop(0)
            with os.scandir(src_dir) as it:
                for entry in it:
                    if entry.is_file():
                        # wipe the queue and set copy flag
                        scan_queue.clear()
                        need_copytree = True
                        break
                    elif entry.is_dir():
                        scan_queue.append(entry.path)
        if need_copytree:
            logger.info("found files in src dir, moving the tree")
            move_tree(self.src, self.dst)

        # set observer
        self._observer.schedule(FileEventHandler(self._mq), self.src, recursive=True)
        self._observer.start()

        # set mover
        self._flush_queue_event.clear()
        self._mover.start()

        # set watchdog
        self._kill_watchdog_event.clear()
        self._watchdog.start()

    def stop(self):
        """Stop the worker activity."""
        # stop watchdog
        self._kill_watchdog_event.set()
        self._watchdog.join()

        # stop mover
        self._mq.put(None)  # poison pill
        self._flush_queue_event.set()
        self._mover.join()

        # stop observer
        self._observer.stop()
        self._observer.join()
        self._observer.unschedule_all()

    ##

    def _mover(self):
        logger.debug("mover started")
        is_poisoned = False
        while not is_poisoned:
            n_events = 0
            if self._mq.qsize() > self.threshold:
                n_events = self._mq.qsize() - self.threshold
            if self._flush_queue_event.is_set():
                self._flush_queue_event.clear()
                n_events = self._mq.qsize()
                if n_events > 0:
                    logger.info(f"flushing remaining {n_events} event(s) in the queue")
            events = [self._mq.get() for _ in range(n_events)]

            if not events:
                continue
            else:
                # we are going to do something, pet the dog
                self._pet_watchdog()
                logger.info(f"processing {len(events)} event(s)")

            for event in events:
                if event is None:
                    # poison, stop mover
                    is_poisoned = True
                    break

                # generate target path
                rel_path = os.path.relpath(event.src_path, self.src)
                dst_path = os.path.join(self.dst, rel_path)

                if event.is_directory:
                    # create directory
                    logger.debug(f'mkdir "{os.path.basename(dst_path)}"')
                    os.makedirs(dst_path)
                else:
                    # move file
                    logger.debug(f'mv "{os.path.basename(dst_path)}"')
                    os.rename(event.src_path, dst_path)
        logger.debug("mover stopped")

    ##

    def _watchdog(self, timeout: int, update_rate=5):
        """
        The watchdog activity.

        Args:
            timeout (int): timeout in seconds
            update_rate (int, optional): watchdog update interval, in seconds
        """
        self._pet_watchdog()

        print(timeout)
        logger.debug("watchdog started")
        while True:
            if self._kill_watchdog_event.wait(update_rate):
                break

            dt = time.time() - self._t0
            if dt > timeout:
                logger.info(f"watchdog timeout, bark")
                self._flush_queue_event.set()
        logger.debug("watchdog stopped")

    def _pet_watchdog(self):
        self._t0 = time.time()
