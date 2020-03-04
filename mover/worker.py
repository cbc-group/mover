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
        self._t0 = -1 # internal timestamp book-keeping
        self._kill_watchdog_event = threading.Event()
        self._watchdog = threading.Thread(
            target=self._watchdog, args=(timeout,), name="watchdog"
        )

    ##

    @property
    def threshold(self) -> int:
        return self._threshold

    @threshold.setter
    def threshold(self, threshold: int):
        # TODO  stop and restart the loop if necessary

        assert threshold > 0, "backlog threshold should >= 0"
        self._threshold = threshold

    @property
    def src(self) -> str:
        return self._src

    @property
    def dst(self) -> str:
        return self._dst

    ##

    def start(self):
        """Start the worker activity."""
        with os.scandir(self.src) as it:
            for entry in it:
                if entry.is_file():
                    need_copytree = True
                    break
            else:
                need_copytree = False
        if need_copytree > 0:
            logger.info("found files in src dir, moving the tree")
            copytree(
                self.src,
                self.dst,
                copy_function=lambda src, dst, follow_symlinks: os.rename(src, dst),
            )

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
        self._mover.join()

        # stop observer
        self._observer.stop()
        self._observer.join()
        self._observer.unschedule_all()

    ##

    def _mover(self):
        logger.debug("mover started")
        is_poisoned = False
        while is_poisoned:
            n_events = 0
            if self._mq.qsize() > self.threshold:
                n_events = self._mq.qsize() - self.threshold
            if self._flush_queue_event:
                self._flush_queue_event.clear()
                n_events = self._mq.qsize()
                logger.info(f"flushing remaining {n_events} event(s) in the queue")
            events = [self._mq.get() for _ in range(n_events)]

            # we are doing something, pet the dog
            self._pet_watchdog()

            for event in events:
                if event is None:
                    # poison, stop mover
                    is_poisoned = True

                if event.is_dir:
                    # create directory
                    pass
                else:
                    # move file
                    pass
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

        logger.debug("watchdog started")
        while True:
            if self._kill_watchdog_event.wait(update_rate):
                break

            dt = time.time() - self._t0
            if dt > timeout:
                logger.info(f"flushing remaining files due to watchdog timeout")
                self._flush_queue_event.set()
        logger.debug("watchdog stopped")

    def _pet_watchdog(self):
        self._t0 = time.time()
        logger.debug(f"watchdog pet at t:{self._t0}")
