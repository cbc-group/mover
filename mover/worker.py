import logging
import os

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

__all__ = ["Worker"]

logger = logging.getLogger(__name__)


class NewFileEventHandler(FileSystemEventHandler):
    def __init__(self, threshold, source, destination):
        self._files = []
        self._threshold = threshold
        self._source, self._destination = source, destination

    def on_created(self, event):
        if event.is_directory:
            # create it but do nothing
            dst_dir = self._get_destination_path(event.src_path)
            logger.debug(f'mkdir "{dst_dir}"')
            os.makedirs(dst_dir)
        else:
            self._files.append(event.src_path)

            if len(self._files) >= self._threshold:
                # trigger move file
                src_path = self._files.pop(0)
                dst_path = self._get_destination_path(src_path)
                logger.debug(f'mv "{dst_path}"')
                os.rename(src_path, dst_path)

    ##

    def _get_destination_path(self, path):
        rel_path = os.path.relpath(path, self._source)
        abs_path = os.path.join(self._destination, rel_path)
        return abs_path


class Worker(object):
    def __init__(self):
        self._source, self._destination = None, None
        self._n_backlogs = 10

        self._observer = Observer()

    ##

    @property
    def can_run(self):
        return self._source is not None and self._destination is not None

    @property
    def is_running(self):
        return self._observer.is_alive()

    ##

    def start(self):
        handler = NewFileEventHandler(self._n_backlogs, self._source, self._destination)
        self._observer.schedule(handler, self._source, recursive=True)
        self._observer.start()

    def stop(self):
        if self.is_running:
            self._observer.stop()
            self._observer.join()

        # cleanup
        self._observer.unschedule_all()

    ##

    def set_source(self, path):
        resume = self.is_running
        self.stop()

        logger.info(f'update source to "{path}"')
        if os.listdir(path):
            logger.error(f"source directory is not empty, abort")
            self._source = None
            return
        self._source = path

        if resume:
            self.start()

    def set_destination(self, path):
        resume = self.is_running
        self.stop()

        logger.info(f'update destination to "{path}"')
        self._destination = path

        if resume:
            self.start()

    def set_number_of_backlogs(self, n):
        resume = self.is_running
        self.stop()

        self._n_backlogs = n

        if resume:
            self.start()
