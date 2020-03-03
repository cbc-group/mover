import logging

from .systray import SystrayIcon
from .worker import Worker

__all__ = ["Mover"]

logger = logging.getLogger(__name__)


class Mover(object):
    def __init__(self):
        self.worker = Worker()
        self.systray = SystrayIcon(Worker)

    ##

    def start(self):
        self.systray.start()
