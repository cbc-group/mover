import logging
import time
import click
import coloredlogs

from .systray import SystrayIcon
from .worker import Worker

__all__ = ["Mover", "cli"]

logger = logging.getLogger(__name__)


class Mover(object):
    def __init__(self):
        self.worker = Worker()
        self.systray = SystrayIcon(Worker)

    ##

    def start(self):
        self.systray.start()


@click.command()
@click.argument("src")
@click.argument("dst")
@click.option("-b", "--backlog", default=5, help="number of backlog files")
@click.option("-t", "--timeout", default=300, help="timeout (seconds)")
@click.option("-v", "--verbose", is_flag=True, help="show debug messages")
def cli(src, dst, backlog, timeout, verbose):
    level = "DEBUG" if verbose else "INFO"
    coloredlogs.install(
        level=level, fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
    )

    worker = Worker()
    worker.src = src
    worker.dst = dst
    worker.threshold = backlog
    worker.timeout = timeout

    worker.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
