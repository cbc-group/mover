import logging
import signal
import threading
import time
from functools import partial

import click
import coloredlogs

from mover import Mover
from mover.worker import Worker

coloredlogs.install(
    level="DEBUG", fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
)

logger = logging.getLogger(__name__)


def main():
    # mover = Mover()
    # mover.start()

    worker = Worker(timeout=10)
    worker.src = "C:/Users/Andy/Desktop/test_ground/src"
    worker.dst = "C:/Users/Andy/Desktop/test_ground/dst"
    worker.threshold = 5

    worker.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
