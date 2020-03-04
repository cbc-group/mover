import logging
import tkinter as tk
from functools import partial
from tkinter import filedialog

from pystray import Icon, Menu, MenuItem

from .icon import MOVER_ICON
from .worker import Worker

__all__ = ["SystrayIcon"]

logger = logging.getLogger(__name__)


class SystrayIcon(object):
    def __init__(self, worker: Worker):
        # save worker reference
        self._worker = worker

        # create the icon
        self._icon = Icon("Mover")

        # set the image
        self._icon.icon = MOVER_ICON
        # set actions
        #   - set source
        #   - set destination
        #   - number of backlogs
        #   - start/stop
        #   - exit
        self._start_stop = MenuItem(
            "Start", self.on_start_stop, enabled=False
        )  # initial state
        self._set_source = MenuItem("Set Source", self.on_set_source)
        self._set_destination = MenuItem("Set Destination", self.on_set_destination)
        self._number_of_backlogs = MenuItem(
            "Number of Backlogs",
            Menu(
                MenuItem("Immediate", partial(self.on_number_of_backlogs_changed, 0)),
                MenuItem("5", partial(self.on_number_of_backlogs_changed, 5)),
                MenuItem("10", partial(self.on_number_of_backlogs_changed, 10)),
                MenuItem("25", partial(self.on_number_of_backlogs_changed, 25)),
            ),
        )
        self._timeout = MenuItem(
            'Timeout',
            Menu(
                MenuItem('Never', partial(self.on_timeout_changed, 0)),
                MenuItem('30 s', partial(self.on_timeout_changed, 30))
            )
        )
        self._exit = MenuItem("Exit", self.on_exit)
        self._icon.menu = Menu(
            self._start_stop,
            Menu.SEPARATOR,
            self._set_source,
            self._set_destination,
            self._number_of_backlogs,
            Menu.SEPARATOR,
            self._exit,
        )

    ##

    def start(self):
        self._icon.run()

    def stop(self):
        self._icon.stop()

    ##
    def on_start_stop(self):
        # toggle state
        if self._worker.is_running:
            self._worker.stop()
        else:
            self._worker.start()

    def on_set_source(self):
        path = self._ask_folder_path("Please select the source directory")
        #self._worker.set_source(path)

        self._update_start_stop_state()

    def on_set_destination(self):
        path = self._ask_folder_path("Please select the destination directory")
        self._worker.set_destination(path)

        self._update_start_stop_state()

    def on_number_of_backlogs_changed(self, n):
        self._worker.set_number_of_backlogs(n)

    def on_timeout_changed(self, t):
        self._worker.stop()
        self._worker = Worker(t)


    def on_exit(self):
        logger.debug(f"systray stopped")
        self.stop()

    ##

    def _update_start_stop_state(self):
        self._start_stop.enabled = self._worker.can_run
        self._icon.update_menu()

    def _ask_folder_path(self, title="Please select a directory"):
        root = tk.Tk()
        root.withdraw()
        return filedialog.askdirectory(title=title)
