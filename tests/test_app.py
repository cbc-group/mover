from mover import Mover
from mover.worker import Worker

# mover = Mover()
# mover.start()

worker = Worker()
worker.set_source("C:/Users/Andy/Desktop/test_ground/src")
worker.set_destination("C:/Users/Andy/Desktop/test_ground/dst")
worker.set_number_of_backlogs(5)

worker.start()

input("Press Enter to stop...")

worker.stop()