"""Module containing the base class for the queue messages and the derived
classes for logs and metric data"""

from queue import Queue


class InputQueue(Queue):
    """Class that is the base class used by derived classes"""

    def __init__(self, max_size: int, cycle_queue: bool = False):
        """Method to initialise queue parameters"""

        self.max_size = max_size
        self.cycle_queue = cycle_queue
        self.messages_in = 0
        self.messages_out = 0
        super().__init__(max_size)

    def deque_message(self):
        """Method used to pop data from queue"""

        if not self.empty():
            self.messages_out += 1
            return self.get()
        return None

    def enque_message(self, value):
        """Method used to push data to queue"""

        if not self.full():
            self.messages_in += 1
            self.put(value)

    def get_size(self):
        """Method to get the current queue size"""

        return self.qsize()

    def clear(self):
        """Method to delete all items from queue"""
        with self.mutex:
            self.queue.clear()
