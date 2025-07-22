"""Module containing a data store of queues for the data coming in. This is a
class that provides queues for adding items that will eventually be processed
by the data processing unit"""

from typing import Dict
import product_common_logging as logging
from input_unit.input_queues import InputQueue
from input_unit.shared_data import MessageType
from config_manager.models import GeneralConfig


class InputDataStore:
    """Class that holds the queue buffers for the metric and log data"""

    def __init__(self, config: GeneralConfig):
        """Method to initialise a list with all queue types"""

        self.queues: Dict[MessageType, InputQueue] = {
            MessageType.LOG: InputQueue(
                config.queue_max_items_logs, config.cycle_queue_logs
            ),
            MessageType.METRIC: InputQueue(
                config.queue_max_items_metrics, config.cycle_queue_metrics
            ),
        }

    def put_message(self, message_type: MessageType, message):
        """Method that looks if there is a queue that holds messages for a
        specific message_type contained in the prod1_types list. If so
        it pushes the message in the relevant queue. If cycle_queue flag is
        set and max size is reached it pops the first message in. It
        can raise exceptions"""

        queue = self.queues.get(MessageType[message_type.name], None)
        if not queue:
            logging.warning(
                "Could not find queue type to store. Dropping message")
            return

        if queue.get_size() >= queue.max_size:
            if not queue.cycle_queue:
                logging.warning(
                    "Max size reached cannot store any more messages. "
                    "Dropping message"
                )
                return

            p_msg = queue.deque_message()
            self.log_que_add_del_op(queue, p_msg, message)

        queue.enque_message(message)

    def log_que_add_op(self, queue: InputQueue, message):
        """Method that logs addition of element"""

        logging.debug(
            f"Found log type: {queue}. "
            f"Current size: {queue.get_size()}. "
            f"Message time tag: {message['Hd']['Ti']}"
        )

        logging.debug(f"-----Input Message Counter: {queue.messages_in}-----")

    def log_que_add_del_op(self, queue: InputQueue, p_msg, message):
        """Method that logs an element after deletion of another"""

        logging.debug(
            f"Max {queue} queue size reached. "
            f"Popped msg time tag: {p_msg['Hd']['Ti']}. "
            f"New msg time tag: {message['Hd']['Ti']}. "
            f"Log type: {queue}. "
            f"Current size: {queue.get_size()}."
        )

        logging.debug(
            f"----Input Message Counter for queue {queue}:"
            f" {queue.messages_in}----"
        )

    def get_message(self, message_type: MessageType):
        """Method that looks if there is a queue that holds messages for a
        specific message_type contained in the prod1_types list. If so
        it pops a message from the relevant queue otherwise returns None"""

        queue = self.queues.get(message_type, None)
        if not queue:
            raise TypeError(f"No queue found for {message_type} type.")
        return queue.deque_message()

    def get_queue_size(self, message_type: MessageType):
        """Method to get queue size"""

        queue = self.queues.get(message_type, None)
        if not queue:
            raise TypeError(f"No queue found for {message_type} type.")
        return queue.get_size()

    def clear(self, message_type: MessageType):
        """Method to clear queue of given type"""

        queue = self.queues.get(message_type, None)
        if not queue:
            raise TypeError(f"No queue found for {message_type} type.")
        logging.debug(f'cleared queue of type {message_type}')
        return queue.clear()

    def clear_all(self):
        """Method to clear all queues"""
        for message_type, queue in self.queues.items():
            logging.debug(f'cleared queue of type {message_type}')
            queue.clear()
