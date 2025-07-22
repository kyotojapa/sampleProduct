"""Class that holds the functional bits of the input unit. It holds
a data store for the data, a data processor and a tcp listener"""

from socket import socket

from config_manager.models import (
    AMQPSubscriberConfig, GeneralConfig, InputUnitType)
from input_unit.amqp_subscriber import AMQPSubscriber
from input_unit.input_data_processor import InputDataProcessor
from input_unit.input_data_store import InputDataStore
from input_unit.shared_data import DEFAULT_IP_ADDRESS, get_events_queue
from input_unit.tcp_listener import TcpListener


class InputUnit:
    """Class that handles initialization and maintenance tasks
    of the input unit."""

    def __init__(self, startup_cfg: dict):

        self.general_cfg = startup_cfg.get("General", GeneralConfig())
        self.amqp_subscriber_cfg = startup_cfg.get(
            "AMQPSubscriber", AMQPSubscriberConfig())
        self.data_store = InputDataStore(self.general_cfg)
        self.events_queue = get_events_queue()
        self.tcp_listener = TcpListener(self)
        self.amqp_subscriber = AMQPSubscriber(self, self.data_store)

    def get_data_processor(self, sock: socket):
        """Create a new Data Processor thread/instance."""
        return InputDataProcessor(sock, self.data_store, self.general_cfg)

    def drop_all_clients(self):
        """Drop active connections."""
        if self.tcp_listener.is_alive():
            self.tcp_listener.drop_clients()

    def start(self):
        """Set TCP listener thread and start listening.

        Raises:
            * OSError: If the desired TCP port is busy.
        """
        if self.general_cfg.enabled_input_unit == InputUnitType.TCP_LISTENER:
            self.tcp_listener.setup_listener(
                DEFAULT_IP_ADDRESS,
                self.general_cfg.listening_port,
            )
            self.tcp_listener.name = self.tcp_listener.__class__.__name__
            self.tcp_listener.daemon = True
            self.tcp_listener.start()

        if (self.general_cfg.enabled_input_unit ==
                InputUnitType.AMQP_SUBSCRIBER):
            self.amqp_subscriber.name = self.amqp_subscriber.__class__.__name__
            self.amqp_subscriber.daemon = True
            self.amqp_subscriber.start()

    def stop(self):
        """Stop running thread."""
        if self.general_cfg.enabled_input_unit == InputUnitType.TCP_LISTENER:
            self.tcp_listener.stop()
        if (self.general_cfg.enabled_input_unit ==
                InputUnitType.AMQP_SUBSCRIBER):
            self.amqp_subscriber.stop()
