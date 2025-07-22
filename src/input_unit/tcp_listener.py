""" Class that sets up a Listener for a TCP socket
This is used as part of the prod1 logs and metrics input unit"""

import datetime
import selectors
import socket
import time
from threading import Thread

import product_common_logging as logging

from input_unit.shared_data import (
    ClientDisconnectedException, EventType, get_events_queue)
from output_unit.metric_storage import MetricStore

DEFAULT_SOCK_OPTIONS = (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


class TcpListener(Thread):
    """Class that sets up a socket, listens for connections"""

    def __init__(self, input_unit):
        """Method to set up the class values using parameters.
        It can raise an exception.
        """
        self.input_unit = input_unit
        self.events_queue = get_events_queue()

        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(*DEFAULT_SOCK_OPTIONS)

        self.data_processors = []
        self.last_socket_disconnect = datetime.datetime.utcnow()
        self.prv_time_socket_discon = (datetime.datetime.utcnow() -
                                       datetime.timedelta(60))
        self.current_connection_status = True

        self.__keep_running = True

        super().__init__()

    def setup_listener(self, host, port):
        """Method that create the socket to listen on it.
        It can raise an exception if the port is already in use"""

        self.socket.bind((host, port))
        self.socket.listen()
        self.socket.setblocking(False)

        logging.info(f"TCP socket configured for {host}:{port}")
        self.selector.register(self.socket, selectors.EVENT_READ, data=None)

    def accept_connection(self):
        """Callback for accepting new connections
        It can raise an exception"""

        conn, client_details = self.socket.accept()
        self.set_sock_options(conn)

        logging.info(f"Accepted connection from {client_details}")
        self.events_queue.put(EventType.ACCEPTED_CLIENT_CONNECTION)

        input_data_proc = self.input_unit.get_data_processor(conn)
        self.selector.register(
            conn, selectors.EVENT_READ, data=input_data_proc)

    def set_sock_options(self, conn, after_idle_sec=1, interval_sec=1,
                         max_fails=5):
        """Set TCP options on an open socket. These options allow the
        reuse of address. They also set keepalive options. The keepalive
        activates after after_idle_sec of idleness, it sends a keepalive
        ping once every interval_sec, and closes the connection
        after max_fails failed ping, or interval_sec*max_fails seconds
        """

        conn.setsockopt(*DEFAULT_SOCK_OPTIONS)
        conn.setblocking(False)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
                        after_idle_sec)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)

    def drop_clients(self):
        """Drop all clients."""
        for dp in self.data_processors:
            self.selector.unregister(dp.active_sock)
            dp.active_sock.shutdown(socket.SHUT_RDWR)
            dp.active_sock.close()
            logging.info("Clients were cleaned up")
            self.set_connection_metric()
        self.data_processors.clear()

    def run(self):
        """Overload method for the thread. Grabbing exceptions
        for other methods"""
        while self.__keep_running:
            events = self.selector.select(timeout=0.1)
            for key, _ in events:
                if key.data is None:
                    self.accept_connection()
                else:
                    try:
                        key.data.process_event()
                    except ClientDisconnectedException as err:
                        if err.socket:
                            try:
                                self.selector.unregister(err.socket)
                                err.socket.shutdown(socket.SHUT_RDWR)
                                err.socket.close()
                                logging.info("Socket was cleaned up")
                            except OSError:
                                logging.warning("Trying to close a closed "
                                                "socket.")
                        self.set_connection_metric()

            time.sleep(0.001)
            
    def set_connection_metric(self):
        """Method to set the metric for connection status"""

        connection_status_retrieved = self.get_connection_status()
        if not connection_status_retrieved:
            if MetricStore():
                MetricStore().set_metric(
                    name="services.hosting.virtual_machine.sdwan_end_point."
                         "agent.assurance.tcp_log_connection_status",
                    value=0,
                    attrs={},
                    use_default_attributes=True)
        self.prv_time_socket_discon = datetime.datetime.utcnow()

    def get_connection_status(self):
        """Method that returns the connection status of the TCP connection"""

        connection_status = True
        if (datetime.timedelta(seconds=3) < datetime.datetime.utcnow() -
                self.prv_time_socket_discon <
                datetime.timedelta(seconds=25)):
            connection_status = False
        return connection_status

    def stop(self):
        """Drop clients and stop."""
        self.drop_clients()
        self.__keep_running = False

    def __del__(self):
        """Cleanup socket and selector."""

        logging.info("Cleaning up socket resources.")

        try:

            self.selector.unregister(self.socket)
            self.selector.close()

        except KeyError:
            logging.warning("Trying to unregister unregistered resource.")

        try:
            self.socket.close()
        except OSError:
            logging.error("Can not close listening socket properly.")
