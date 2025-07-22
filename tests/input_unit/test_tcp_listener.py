""" Module to run tests on input unit tcp listener"""

import ipaddress
import selectors
import socket
import time
import unittest
from unittest import mock

import product_common_logging as logs

from config_manager.models import GeneralConfig
from input_unit.input import InputUnit
from input_unit.input_data_processor import InputDataProcessor
from input_unit.shared_data import (
    DecoderErrorScheme, EventType, PurgeScheme, get_events_queue)
from input_unit.tcp_listener import TcpListener

logs.setLevel(logs.ERROR)
# pylint: disable=no-member


class TestTcpListener(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        cls.startup_cfg_dict = {
            "General": GeneralConfig(
                listening_port=5600,
                listening_address="0.0.0.0",
                input_buffer_chunk_size=4096,
                input_buffer_size=32768,
                input_buffer_purge_scheme=PurgeScheme.DISABLED,
                input_buffer_error_scheme=DecoderErrorScheme.REPLACE,
                queue_max_items_metrics=1000,
                queue_max_items_logs=1000,
                cycle_queue_metrics=True,
                cycle_queue_logs=True,
            )
        }
        cls.gen_config = cls.startup_cfg_dict.get("General")
        cls.input_unit = InputUnit(cls.startup_cfg_dict)

    @classmethod
    def tearDownClass(cls):
        time.sleep(2.0)

    def test_tcp_listener_init(self):
        """Method that tests the initialise function of the
        tcp listener"""

        tcp_list = TcpListener(TestTcpListener.input_unit)
        self.assertIsInstance(tcp_list.input_unit, InputUnit)
        self.assertIsInstance(tcp_list.selector,
                              type(selectors.DefaultSelector()))
        self.assertIsInstance(tcp_list.socket,
                              type(socket.socket
                                   (socket.AF_INET, socket.SOCK_STREAM)))
        self.assertIsInstance(tcp_list.data_processors, list)
        self.assertIsInstance(tcp_list.events_queue, type(get_events_queue()))

    @mock.patch("input_unit.tcp_listener.logging")
    def test_tcp_listener_setup(self, mock_logs):
        """Method that tests the setup listener function of the
        tcp listener"""

        tcp_list = TcpListener(TestTcpListener.input_unit)
        with mock.patch.object(tcp_list, "socket") as mock_sock:
            with mock.patch.object(tcp_list, "selector") as mock_sel:
                tcp_list.setup_listener(
                    TestTcpListener.gen_config.listening_address,
                    TestTcpListener.gen_config.listening_port + 1)

                mock_sock.bind.assert_called_with(
                    (TestTcpListener.gen_config.listening_address,
                     TestTcpListener.gen_config.listening_port + 1))
                mock_sock.listen.assert_called_with()
                mock_sock.setblocking.assert_called_with(False)
                mock_sel.register.assert_called_with(mock_sock,
                                                     selectors.EVENT_READ,
                                                     data=None)
                mock_logs.info.assert_called_with(
                    "TCP socket configured for "
                    f"{TestTcpListener.gen_config.listening_address}:"
                    f"{TestTcpListener.gen_config.listening_port + 1}")

                mock_sel.unregister(mock_sock)
                mock_sock.close()

    @mock.patch("input_unit.tcp_listener.logging")
    def test_tcp_listener_accept(self, mock_logs):
        """Method that tests the accept function of the
        tcp listener"""

        tcp_list = TcpListener(TestTcpListener.input_unit)
        tcp_list.events_queue.queue.clear()
        tcp_list.setup_listener(
            TestTcpListener.gen_config.listening_address,
            TestTcpListener.gen_config.listening_port + 2)
        with mock.patch.object(tcp_list, "socket") as mock_sock:
            with mock.patch.object(tcp_list, "selector") as mock_sel:
                with mock.patch.object(tcp_list, "input_unit") as inp_unit:

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    ip_addr = ipaddress.ip_address(
                        TestTcpListener.gen_config.listening_address)

                    mock_sock.accept.return_value = sock, ip_addr
                    data_proc = InputDataProcessor(sock, None,
                                                   TestTcpListener.gen_config)

                    inp_unit.get_data_processor.return_value = data_proc

                    tcp_list.accept_connection()
                    mock_sock.accept.assert_called_with()

                    conn, client_details = mock_sock.accept.return_value
                    data_proc = inp_unit.get_data_processor.return_value
                    mock_logs.info.assert_called_with(
                        f"Accepted connection from {client_details}")

                    self.assertGreaterEqual(tcp_list.events_queue.qsize(), 1)
                    self.assertEqual(
                        str(tcp_list.events_queue.get()),
                        str(EventType.ACCEPTED_CLIENT_CONNECTION))

                    mock_sel.register.assert_called_with(
                        conn,
                        selectors.EVENT_READ,
                        data=data_proc)

                    mock_sel.unregister(sock)
                    sock.close()

    def test_tcp_listener_set_options(self):
        """Method that tests the set options function of the
        tcp listener"""

        tcp_list = TcpListener(TestTcpListener.input_unit)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_list.set_sock_options(sock)

        self.assertEqual(sock.getsockopt(socket.SOL_SOCKET,
                                         socket.SO_KEEPALIVE), 1)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP,
                                         socket.TCP_KEEPIDLE), 1)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP,
                                         socket.TCP_KEEPINTVL), 1)
        self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP,
                                         socket.TCP_KEEPCNT), 5)
        self.assertEqual(sock.getsockopt(socket.SOL_SOCKET,
                                         socket.SO_REUSEADDR), 1)

    def test_tcp_listener_drop_clients(self):
        """Method that tests the drop clients function of the
        tcp listener"""

        tcp_list = TcpListener(TestTcpListener.input_unit)
        tcp_list.setup_listener(
            TestTcpListener.gen_config.listening_address,
            TestTcpListener.gen_config.listening_port + 3)

        with mock.patch.object(tcp_list, "selector") as mock_sel:

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            data_proc = InputDataProcessor(sock, None,
                                           TestTcpListener.gen_config)

            tcp_list.data_processors.append(data_proc)
            time.sleep(1)
            with mock.patch.object(data_proc, "active_sock") as sock_mock:

                tcp_list.drop_clients()
                mock_sel.unregister.assert_called_with(
                    data_proc.active_sock)
                sock_mock.shutdown.assert_called_with(socket.SHUT_RDWR)

            self.assertEqual(len(tcp_list.data_processors), 0)
            mock_sel.unregister(sock)
            sock.close()
