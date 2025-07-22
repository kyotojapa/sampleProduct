""" Module to run tests on input unit functionality"""

import random
import socket
import time
import unittest
from pathlib import Path
from unittest import mock

from config_manager.models import GeneralConfig, InputUnitType
from input_unit.input import InputUnit
from input_unit.input_data_store import MessageType
from input_unit.shared_data import DecoderErrorScheme, PurgeScheme
from tests.input_unit.inp_unit_tests_helper import (
    close, connect, send_file_data)

# pylint: disable=no-member


class TestInputUnit(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        startup_cfg_dict = {
            "General": GeneralConfig(
                listening_port=5677,
                listening_address="0.0.0.0",
                input_buffer_chunk_size=4096,
                input_buffer_size=32768,
                input_buffer_purge_scheme=PurgeScheme.DISABLED,
                input_buffer_error_scheme=DecoderErrorScheme.REPLACE,
                queue_max_items_metrics=1000,
                queue_max_items_logs=1000,
                cycle_queue_metrics=True,
                cycle_queue_logs=True,
                enabled_input_unit=InputUnitType.TCP_LISTENER
            )
        }
        cls.ip_addr = "0.0.0.0"
        cls.port = 5677
        cls.run_times = "2"
        cls.input_unit = InputUnit(startup_cfg_dict)
        cls.input_unit.start()

    @classmethod
    def tearDownClass(cls):
        time.sleep(2.0)

    def test_connect_disconnect(self):
        """Method that connects and then disconnects
        from the input unit listener and checks if listener
        has restarted"""

        number_of_runs = int(self.run_times)
        while number_of_runs > 0:

            sock, mysel = connect(TestInputUnit.ip_addr,
                                  int(TestInputUnit.port))
            with open(Path(__file__).parents[2] / "tests" / "input_unit" / "metric_data.txt",
                      mode='rt', encoding='utf-8') as file_pointer:
                chunk = file_pointer.readlines()
                if chunk != []:
                    sock.sendall(bytearray(chunk[1], 'utf-8'))
            close(sock, mysel)
            time.sleep(2.0)
            number_of_runs -= 1

        self.assertEqual(self.input_unit.tcp_listener.is_alive(),
                         True)

    def test_data_sending_metric_complete(self):
        """Method that connects to listener and checks if
        all metric data has been received
        """
        self.input_unit.data_store.clear_all()
        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "metric_data.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(500, 1400)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))

        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(5.0)
        self.assertEqual(
            TestInputUnit.input_unit.data_store.get_queue_size(MessageType.METRIC),
            999)

    def test_data_sending_metric_incomplete(self):
        """Method that connects to listener and checks if
        all metric data has been received using incomplete log
        """
        self.input_unit.data_store.clear_all()
        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "metric_data_incomplete.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(500, 1400)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))
        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(5.0)

        self.assertEqual(
            TestInputUnit.input_unit.data_store.get_queue_size(
                MessageType.METRIC), 997)

    def test_data_sending_full_log(self):
        """Method that connects to listener and checks if
        all metric data has been received
        """

        self.input_unit.data_store.clear_all()
        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "log_data.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(200, 300)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))
        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(4.0)

        self.assertEqual(
            TestInputUnit.input_unit.data_store.get_queue_size(
                MessageType.LOG),
            998)

    def test_data_sending_log_inc(self):
        """Method that connects to listener and checks if
        all metric data has been received using incomplete log
        """

        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "log_data_incomplete.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(200, 300)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))
        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(5.0)

        self.assertEqual(
            TestInputUnit.input_unit.data_store.get_queue_size(
                MessageType.LOG),
            1000)

    def test_get_all_metric_data(self):
        """Method that tries to retrieve all metric data stored in previous
        test"""

        self.input_unit.data_store.clear_all()
        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "metric_data_incomplete.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(500, 1400)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))
        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(5.0)
        metric_data_counter = 0
        while (self.input_unit.data_store.get_queue_size(
                MessageType.METRIC) > 0):
            TestInputUnit.input_unit.data_store.get_message(MessageType.METRIC)
            metric_data_counter += 1

        self.assertEqual(metric_data_counter, 997)

    def test_get_all_log_data(self):
        """Method that tries to retrieve all log data stored in previous
        test"""

        self.input_unit.data_store.clear_all()
        sock, mysel = connect(TestInputUnit.ip_addr, int(TestInputUnit.port))
        keep_reading = True
        with open(Path(__file__).parents[2] / "tests" / "input_unit" / "log_data_incomplete.txt",
                  mode='rt', encoding='utf-8') as file_pointer:
            while keep_reading:
                number_of_bytes_to_read = random.randint(200, 300)
                chunk = file_pointer.read(number_of_bytes_to_read)
                if chunk == '':
                    keep_reading = False
                else:
                    sock.sendall(bytearray(chunk, 'utf-8'))
        time.sleep(0.5)
        close(sock, mysel)
        time.sleep(5.0)

        log_data_counter = 0
        while self.input_unit.data_store.get_queue_size(
                MessageType.LOG) > 0:
            TestInputUnit.input_unit.data_store.get_message(MessageType.LOG)
            log_data_counter += 1

        self.assertEqual(log_data_counter, 998)

    @mock.patch("input_unit.input.InputDataProcessor")
    def test_get_data_processor(self, mock_proc):
        """Method that tries to check get_data_processor function of
        input unit"""

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        idp = TestInputUnit.input_unit.get_data_processor(sock)
        mock_proc.assert_called_with(sock, self.input_unit.data_store,
                                     self.input_unit.general_cfg)
        self.assertNotEqual(idp, None)
        self.assertTrue(idp.daemon, True)

    def test_drop_all_clients(self):
        """Method that tries to check drop_all_clients function of
        input unit"""

        with mock.patch.object(self.input_unit,
                               "tcp_listener") as list_mock:
            list_mock.is_alive.return_value = True

            TestInputUnit.input_unit.drop_all_clients()

            list_mock.is_alive.assert_called_with()
            list_mock.drop_clients.assert_called_with()

