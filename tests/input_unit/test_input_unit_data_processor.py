""" Module to run tests on input unit data processor"""

import socket
import unittest
from unittest.mock import patch

from json import loads
import product_common_logging as logs

from config_manager.models import GeneralConfig
from data_processing.models.unified_model import UnifiedModel
from input_unit.input import InputUnit
from input_unit.input_data_processor import InputDataProcessor
from input_unit.input_data_store import InputDataStore
from input_unit.shared_data import (DecoderErrorScheme,
                                    PurgeScheme)
from input_unit.shared_data import ClientDisconnectedException, MessageType

logs.setLevel(logs.ERROR)
# pylint: disable=no-member


class TestInputDataProcessor(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    def setUp(self):
        startup_cfg_dict = {
            "General": GeneralConfig(
                listening_port=5680,
                listening_address="0.0.0.0",
                input_buffer_chunk_size=4096,
                input_buffer_size=32768,
                input_buffer_purge_scheme=PurgeScheme.DISABLED,
                input_buffer_error_scheme=DecoderErrorScheme.STRICT,
                queue_max_items_metrics=1000,
                queue_max_items_logs=1000,
                cycle_queue_metrics=True,
                cycle_queue_logs=True,
            )
        }

        self.gen_config = startup_cfg_dict.get("General")
        self.input_unit = InputUnit(startup_cfg_dict)
        self.log = (
            '{"Hd":{"Ty":"Audit","P":'
            '{"Fi":"lw/shared/prod1/src/tenants/TenantManager.cpp",'
            '"Li":3525},"Ct":"RazorLink.Internal.Razor.Tenants.Verbose",'
            '"Ti":"20230117T141244.696419","Cl":{"ownr":"","oid":"",'
            '"info":"","sinf":"","goid":""},"Fu":"OnConntrackTimer"}, '
            '"txt":""}')
        self.big_log = '{"Hd":{"Ty":"Audit","P": {"Fi":"lw/shared/prod1/src/tenants/TenantManager.cpp", "Fi2":"lw/shared/prod1/src/tenants/TenantManager.cpp", "Fi3":"lw/shared/prod1/src/tenants/TenantManager.cpp","Fi4":"lw/shared/prod1/src/tenants/TenantManager.cpp","Fi5":"lw/shared/prod1/src/tenants/TenantManager.cpp","Fi6":"lw/shared/prod1/src/tenants/TenantManager.cpp", "Li":3525},"Ct":"RazorLink.Internal.Razor.Tenants.Verbose", "Ti":"20230117T141244.696419","Cl":{"ownr":"","oid":"", "info":"","sinf":"","goid":""},"Fu":"OnConntrackTimer"}, "txt":""}'
        self.metric = '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/LoggingObject.cpp","Li":862},"Ct":"Components.RazorLink.snapshot","Ti":"20240215T160110.031609","Cl":{"ownr":"","oid":"<CSCommsReader!1029>","info":"","sinf":"","goid":""},"Fu":"DoSnapshotLog"},"0":"","snapshot":{"sid":26865,"ctx":"","obj":"<CSCommsReader!1029>","val":{"Sta":{"nIf":3,"tTx":0,"tRx":0}}}}'
        self.log_data = loads(self.log)
        self.met_data = loads(self.metric)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.data_store = InputDataStore(self.gen_config)
        self.data_proc = InputDataProcessor(self.sock, self.data_store,
                                            self.gen_config)

    def test_data_proc_init(self):
        """Method that tests the initialise function of the
        input data processor"""

        self.assertEqual(self.data_proc.active_sock, self.sock)
        self.assertEqual(self.data_proc.config,
                         self.gen_config)

        self.assertEqual(self.data_proc.chunk_size, 4096)
        self.assertEqual(self.data_proc.buffer_size, 32768)
        self.assertEqual(self.data_proc.purge_scheme, PurgeScheme.DISABLED)
        self.assertEqual(self.data_proc.buffer, bytearray(
            self.data_proc.buffer_size))
        self.assertEqual(self.data_proc.temp_buffer, bytearray(
            self.data_proc.chunk_size))

    @patch("input_unit.input_data_processor.InputDataProcessor.process_buffer")
    @patch("input_unit.input_data_processor.socket.recv")
    def test_data_proc_process_event_part1(self, recv_mock, proc_buffer_mock):
        "test process event func part1"

        recv_mock.return_value = b'some_value'
        self.data_proc.process_event()
        proc_buffer_mock.assert_called()

    @patch("input_unit.input_data_processor.logging")
    @patch("input_unit.input_data_processor.socket.recv")
    def test_data_proc_process_event_part2(self, recv_mock, logging_mock):
        "test process event func part2"

        recv_mock.return_value = b''
        with self.assertRaises(ClientDisconnectedException):
            self.data_proc.process_event()
            logging_mock.info.assert_called_with('Peer closed.')

    def test_data_proc_process_event_part3(self):
        "test process event func part3"

        with self.assertRaises(ClientDisconnectedException):
            self.data_proc.process_event()

    @patch("input_unit.input_data_processor.InputDataProcessor.process_lines")
    def test_process_buffer_part1(self, pr_line_mock):
        "test process buffer func part1"
        line_data = [bytes(
            'some data', encoding='UTF-8')]
        self.data_proc.temp_buffer = bytearray('some data\n', encoding='UTF-8')
        self.data_proc.buffer = bytearray('', encoding='UTF-8')
        self.data_proc.process_buffer()
        pr_line_mock.assert_called_with(line_data)
        self.assertEqual(self.data_proc.buffer, b'')

    @patch("input_unit.input_data_processor.InputDataProcessor.process_lines")
    def test_process_buffer_part2(self, pr_line_mock):
        "test process buffer func part2"
        line_data = [bytes(
            'some data1', encoding='UTF-8'), bytes(
            'some data2', encoding='UTF-8')]
        self.data_proc.temp_buffer = bytearray('some data1\nsome data2\n', encoding='UTF-8')
        self.data_proc.buffer = bytearray('', encoding='UTF-8')
        self.data_proc.process_buffer()
        pr_line_mock.assert_called_with(line_data)
        self.assertEqual(self.data_proc.buffer, b'')

    @patch("input_unit.input_data_processor.InputDataProcessor.process_lines")
    def test_process_buffer_part3(self, pr_line_mock):
        "test process buffer func part3"
        line_data = [bytes(
            'some data1', encoding='UTF-8'), bytes(
            'some data2', encoding='UTF-8')]
        self.data_proc.temp_buffer = bytearray('some data1\nsome data2\nsome ', encoding='UTF-8')
        self.data_proc.buffer = bytearray('', encoding='UTF-8')
        self.data_proc.process_buffer()
        pr_line_mock.assert_called_with(line_data)
        self.assertEqual(self.data_proc.buffer, b'some ')

    @patch("input_unit.input_data_processor.InputDataProcessor.process_lines")
    def test_process_buffer_part4(self, pr_line_mock):
        "test process buffer func part4"
        line_data = [bytes(
            'some data1', encoding='UTF-8'), bytes(
            'some data2', encoding='UTF-8')]
        self.data_proc.temp_buffer = bytearray(' data1\nsome data2\nsome ', encoding='UTF-8')
        self.data_proc.buffer = bytearray('some', encoding='UTF-8')
        self.data_proc.process_buffer()
        pr_line_mock.assert_called_with(line_data)
        self.assertEqual(self.data_proc.buffer, b'some ')

    def test_get_message_type(self):
        """Method that tests the get message type function of the
        input data processor"""

        self.assertEqual(self.data_proc.get_message_type(
            self.log_data), MessageType.LOG)

        self.assertEqual(self.data_proc.get_message_type(
            self.met_data), MessageType.METRIC)

        dummy_data = 322
        self.assertEqual(self.data_proc.get_message_type(dummy_data),
                         MessageType.UNKNOWN)

    def test_decode_data_part1(self):
        """Method that tests the get decode data part1 function of the
        input data processor"""

        res = self.data_proc.decode_data(
            bytearray(self.metric, encoding='UTF-8'))
        self.assertEqual(res, self.metric)

    @patch("input_unit.input_data_processor.logging")
    def test_decode_data_part2(self, log_mock):
        """Method that tests the get decode data part2 function of the
        input data processor"""

        data = bytearray(b'\xff\x91')
        res = self.data_proc.decode_data(data)
        log_mock.error.assert_called()
        self.assertEqual(res, "")

    @patch("input_unit.input_data_processor.logging")
    def test_handle_json_parse_success_part1(self, log_mock):
        """Method that tests the handle json parse success part1 function of
        the input data processor"""

        self.data_proc.handle_json_parse_success(None, "")
        log_mock.debug.assert_called()

    @patch("input_unit.input_data_processor.InputDataStore.put_message")
    def test_handle_json_parse_success_part2(self, put_msg_mock):
        """Method that tests the handle json parse success part2 function of
        the input data processor"""

        self.data_proc.handle_json_parse_success(
            MessageType.LOG, {"a_key": "a_value"})
        put_msg_mock.assert_called()

    @patch("input_unit.input_data_processor.logging")
    def test_handle_process_line_part1(self, log_mock):
        """Method that tests the process line part1 function of
        the input data processor"""

        self.data_proc.buffer_size = 4096
        lines = [bytes("cause validation error", encoding='utf-8')]
        self.data_proc.process_lines(lines)
        log_mock.debug.assert_called()

    @patch("input_unit.input_data_processor.InputDataProcessor."
           "handle_json_parse_success")
    def test_handle_process_line_part2(self, parse_mock):
        """Method that tests the process line part2 function of
        the input data processor"""

        self.data_proc.buffer_size = 4096
        lines = [bytes(self.metric, encoding='utf-8')]
        self.data_proc.process_lines(lines)
        parse_mock.assert_called_with(MessageType.METRIC, UnifiedModel.parse_obj(loads(self.metric)))

    @patch("input_unit.input_data_processor.InputDataProcessor."
           "handle_json_parse_success")
    def test_handle_process_line_part3(self, parse_mock):
        """Method that tests the process line part3 function of
        the input data processor"""

        self.data_proc.buffer_size = 4096
        self.data_proc.purge_scheme = PurgeScheme.ENABLED
        lines = [bytes(self.log, encoding='utf-8')]
        self.data_proc.process_lines(lines)
        parse_mock.assert_called_with(MessageType.LOG, loads(self.log))


    @patch("input_unit.input_data_processor.InputDataProcessor."
           "handle_json_parse_success")
    def test_handle_process_line_part4(self, parse_mock):
        """Method that tests the process line part4 function of
        the input data processor"""

        self.data_proc.buffer_size = 4096
        self.data_proc.purge_scheme = PurgeScheme.DISABLED
        lines = [bytes(self.log, encoding='utf-8')]
        self.data_proc.process_lines(lines)
        parse_mock.assert_called_with(MessageType.LOG, loads(self.log))

    @patch("input_unit.input_data_processor.logging")
    def test_handle_process_line_part5(self, log_mock):
        """Method that tests the process line part4 function of
        the input data processor"""
        self.data_proc.purge_scheme = PurgeScheme.ENABLED
        lines = [bytes(self.big_log, encoding='utf-8')]
        self.data_proc.buffer_size = 300
        self.data_proc.process_lines(lines)
        log_mock.debug.assert_called_with(
            "Big log ingested and dropped. "
            f"Length: {len(lines[0])}")

    @patch("input_unit.input_data_processor.logging")
    def test_handle_process_line_part6(self, log_mock):
        """Method that tests the process line part4 function of
        the input data processor"""
        self.data_proc.purge_scheme = PurgeScheme.ENABLED
        self.data_proc.buffer_size = 4096
        lines = [bytes('{}', encoding='utf-8')]
        self.data_proc.process_lines(lines)
        log_mock.debug.assert_called_with(
            "Failed to extract json log. No data after loading.")
