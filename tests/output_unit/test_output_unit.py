import json
import unittest
from json import loads
from unittest.mock import call, patch

from config_manager.models import (
    FileAdapterConfig,
    FluentdAdapterConfig,
    GeneralConfig,
    MQTTAdapterConfig,
    OTELMetricsAdapterConfig,
)
from output_unit.adapters import (
    FluentdAdapter,
    LogsFileAdapter,
    MetricsFileAdapter,
    MQTTLogsAdapter,
    MQTTMetricsAdapter,
    OTELMetricsAdapter,
    PrometheusAdapter,
)
from output_unit.metric_storage import MetricStore
from output_unit.otel_metrics import OTELMetrics
from output_unit.output_unit import STOP_THREADS_EVENT, OutputUnit, Queue
from output_unit.policies import (
    PolicyLogsDebug,
    PolicyLogsDefault,
    PolicyMetricsDebug,
    PolicyMetricsDefault,
    PolicyShoreLogsDefault,
)
from output_unit.utils import (
    BufferFullError,
    BufferNames,
    BufferNotFoundError,
    InvalidDataFormatError,
)


class TestOutputUnit(unittest.TestCase):
    def setUp(self) -> None:
        general_config = GeneralConfig()
        general_config.enabled_adapters = [
            "LogsFileAdapter",
            "MetricsFileAdapter",
            "MQTTLogsAdapter",
            "MQTTMetricsAdapter",
            "OTELMetricsAdapter",
        ]
        self.adapters_config = {
            "General": general_config,
            "MQTTLogsAdapter": MQTTAdapterConfig(),
            "MQTTMetricsAdapter": MQTTAdapterConfig(),
            "LogsFileAdapter": FileAdapterConfig(),
            "MetricsFileAdapter": FileAdapterConfig(),
            "FluentAdapter": FluentdAdapterConfig(),
            "OTELMetricsAdapter": OTELMetricsAdapterConfig(),
        }

        self.output_unit = OutputUnit(self.adapters_config, 10, 10)
        self.dummy_data = loads(
            '{"Hd": {"Ct": "RazorLink.Internal.Razor.' 'Tenants.Verbose"}}'
        )
        with open("./tests/output_unit/otel.json", "r") as f:
            self.otel_config =  json.load(f)

    def tearDown(self) -> None:
        del self.output_unit
        del self.adapters_config
        if hasattr(MetricStore, 'instance'):
            MetricStore.keep_running = False
            del MetricStore.instance

    def test_output_unit_instance(self):
        self.assertIsInstance(
            self.output_unit._output_buffers[BufferNames.LOGS_BUFFER], Queue
        )
        self.assertIsInstance(
            self.output_unit._output_buffers[BufferNames.METRICS_BUFFER], Queue
        )
        self.assertIsInstance(self.output_unit._adapters_config, dict)
        self.assertEqual(self.output_unit._adapters_config, self.adapters_config)
        self.assertTrue(PolicyLogsDebug() in self.output_unit._policies)
        self.assertTrue(PolicyMetricsDebug() in self.output_unit._policies)
        self.assertTrue(PolicyLogsDefault() in self.output_unit._policies)
        self.assertTrue(PolicyMetricsDefault() in self.output_unit._policies)
        self.assertTrue(PolicyShoreLogsDefault() in self.output_unit._policies)

    def test_output_unit_buffer_size_invalid_type(self):
        with self.assertRaises(TypeError):
            output_unit = OutputUnit(self.adapters_config, "10", 100)
            self.assertIsInstance(output_unit, None)

    def test_check_buffer_name_successful(self):
        self.assertTrue(
            self.output_unit._OutputUnit__check_buffer_name(BufferNames.LOGS_BUFFER)
        )

    def test_check_buffer_name_failure(self):
        with self.assertRaises(BufferNotFoundError):
            self.assertFalse(
                self.output_unit._OutputUnit__check_buffer_name("METRICS_CONFIG")
            )

    def test_enque_data_in_buffer_successful(self):
        self.output_unit.enque_data_in_buffer(BufferNames.LOGS_BUFFER, self.dummy_data)
        self.assertTrue(
            not self.output_unit._output_buffers[BufferNames.LOGS_BUFFER].empty()
        )
        added_data = self.output_unit._output_buffers[BufferNames.LOGS_BUFFER].get()
        self.assertEqual(added_data, self.dummy_data)

    def test_enque_data_in_buffer_not_found_failure(self):
        with self.assertRaises(BufferNotFoundError):
            self.output_unit.enque_data_in_buffer("LOGS_BUFFER", self.dummy_data)
        self.assertTrue(
            self.output_unit._output_buffers[BufferNames.LOGS_BUFFER].empty()
        )

    def test_enque_data_in_buffer_wrong_data_format_failure(self):
        with self.assertRaises(InvalidDataFormatError):
            self.output_unit.enque_data_in_buffer(BufferNames.METRICS_BUFFER, "data")
        self.assertTrue(
            self.output_unit._output_buffers[BufferNames.METRICS_BUFFER].empty()
        )

    def test_enque_data_in_buffer_full_failure(self):
        for i in range(10):
            self.output_unit.enque_data_in_buffer(
                BufferNames.LOGS_BUFFER, self.dummy_data
            )
        with self.assertRaises(BufferFullError):
            self.output_unit.enque_data_in_buffer(
                BufferNames.LOGS_BUFFER, self.dummy_data
            )
        self.assertTrue(
            self.output_unit._output_buffers.get(BufferNames.LOGS_BUFFER).full()
        )

    def test_deque_data_from_buffer_successful(self):
        self.output_unit._output_buffers[BufferNames.METRICS_BUFFER].put(
            self.dummy_data
        )
        dequed_data = self.output_unit.deque_data_from_buffer(
            BufferNames.METRICS_BUFFER
        )
        self.assertEqual(dequed_data, self.dummy_data)

    def test_deque_data_from_buffer_not_found_failure(self):
        with self.assertRaises(BufferNotFoundError):
            dequed_data = self.output_unit.deque_data_from_buffer("METRICS_BUFFER")
            self.assertEqual(dequed_data, {})

    def test_deque_data_from_buffer_empty(self):
        dequed_data = self.output_unit.deque_data_from_buffer(BufferNames.LOGS_BUFFER)
        self.assertEqual(dequed_data, {})

    def test_get_available_adapters(self):
        available_adapters = self.output_unit.get_available_adapters()
        self.assertIsInstance(available_adapters, list)
        self.assertTrue(MQTTLogsAdapter in available_adapters)
        self.assertTrue(MQTTMetricsAdapter in available_adapters)
        self.assertTrue(LogsFileAdapter in available_adapters)
        self.assertTrue(MetricsFileAdapter in available_adapters)
        self.assertTrue(FluentdAdapter in available_adapters)
        self.assertTrue(PrometheusAdapter in available_adapters)
        self.assertTrue(OTELMetricsAdapter in available_adapters)

    def test_register_adapters(self):
        """available_adapters list will look like this [class LogsFileAdapter,
        class MetricsFileAdapter, class FluentdAdapter, class MQTTAdapter,
        PrometheusAdapter]
        self._adapters_config is the same self.adapters_config given in setUp,
        so FluentdAdapter is missing from the configuration
        """
        with patch.object(OTELMetrics, "read_config") as read_conf_mock:
            read_conf_mock.return_value = self.otel_config
            unreg_adapters = self.output_unit.register_adapters()
            self.assertTrue(FluentdAdapter in unreg_adapters)
            self.assertTrue(PrometheusAdapter in unreg_adapters)
            self.assertTrue("LogsFileAdapter" in self.output_unit._adapters)
            self.assertTrue("MetricsFileAdapter" in self.output_unit._adapters)
            self.assertTrue("MQTTLogsAdapter" in self.output_unit._adapters)
            self.assertTrue("MQTTMetricsAdapter" in self.output_unit._adapters)
            self.assertTrue("OTELMetricsAdapter" in self.output_unit._adapters)


    def test_register_adapters_only_otel(self):
        with patch.object(OTELMetrics, "read_config") as read_conf_mock:
            read_conf_mock.return_value = self.otel_config
            self.output_unit.register_adapters()
            return_val = self.output_unit.register_adapters(
                only_otel=True, system_id="c9fe852f-2434-4ae3-a7d7-c06d43c1ba63"
            )
            self.assertEqual(return_val, [])

    def test_count_messages_successful(self):
        self.output_unit._OutputUnit__count_messages(BufferNames.METRICS_BUFFER)
        self.output_unit._OutputUnit__count_messages(BufferNames.LOGS_BUFFER)
        self.assertEqual(self.output_unit._metrics_counter, 1)
        self.assertEqual(self.output_unit._logs_counter, 1)

    def test_count_messages_failure(self):
        self.output_unit._OutputUnit__count_messages("METRICS_BUFFER")
        self.output_unit._OutputUnit__count_messages("LOGS_BUFFER")
        self.assertEqual(self.output_unit._metrics_counter, 0)
        self.assertEqual(self.output_unit._logs_counter, 0)

    def test_process_streaming_successful(self):
        self.output_unit.enque_data_in_buffer(BufferNames.LOGS_BUFFER, self.dummy_data)
        self.output_unit._OutputUnit__process_streaming(BufferNames.LOGS_BUFFER)
        self.assertTrue(self.output_unit._logs_counter, 1)

    def test_process_streaming_failure(self):
        with self.assertRaises(BufferNotFoundError):
            self.output_unit._OutputUnit__process_streaming("LOGS_BUFFER")
        self.assertFalse(self.output_unit._logs_counter, 1)

    @patch("output_unit.output_unit.OutputUnit._OutputUnit__process_streaming")
    @patch("output_unit.output_unit.STOP_THREADS_EVENT")
    def test_process_metrics_successful(self, stop_event_mock, streaming_mock):
        sleep_time = 2
        stop_event_mock.is_set.side_effect = [False, False, True]
        streaming_mock.return_value = None
        self.output_unit.process_metrics(sleep_time)
        stop_event_mock.is_set.assert_has_calls([call(), call(), call()])

    @patch("output_unit.output_unit.OutputUnit._OutputUnit__process_streaming")
    @patch("output_unit.output_unit.STOP_THREADS_EVENT")
    def test_process_metrics_failure(self, stop_event_mock, streaming_mock):
        sleep_time = 2
        stop_event_mock.is_set.side_effect = [False, False, True]
        exception_message = "Caught Exception while processing metrics!"
        streaming_mock.side_effect = Exception(exception_message)
        with self.assertRaises(Exception):
            self.output_unit.process_metrics(sleep_time)
            streaming_mock.assert_called_once()

    @patch("output_unit.output_unit.OutputUnit._OutputUnit__process_streaming")
    @patch("output_unit.output_unit.STOP_THREADS_EVENT")
    def test_process_logs_successful(self, stop_event_mock, streaming_mock):
        sleep_time = 1
        stop_event_mock.is_set.side_effect = [False, False, True]
        streaming_mock.return_value = None
        self.output_unit.process_logs(sleep_time)
        stop_event_mock.is_set.assert_has_calls([call(), call(), call()])

    @patch("output_unit.output_unit.OutputUnit._OutputUnit__process_streaming")
    @patch("output_unit.output_unit.STOP_THREADS_EVENT")
    def test_process_logs_failure(self, stop_event_mock, streaming_mock):
        sleep_time = 1
        stop_event_mock.is_set.side_effect = [False, False, True]
        exception_message = "Caught Exception while processing logs!"
        streaming_mock.side_effect = Exception(exception_message)
        with self.assertRaises(Exception):
            self.output_unit.process_logs(sleep_time)
            streaming_mock.assert_called_once()

    def test_buffer_threads(self):
        self.output_unit.run_threads()

        self.assertEqual(self.output_unit.thread_logs.name, "OutputUnitLogs")
        self.assertEqual(self.output_unit.thread_metrics.name, "OutputUnitMetrics")
        self.assertTrue(self.output_unit.thread_metrics.is_alive())
        self.assertTrue(self.output_unit.thread_logs.is_alive())
        self.output_unit.set_stop_event()
        self.assertTrue(STOP_THREADS_EVENT.is_set)
        self.output_unit.thread_metrics.join()
        self.output_unit.thread_logs.join()


    def test_read_and_amend_exporters_config(self):
        """Test to check the read_and_amend_exporters_config """

        _exporters = ["otlp", "mqtt"]
        exporter_cfg = {}
        for exporter in _exporters:
            exporter_cfg[exporter] = {
                "enabled": True,
                "interval": 10,
                "timeout_interval": 10
            }
        _intervals = [exporter_cfg["mqtt"]["interval"] * 1000, exporter_cfg["otlp"]["interval"] * 1000]
        _timeout_intervals = [exporter_cfg["mqtt"]["timeout_interval"] * 1000, exporter_cfg["otlp"]["timeout_interval"] * 1000]
        exporters, intervals, timeout_intervals = self.output_unit._read_and_amend_exporters_config(exporters_config=exporter_cfg)
        self.assertEqual(intervals, _intervals)
        self.assertEqual(timeout_intervals, _timeout_intervals)
        self.assertEqual(exporters, _exporters)


    def test_read_and_amend_exporters_config_no_exporter(self):
        """Test to check the read_and_amend_exporters_config """

        _exporters = ["otlp", "mqtt"]
        exporter_cfg = {}
        for exporter in _exporters:
            exporter_cfg[exporter] = {
                "enabled": False,
                "interval": 10,
                "timeout_interval": 10
            }
        exporters, intervals, timeout_intervals = self.output_unit._read_and_amend_exporters_config(exporters_config=exporter_cfg)
        self.assertEqual(intervals, [])
        self.assertEqual(timeout_intervals, [])
        self.assertEqual(exporters, [])


    def test_read_and_amend_exporters_config_only_one_exp_enabled(self):
        """Test to check the read_and_amend_exporters_config """

        _exporters = ["otlp", "mqtt"]
        exporter_cfg = {
            _exporters[0]: {
                 "enabled": True,
                "interval": 10,
                "timeout_interval": 10
            },
            _exporters[1]: {
                 "enabled": False,
                "interval": 10,
                "timeout_interval": 10
            }
        }

        _intervals = [exporter_cfg["otlp"]["interval"] * 1000]
        _timeout_intervals = [exporter_cfg["otlp"]["timeout_interval"] * 1000]        
        exporters, intervals, timeout_intervals = self.output_unit._read_and_amend_exporters_config(exporters_config=exporter_cfg)
        self.assertEqual(intervals, _intervals)
        self.assertEqual(timeout_intervals, _timeout_intervals)
        self.assertEqual(exporters, [_exporters[0]])