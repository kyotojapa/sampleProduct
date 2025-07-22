import json
import logging as logs
import os
import shutil
import time
import unittest
from json import loads
from pathlib import Path
from pathlib import Path as path
from typing import Dict
from unittest import mock

import paho.mqtt.client as mqtt  # type: ignore
import product_common_logging as logging  # type: ignore
from config_manager.models import (
    FileAdapterConfig,
    FluentdAdapterConfig,
    MQTTAdapterConfig,
    OTELMetricsAdapterConfig,
)
from data_processing.models.unified_model import UnifiedModel
from fluent import sender
from output_unit import adapters
from output_unit.adapters import (
    ConnectionStatus,
    FluentdAdapter,
    MQTTAdapterBase,
    get_transform_functions,
)
from output_unit.metric_storage import MetricStore
from output_unit.otel_metrics import OTELMetrics

logging.setLevel(logging.ERROR)


@mock.patch('output_unit.otel_metrics.PeriodicExportingMetricReader', mock.MagicMock())
class TestAdapters(unittest.TestCase):

    def setUp(self) -> None:
        self.mqtt_adapter = adapters.MQTTAdapterBase()
        self.logs_mqtt_adapter = adapters.MQTTLogsAdapter()
        self.metric_mqtt_adapter = adapters.MQTTMetricsAdapter()
        self.file_adapter = adapters.FileAdapterBase()
        self.log_file_adapter = adapters.LogsFileAdapter()
        self.metrics_file_adapter = adapters.MetricsFileAdapter()
        self.fluentd_adapter = adapters.FluentdAdapter()
        self.prometheus_adapter = adapters.PrometheusAdapter()
        self.otel_metrics_adapter = adapters.OTELMetricsAdapter()
        self.dummy_adapters_config = {
            "MQTTAdapter": MQTTAdapterConfig(),
            "FileAdapter": FileAdapterConfig(file_path=path.home() / "log"),
            "MetricsFileAdapter": FileAdapterConfig(
                file_path=path.home() / "metricfilelog"
            ),
            "LogsFileAdapter": FileAdapterConfig(file_path=path.home() / "logsfilelog"),
            "FluentdAdapter": FluentdAdapterConfig(),
            "OTELMetricsAdapter": OTELMetricsAdapterConfig(),
        }
        self.dummy_data = loads(
            '{"Hd": {"Ct": "RazorLink.Internal.Razor.' 'Tenants.Verbose"}}'
        )

    def tearDown(self) -> None:

        del self.mqtt_adapter
        del self.file_adapter
        del self.fluentd_adapter
        del self.prometheus_adapter
        del self.otel_metrics_adapter
        if hasattr(MetricStore, 'instance'):
            MetricStore.keep_running = False
            del MetricStore.instance

    def test_mqtt_adapter(self):
        self.assertIsInstance(self.mqtt_adapter, adapters.Adapter)
        self.assertIsInstance(self.mqtt_adapter, adapters.MQTTAdapterBase)
        self.assertIsInstance(self.logs_mqtt_adapter, adapters.Adapter)
        self.assertIsInstance(self.logs_mqtt_adapter, adapters.MQTTLogsAdapter)
        self.assertIsInstance(self.metric_mqtt_adapter, adapters.Adapter)
        self.assertIsInstance(self.metric_mqtt_adapter, adapters.MQTTMetricsAdapter)

    def test_file_adapter(self):
        self.assertIsInstance(self.file_adapter, adapters.Adapter)
        self.assertIsInstance(self.file_adapter, adapters.FileAdapterBase)
        self.assertIsInstance(self.log_file_adapter, adapters.Adapter)
        self.assertIsInstance(self.log_file_adapter, adapters.LogsFileAdapter)
        self.assertIsInstance(self.metrics_file_adapter, adapters.Adapter)
        self.assertIsInstance(self.metrics_file_adapter, adapters.MetricsFileAdapter)

    def test_fluentd_adapter(self):
        self.assertIsInstance(self.fluentd_adapter, adapters.Adapter)
        self.assertIsInstance(self.fluentd_adapter, adapters.FluentdAdapter)

    def test_prometheus_adapter(self):
        self.assertIsInstance(self.prometheus_adapter, adapters.Adapter)
        self.assertIsInstance(self.prometheus_adapter, adapters.PrometheusAdapter)

    def test_otel_metrics_adapter(self):
        self.assertIsInstance(self.otel_metrics_adapter, adapters.Adapter)
        self.assertIsInstance(self.otel_metrics_adapter, adapters.OTELMetricsAdapter)

    def test_mqtt_configure(self):
        adapter_config = self.dummy_adapters_config.get("MQTTAdapter", None)
        self.assertIsNone(self.mqtt_adapter.configure(adapter_config))
        self.assertIsNone(self.logs_mqtt_adapter.configure(adapter_config))
        self.assertIsNone(self.metric_mqtt_adapter.configure(adapter_config))

    def test_mqtt_write_data(self):
        self.assertIsNone(self.mqtt_adapter.write_data(self.dummy_data))
        self.assertIsNone(self.logs_mqtt_adapter.write_data(self.dummy_data))
        self.assertIsNone(self.metric_mqtt_adapter.write_data(self.dummy_data))

    def test_file_configure(self):
        adapter_config = self.dummy_adapters_config.get("FileAdapter", None)
        met_adapter_config = self.dummy_adapters_config.get("MetricsFileAdapter", None)
        log_adapter_config = self.dummy_adapters_config.get("LogsFileAdapter", None)
        self.assertIsNone(self.file_adapter.configure(adapter_config))
        self.assertIsNone(self.log_file_adapter.configure(met_adapter_config))
        self.assertIsNone(self.metrics_file_adapter.configure(log_adapter_config))

    def test_file_write_data(self):
        self.assertIsNone(self.file_adapter.write_data(self.dummy_data))
        self.assertIsNone(self.log_file_adapter.write_data(self.dummy_data))
        self.assertIsNone(self.metrics_file_adapter.write_data(self.dummy_data))

    def test_fluentd_configure(self):
        adapter_config = self.dummy_adapters_config.get("FluentdAdapter", None)
        self.assertIsNone(self.fluentd_adapter.configure(adapter_config))

    def test_fluentd_write_data(self):
        self.assertIsNone(self.fluentd_adapter.write_data(self.dummy_data))

    def test_prometheus_configure(self):
        adapter_config = self.dummy_adapters_config.get("PrometheusAdapter", None)
        self.assertIsNone(self.prometheus_adapter.configure(adapter_config))

    def test_prometheus_write_data(self):
        self.assertIsNone(self.prometheus_adapter.write_data(self.dummy_data))

    @mock.patch("output_unit.adapters.OTELMetrics")
    def test_otel_metrics_configure(self, mock_otel_metrics):
        with mock.patch.object(
                self.otel_metrics_adapter,
                "metrics_definition_path",
                new=Path("./tests/output_unit/otel.json")):
            adapter_config = self.dummy_adapters_config.get("OTELMetricsAdapter", None)
            self.assertIsNone(self.otel_metrics_adapter.configure(adapter_config))


    def test_otel_metrics_write_data(self):
        self.assertIsNone(self.otel_metrics_adapter.write_data(self.dummy_data))


class TestFileAdapter(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        adapter_config_dict = {
            "file_path": path(f"{path.home()}"),
            "rolling_file_max_bytes": 2048,
            "rolling_file_count": 5,
        }

        cls.logs_file_adapter = adapters.LogsFileAdapter()
        cls.metrics_file_adapter = adapters.MetricsFileAdapter()
        cls.adapter_config = FileAdapterConfig.parse_obj(adapter_config_dict)
        cls.log_path = TestFileAdapter.adapter_config.file_path / (
            TestFileAdapter.logs_file_adapter.file_path
        )
        cls.metric_path = TestFileAdapter.adapter_config.file_path / (
            TestFileAdapter.metrics_file_adapter.file_path
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(TestFileAdapter.log_path.parent)
        shutil.rmtree(TestFileAdapter.metric_path.parent)
        del TestFileAdapter.logs_file_adapter
        del TestFileAdapter.metrics_file_adapter

    def test_configure(self):
        """Method that tests that after configuration a file logger
        exists"""

        self.logs_file_adapter.configure(self.adapter_config)
        self.assertNotEqual(self.logs_file_adapter.file_logger, None)
        self.assertIsInstance(
            type(self.logs_file_adapter.file_logger), type(logs.Logger)
        )

        self.metrics_file_adapter.configure(self.adapter_config)
        self.assertNotEqual(self.metrics_file_adapter.file_logger, None)
        self.assertIsInstance(
            type(self.metrics_file_adapter.file_logger), type(logs.Logger)
        )

    def test_can_write_log(self):
        """Method that checks that data have been written to a log file"""

        if os.path.exists(TestFileAdapter.log_path.parent):
            shutil.rmtree(TestFileAdapter.log_path.parent)
        self.logs_file_adapter.configure(self.adapter_config)
        self.logs_file_adapter.write_data("Hello World")

        self.assertEqual(TestFileAdapter.log_path.stat().st_size > 0, True)
        with open(TestFileAdapter.log_path, mode="rt", encoding="utf-8") as f_p:
            data = f_p.read(len("Hello World"))

        self.assertEqual(data, "Hello World")

    def test_can_write_metrics(self):
        """Method that checks that data have been written to a metric file"""
        if os.path.exists(TestFileAdapter.log_path.parent):
            shutil.rmtree(TestFileAdapter.log_path.parent)
        self.metrics_file_adapter.configure(self.adapter_config)
        self.metrics_file_adapter.write_data("Hello World")

        self.assertEqual(TestFileAdapter.metric_path.stat().st_size > 0, True)
        with open(TestFileAdapter.metric_path, mode="rt", encoding="utf-8") as f_p:
            data = f_p.read(len("Hello World"))

        self.assertEqual(data, "Hello World")

    def test_rolling_file_count_is_correct_logs(self):
        """Method that checks that data have been written to a log file"""
        self.logs_file_adapter.configure(self.adapter_config)
        write_counter = 1
        keep_running = True
        file_count = get_file_count(TestFileAdapter.log_path.parent)
        while keep_running and file_count < (self.adapter_config.rolling_file_count):
            file_count = get_file_count(TestFileAdapter.log_path.parent)
            self.logs_file_adapter.write_data(f"Counter: {write_counter}")
            write_counter += 1
            time.sleep(0.01)

        self.assertEqual(get_file_count(TestFileAdapter.log_path.parent), file_count)

    def test_rolling_file_count_is_correct_metrics(self):
        """Method that checks that rolling count for metric files is correct"""
        self.metrics_file_adapter.configure(self.adapter_config)
        write_counter = 1
        keep_running = True
        file_count = get_file_count(TestFileAdapter.metric_path.parent)
        while keep_running and file_count < (self.adapter_config.rolling_file_count):
            file_count = get_file_count(TestFileAdapter.metric_path.parent)
            self.metrics_file_adapter.write_data(f"Counter: {write_counter}")
            write_counter += 1
            time.sleep(0.01)

        self.assertEqual(get_file_count(TestFileAdapter.metric_path.parent), file_count)

    def test_max_file_size_is_valid_metrics(self):
        """Method that checks that the file size is less than
        the rolling_file_size
        """

        self.metrics_file_adapter.configure(self.adapter_config)
        keep_running = True
        file_count = get_file_count(TestFileAdapter.metric_path.parent)
        write_counter = 1

        while keep_running and file_count < (self.adapter_config.rolling_file_count):
            file_count = get_file_count(TestFileAdapter.metric_path.parent)
            self.metrics_file_adapter.write_data(f"Counter: {write_counter}")
            write_counter += 1
            time.sleep(0.01)

        file_name = str(TestFileAdapter.metric_path) + (
            f".{self.adapter_config.rolling_file_count-1}"
        )
        file_size = os.stat(file_name).st_size

        self.assertLessEqual(file_size, self.adapter_config.rolling_file_max_bytes)

    def test_max_file_size_is_valid_log(self):
        """Method that checks that the file size is less than
        the rolling_file_size
        """

        self.logs_file_adapter.configure(self.adapter_config)
        keep_running = True
        file_count = get_file_count(TestFileAdapter.log_path.parent)
        write_counter = 1

        while keep_running and file_count < (self.adapter_config.rolling_file_count):
            file_count = get_file_count(TestFileAdapter.log_path.parent)
            self.logs_file_adapter.write_data(f"Counter: {write_counter}")
            write_counter += 1
            time.sleep(0.01)

        file_name = str(TestFileAdapter.log_path) + (
            f".{self.adapter_config.rolling_file_count-1}"
        )
        file_size = os.stat(file_name).st_size

        self.assertLessEqual(file_size, self.adapter_config.rolling_file_max_bytes)


class TestMQTTAdapter(unittest.TestCase):
    """Test if the mqtt adapter in the output unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        adapter_config_dict = {
            "mqtt_version": 5,
            "broker_port": 1883,
            "broker_address": "127.0.0.1",
        }
        adapter_config = MQTTAdapterConfig.parse_obj(adapter_config_dict)

        cls.mqtt_adapter = MQTTAdapterBase()
        cls.adapter_config = adapter_config
        cls.valid_msg = loads(
            '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/Loggi'
            'ngObject.cpp","Li":862},"Ct":"lw.comp.IR.snapshot","Ti":"20240217'
            'T083551.181535","Cl":{"ownr":"<IrMgr!5851>","oid":"<IrIf!5886>","'
            'info":"bfCSC=0,AddrDoms=private,public,sock=<PubDgrmComms!5897>,S'
            'vr=1","sinf":"0203 - Active","goid":"0efbd20e-9853-4582-ba95-b84c'
            'fcca8e42"},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":3302,"'
            'ctx":"<IrMgr!323222>","obj":"<IrIf!5886>","val":{"Sta":{"g":{"IfI'
            'd":"Ethernet_eth2_4031","S":{"cSt":203,"bfCSC":0,"svcCost'
            '":"1"}},"tx":{"qued":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"tuB":'
            '6710887,"Bu":{"B":10066330}}},"Sts":{"tx":{"Bu":{"tTu":10210652,"'
            'nTu":15219,"nooB":0,"tTkn":144322,"tSto":0},"d":{"nP":"TCV-NotSet'
            '","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"t'
            '":{"nP":"100000","nB":"20000"}},"rx":{"r":{"nP":"TCV-NotSet","nB"'
            ':"TCV-NotSet"},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP'
            '":"TCV-NotSet","nB":"TCV-NotSet"}}}}}}'
        )

    @classmethod
    def tearDownClass(cls):
        """Tear down method"""

        del TestMQTTAdapter.mqtt_adapter

    def test_configure(self):
        """Method that tests that after configuration an mqtt client
        is configured"""

        TestMQTTAdapter.adapter_config.mqtt_version = 5
        TestMQTTAdapter.adapter_config.broker_port = 51883
        TestMQTTAdapter.adapter_config.broker_address = "127.0.0.2"
        topic = "network/sdwan/uncategorised"
        TestMQTTAdapter.mqtt_adapter.client.keep_trying_to_connect.set()
        TestMQTTAdapter.mqtt_adapter.configure(TestMQTTAdapter.adapter_config)
        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.mqtt_adapter.topic = "network/sdwan/uncategorised"

        self.assertEqual(
            TestMQTTAdapter.mqtt_adapter.client.mqtt_version,
            TestMQTTAdapter.adapter_config.mqtt_version,
        )
        self.assertEqual(
            TestMQTTAdapter.mqtt_adapter.client.broker_port,
            TestMQTTAdapter.adapter_config.broker_port,
        )
        self.assertEqual(
            TestMQTTAdapter.mqtt_adapter.client.broker_address,
            TestMQTTAdapter.adapter_config.broker_address,
        )
        self.assertEqual(TestMQTTAdapter.mqtt_adapter.topic, topic)

        self.assertNotEqual(TestMQTTAdapter.mqtt_adapter.client, None)
        self.assertIsInstance(
            TestMQTTAdapter.mqtt_adapter.client.mqtt_client, mqtt.Client)
        self.assertEqual(
            TestMQTTAdapter.mqtt_adapter.client.connect_thread.daemon, True)
        self.assertEqual(
            TestMQTTAdapter.mqtt_adapter.client.connect_thread.is_alive(),
            True)

    @mock.patch("output_unit.adapters.properties")
    def test_can_write(self, mock_properties):
        "Method that checks if data can be written with client"

        props = mock_properties.return_value
        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.mqtt_adapter.topic = "some_topic"

        with mock.patch.object(
            TestMQTTAdapter.mqtt_adapter.client, "mqtt_client"
        ) as mqtt_mock:
            TestMQTTAdapter.mqtt_adapter.write_data("some_data")
            mqtt_mock.publish.assert_called_with(
                "some_topic", json.dumps("some_data"), qos=2, properties=props
            )

    @mock.patch("output_unit.adapters.logging")
    def test_fail_to_write(self, mock_logs):
        "Method that checks if data will fail to be written"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.DISCONNECTED)
        TestMQTTAdapter.mqtt_adapter.write_data("some_data")

        mock_logs.debug.assert_called_with("MQTT Client is not connected")

    @mock.patch("output_unit.mqtt_client.logging")
    def test_on_disconnect_rc_not_zero(self, mock_logs):
        """Method that checks if on_disconnect with rc not zero is working
        properly"""

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.mqtt_adapter.client.disconnected_evt.clear()
        TestMQTTAdapter.mqtt_adapter.client._MQTTClient__on_disconnect(
            None, "some_data", 1, None
        )

        mock_logs.warning.assert_called_with(
            "MQTT exporter client disconnected unexpectedly. "
            "Client: None "
            "Result Code: 1 "
            "User Data: some_data "
            "Props: None "
        )

        self.assertEqual(
            self.mqtt_adapter.client.connection_status,
            ConnectionStatus.DISCONNECTED
        )
        self.assertEqual(
            self.mqtt_adapter.client.disconnected_evt.is_set(), True)

    @mock.patch("output_unit.mqtt_client.logging")
    def test_on_disconnect_rc_zero(self, mock_logs):
        "Method that checks if on_disconnect with rc zero is working properly"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.mqtt_adapter.client.disconnected_evt.clear()

        TestMQTTAdapter.mqtt_adapter.client._MQTTClient__on_disconnect(
            None, "some_data", 0, None
        )

        mock_logs.info.assert_called_with("mqtt exporter client disconnected")

        self.assertEqual(
            self.mqtt_adapter.client.connection_status, ConnectionStatus.DISCONNECTED
        )
        self.assertEqual(
            self.mqtt_adapter.client.disconnected_evt.is_set(), True)

    @mock.patch("output_unit.mqtt_client.logging")
    def test_on_connect_rc_zero(self, mock_logs):
        "Method that checks if on_connect with rc zero is working properly"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.DISCONNECTED)
        TestMQTTAdapter.mqtt_adapter.client._MQTTClient__on_connect(
            None, "some_data", None, 0, None
        )

        mock_logs.info.assert_called_with("mqtt exporter client connected: None")

        self.assertEqual(
            self.mqtt_adapter.client.connection_status,
            ConnectionStatus.CONNECTED
        )

    @mock.patch("output_unit.mqtt_client.logging")
    def test_on_connect_rc_not_zero(self, mock_logs):
        "Method that checks if on_connect with rc not zero is working properly"

        TestMQTTAdapter.mqtt_adapter.client._MQTTClient__on_connect(
            None, "some_data", None, 1, None
        )

        mock_logs.debug.assert_called_with(
            "Exporter Client status: Not connected "
            "User data: some_data "
            "Flags: None "
            "Result Code: 1 "
            "Properties: None "
        )

    @mock.patch("output_unit.adapters.properties")
    def test_connect_v5(self, mock_properties):
        "Method that checks connect with v5 client"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 5
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)

        props = mock_properties.return_value
        with mock.patch.object(TestMQTTAdapter.mqtt_adapter.client, "mqtt_client") as mqtt_cl_mock:
            with mock.patch.object(TestMQTTAdapter.mqtt_adapter.client, "_opts") as mock_opts:
                TestMQTTAdapter.mqtt_adapter.client._MQTTClient__connect()

                mock_opts.return_value = {
                    "port": TestMQTTAdapter.mqtt_adapter.client.broker_port,
                    "clean_start": mqtt.MQTT_CLEAN_START_FIRST_ONLY,
                    "properties": props,
                    "keepalive": 60,
                }

                mqtt_cl_mock.connect.assert_called_with(
                    TestMQTTAdapter.mqtt_adapter.client.broker_address,
                    **mock_opts
                )
                mqtt_cl_mock.loop_start.assert_called_with()
                self.assertEqual(
                    TestMQTTAdapter.mqtt_adapter.client.disconnected_evt.is_set(), False
                )

    def test_connect_v3(self):
        "Method that checks connect with v3 client"

        TestMQTTAdapter.adapter_config.mqtt_version = 3
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)
        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)

        with mock.patch.object(
            TestMQTTAdapter.mqtt_adapter.client, "mqtt_client"
        ) as mqtt_cl_mock:
            with mock.patch.object(TestMQTTAdapter.mqtt_adapter.client, "_opts") as mock_opts:
                TestMQTTAdapter.mqtt_adapter.client._MQTTClient__connect()

                mock_opts.return_value = {
                    "port": TestMQTTAdapter.mqtt_adapter.client.broker_port,
                    "keepalive": 60,
                }

                mqtt_cl_mock.connect.assert_called_with(
                    TestMQTTAdapter.mqtt_adapter.client.broker_address,
                    **mock_opts
                )
                mqtt_cl_mock.loop_start.assert_called_with()
                self.assertEqual(
                    TestMQTTAdapter.mqtt_adapter.client.disconnected_evt.is_set(), False
                )

    def test_get_v3_options(self):
        "Method that checks the get_v3_options method"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 3
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)
        res = TestMQTTAdapter.mqtt_adapter.client._MQTTClient__get_v3_options()
        self.assertNotEqual(res["options"], None)
        self.assertNotEqual(res["connect_options"], None)
        self.assertNotEqual(res["options"]["client_id"], None)
        self.assertNotEqual(res["options"]["transport"], None)
        self.assertNotEqual(res["options"]["protocol"], None)
        self.assertNotEqual(res["options"]["clean_session"], None)
        self.assertNotEqual(res["connect_options"]["port"], None)
        self.assertNotEqual(res["connect_options"]["keepalive"], None)

    @mock.patch("output_unit.adapters.properties")
    def test_get_v5_options(self, mock_props):
        "Method that checks the get_v5_options method"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 5
        mock_props.SessionExpiryInterval.return_value = 30 * 60
        props = mock_props.return_value
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)
        res = TestMQTTAdapter.mqtt_adapter.client._MQTTClient__get_v5_options(props)
        self.assertNotEqual(res["options"], None)
        self.assertNotEqual(res["connect_options"], None)
        self.assertNotEqual(res["options"]["client_id"], None)
        self.assertNotEqual(res["options"]["transport"], None)
        self.assertNotEqual(res["options"]["protocol"], None)
        self.assertNotEqual(res["connect_options"]["port"], None)
        self.assertNotEqual(res["connect_options"]["clean_start"], None)
        self.assertNotEqual(res["connect_options"]["properties"], None)
        self.assertNotEqual(res["connect_options"]["keepalive"], None)

    @mock.patch("output_unit.mqtt_client.properties")
    def test_get_options_v5(self, mock_props):
        "Method that checks the get_options method"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 5
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)
        mock_props.SessionExpiryInterval.return_value = 30 * 60
        props = mock_props.return_value
        with mock.patch.object(
            TestMQTTAdapter.mqtt_adapter.client, "_MQTTClient__get_v5_options"
        ) as mock_v5_func:

            res = TestMQTTAdapter.mqtt_adapter.client._MQTTClient__get_options(
            )
            mock_v5_func.assert_called_with(props)
            self.assertNotEqual(res, None)

    def test_get_options_v3(self):
        "Method that checks the get_options method"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 3
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)

        with mock.patch.object(
            TestMQTTAdapter.mqtt_adapter.client, "_MQTTClient__get_v3_options"
        ) as mock_v3_func:

            res = TestMQTTAdapter.mqtt_adapter.client._MQTTClient__get_options()
            mock_v3_func.assert_called_with()
            self.assertNotEqual(res, None)

    def test_get_client_and_options(self):
        "Method that checks the __get_cl_and_opts method"

        TestMQTTAdapter.mqtt_adapter.client.connection_status = (
            ConnectionStatus.CONNECTED)
        TestMQTTAdapter.adapter_config.mqtt_version = 3
        TestMQTTAdapter.mqtt_adapter.configure(self.adapter_config)

        with mock.patch.object(
            TestMQTTAdapter.mqtt_adapter.client, "_MQTTClient__get_options"
        ) as mock_opt_func:

            client, opts = (
                TestMQTTAdapter.mqtt_adapter.client._MQTTClient__get_cl_and_opts()
            )
            mock_opt_func.assert_called_with()
            self.assertNotEqual(client, None)
            self.assertNotEqual(opts, None)
            self.assertIsInstance(client, mqtt.Client)


class TestFluentdAdapter(unittest.TestCase):
    """Test if the mqtt adapter in the output unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        fluentd_cfg_dict = {
            "instance_address": "127.0.0.1",
            "instance_port": 24224,
            "log_index": "sdwan",
            "log_app": "SDEP.RLF",
            "buf_max": 10 * 1024**2,
        }
        adapter_config = FluentdAdapterConfig.parse_obj(fluentd_cfg_dict)

        cls.fluentd_adapter = FluentdAdapter()
        cls.adapter_config = adapter_config

    @classmethod
    def tearDownClass(cls):
        """Tear down method"""

        del TestFluentdAdapter.fluentd_adapter

    def test_init(self):
        """Method that tests that after initialisation of a fluentd client
        the latter is configured"""

        TestFluentdAdapter.adapter_config.instance_address = "127.0.0.1"
        TestFluentdAdapter.adapter_config.instance_port = 24224
        TestFluentdAdapter.adapter_config.log_index = "sdwan"
        TestFluentdAdapter.adapter_config.log_app = "SDEP.RLF"
        TestFluentdAdapter.adapter_config.buf_max = 10 * 1024**2

        adapter = FluentdAdapter()

        self.assertEqual(
            adapter.instance_address, TestFluentdAdapter.adapter_config.instance_address
        )
        self.assertEqual(
            adapter.instance_port, TestFluentdAdapter.adapter_config.instance_port
        )
        self.assertEqual(adapter.log_index, TestFluentdAdapter.adapter_config.log_index)
        self.assertEqual(adapter.log_app, TestFluentdAdapter.adapter_config.log_app)
        self.assertEqual(adapter.buf_max, TestFluentdAdapter.adapter_config.buf_max)

        self.assertNotEqual(adapter.sender, None)
        self.assertIsInstance(adapter.sender, sender.FluentSender)

    def test_configure(self):
        """Method that tests that after configuration of a fluentd client
        the latter is configured"""

        TestFluentdAdapter.adapter_config.instance_address = "127.0.0.2"
        TestFluentdAdapter.adapter_config.instance_port = 24225
        TestFluentdAdapter.adapter_config.log_index = "sdwan2"
        TestFluentdAdapter.adapter_config.log_app = "SDEP.RLF2"

        TestFluentdAdapter.fluentd_adapter.configure(TestFluentdAdapter.adapter_config)

        self.assertEqual(
            TestFluentdAdapter.fluentd_adapter.instance_address,
            TestFluentdAdapter.adapter_config.instance_address,
        )
        self.assertEqual(
            TestFluentdAdapter.fluentd_adapter.instance_port,
            TestFluentdAdapter.adapter_config.instance_port,
        )
        self.assertEqual(
            TestFluentdAdapter.fluentd_adapter.log_index,
            TestFluentdAdapter.adapter_config.log_index,
        )
        self.assertEqual(
            TestFluentdAdapter.fluentd_adapter.log_app,
            TestFluentdAdapter.adapter_config.log_app,
        )

        self.assertNotEqual(TestFluentdAdapter.fluentd_adapter.sender, None)
        self.assertIsInstance(
            TestFluentdAdapter.fluentd_adapter.sender, sender.FluentSender
        )

    def test_configure_sender(self):
        "Method that checks the __configure_sender method"

        TestFluentdAdapter.fluentd_adapter.sender.host = "127.0.0.5"

        TestFluentdAdapter.fluentd_adapter._FluentdAdapter__configure_sender()

        self.assertIsInstance(
            TestFluentdAdapter.fluentd_adapter.sender, sender.FluentSender
        )
        self.assertNotEqual(TestFluentdAdapter.fluentd_adapter.sender.host, "127.0.0.5")

    @mock.patch("output_unit.adapters.logging")
    def test_overflow_handler(self, mock_logs):
        "Method that checks if  overflow handler is working properly"

        TestFluentdAdapter.fluentd_adapter._FluentdAdapter__overflow_handler("")
        mock_logs.debug.assert_called_with(
            "An overflow on the fluentd sender buffer has occured"
        )
        self.assertEqual(TestFluentdAdapter.fluentd_adapter.overflow_flag, True)

    def test_write_data(self):
        "Method that checks if write_data is working properly"

        with mock.patch.object(
            TestFluentdAdapter.fluentd_adapter.sender, "clear_last_error"
        ) as mock_clear_last_err_func:
            with mock.patch.object(
                TestFluentdAdapter.fluentd_adapter.sender, "emit_with_time"
            ) as mock_emit_func:
                with mock.patch.object(
                    TestFluentdAdapter.fluentd_adapter.sender, "close"
                ) as mock_close_func:
                    TestFluentdAdapter.fluentd_adapter.overflow_flag = True
                    mock_emit_func.return_value = False
                    TestFluentdAdapter.fluentd_adapter.write_data("some data")
                    mock_clear_last_err_func.assert_called_with()
                    self.assertEqual(
                        TestFluentdAdapter.fluentd_adapter.overflow_flag, False
                    )
                    mock_close_func.assert_called_with()


def get_file_count(path_dir: str) -> int:
    """Method to get number of files in a directory"""

    return len(
        [
            name
            for name in os.listdir(path_dir)
            if os.path.isfile(os.path.join(path_dir, name))
        ]
    )


@mock.patch('output_unit.otel_metrics.PeriodicExportingMetricReader', mock.MagicMock())
class TestOTELMetricsAdapter(unittest.TestCase):
    """Test correct operation of the OTELMetricsAdapter"""

    def setUp(self) -> None:
        if hasattr(MetricStore, 'instance'):
            del MetricStore.instance
        MetricStore.cleanup_thread_timer = 1
        raw_config = {
            "app": "sdwan.assurance_agent.prod1_logs_forwarder",
            "system_id": "70863818-ec7d-4638-a873-f018c62d5d3d",
            "sdep_dns": "sdep-101.shore.prod.prod2.com",
            "collector_endpoint": "sdep-101.shore.prod.prod2.com",
            "export_interval_millis": "2000",
            "export_timeout_millis": "10000",
            "enabled_exporters": "otlp"
        }
        self.adapter_config = OTELMetricsAdapterConfig.parse_obj(raw_config)
        self.adapter = adapters.OTELMetricsAdapter()

        self.valid_cfg = loads(
            '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/Loggi'
            'ngObject.cpp","Li":862},"Ct":"lw.comp.IR.snapshot","Ti":"20240217'
            'T083551.181535","Cl":{"ownr":"<IrMgr!5851>","oid":"<IrIf!5886>","'
            'info":"bfCSC=0,AddrDoms=private,public,sock=<PubDgrmComms!5897>,S'
            'vr=1","sinf":"0203 - Active","goid":"0efbd20e-9853-4582-ba95-b84c'
            'fcca8e42"},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":3302,"'
            'ctx":"<IrMgr!323222>","obj":"<IrIf!5886>","val":{"Sta":{"g":{"IfI'
            'd":"Ethernet_eth2_4031","S":{"cSt":203,"bfCSC":0,"svcCost'
            '":"1"}},"tx":{"qued":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"tuB":'
            '6710887,"Bu":{"B":10066330}}},"Sts":{"tx":{"Bu":{"tTu":10210652,"'
            'nTu":15219,"nooB":0,"tTkn":144322,"tSto":0},"d":{"nP":"TCV-NotSet'
            '","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"t'
            '":{"nP":"100000","nB":"20000"}},"rx":{"r":{"nP":"TCV-NotSet","nB"'
            ':"TCV-NotSet"},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP'
            '":"TCV-NotSet","nB":"TCV-NotSet"}}}}}}'
        )

        self.invalid_cfg = loads(
            '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/Loggi'
            'ngObject.cpp","Li":862},"Ct":"lw.comp.IR.snapshot","Ti":"20240217'
            'T083551.181535","Cl":{"ownr":"<IrMgr!5851>","oid":"<#*@!5886>","'
            'info":"bfCSC=0,AddrDoms=private,public,sock=<PubDgrmComms!5897>,S'
            'vr=1","sinf":"0203 - Active","goid":"0efbd20e-9853-4582-ba95-b84c'
            'fcca8e42"},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":3302,"'
            'ctx":"<IrMgr!323222>","obj":"<#*@!5886>","val":{"Sta":{"g":{"IfI'
            'd":"Ethernet_eth2_4031","S":{"cSt":203,"bfCSC":0,"svcCost'
            '":"1"}},"tx":{"qued":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"tuB":'
            '6710887,"Bu":{"B":10066330}}},"Sts":{"tx":{"Bu":{"tTu":10210652,"'
            'nTu":15219,"nooB":0,"tTkn":144322,"tSto":0},"d":{"nP":"TCV-NotSet'
            '","nB":"TCV-NotSet"},"p":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"t'
            '":{"nP":"100000","nB":"20000"}},"rx":{"r":{"nP":"TCV-NotSet","nB"'
            ':"TCV-NotSet"},"d":{"nP":"TCV-NotSet","nB":"TCV-NotSet"},"p":{"nP'
            '":"TCV-NotSet","nB":"TCV-NotSet"}}}}}}'
        )

        self.valid_mod = UnifiedModel.parse_obj(self.valid_cfg)
        # self.invalid_mod = UnifiedModel.parse_obj(self.invalid_cfg)

    def tearDown(self) -> None:
        del self.adapter
        del self.adapter_config
        if hasattr(MetricStore, 'instance'):
            MetricStore.keep_running = False
            del MetricStore.instance

    def test_init(self):
        """Test the dunder init method"""

        self.assertIsInstance(self.adapter.exporters, Dict)
        # self.assertIsInstance(self.adapter.otel_metrics_obj, OTELMetrics)
        self.assertTrue(self.adapter.unconfigured)

    @mock.patch.object(adapters, "OTELMetrics")
    @mock.patch.object(adapters, "OTLPMetricExporter")
    def test_configure(
        self,
        mock_metric_exporter: mock.MagicMock,
        mock_otel_metrics: mock.MagicMock,
    ):
        """Test the configure method"""

        mock_metric_exporter.return_value = exporter = mock.MagicMock()
        with mock.patch.object(self.adapter, 'metrics_definition_path') as mck_path:

            mock_otel_metrics.return_value = mock.MagicMock()

            self.adapter.configure(self.adapter_config)

            mock_metric_exporter.assert_called_once()
            mock_otel_metrics.assert_called_once_with(
                mck_path, self.adapter_config, {"otlp": exporter}, 
                ota_instruments_config=None, 
                ota_resource_attributes=None
            )
            self.assertFalse(self.adapter.unconfigured)

    @mock.patch.object(OTELMetrics, "rec_getattr")
    @mock.patch.object(OTELMetrics, "set_metric_attributes")
    def test_write_data(
        self,
        set_metric_moc,
        get_attr_mock,
    ):
        "test writing data with adaptor"

        cfg_file = Path(__file__).parent / "otel_adpt.json"
        self.adapter.unconfigured = False
        self.adapter.otel_metrics_obj = OTELMetrics(
            Path(cfg_file),
            self.adapter_config,
            mock.MagicMock(),
            False,
        )
        # get_objid_mock.return_value = "IrIf"
        get_attr_mock.return_value = "100"
        set_metric_moc.return_value = ({}, True)
        self.adapter.write_data(self.valid_cfg)
        # get_objid_mock.assert_called_with(self.valid_mod.snapshot.obj)

    def test_convert_to_model_msg(self):
        """Test to convert a message to a UnifiedModel"""
        snapshot = self.adapter.convert_to_model_msg(self.valid_cfg)
        self.assertEqual(snapshot.__class__, UnifiedModel)
