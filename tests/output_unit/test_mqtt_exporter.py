# pylint: disable-all
import unittest

from unittest import mock
from typing import Dict

from json import loads, dumps

from config_manager.models import MQTTAdapterConfig
from output_unit.mqtt_client import ConnectionStatus, MQTTClient
from output_unit.mqtt_exporter import MQTTExporterV3, MetricExportResult
from opentelemetry.sdk.metrics._internal.point import MetricsData

from output_unit.otel_model import Metric, OTELDataModel


class TestMQTTExporter(unittest.TestCase):

    def setUp(self) -> None:
        self.data = '{"resource_metrics":[{"resource":{"attributes":{"app":"sdwan.assurance_agent.prod1_logs_forwarder","pod-id":"achourmouziadis-XPS","system-id":"cc01b3e1-1148-438c-8822-2abc117de430","sdep-dns-name":"sdep-101.shore.prod.prod2.com","kubernetes_namespace":"sdwan","location":"telco-cloud"},"schema_url":""},"scope_metrics":[{"scope":{"name":"rlf","version":"1.0","schema_url":""},"metrics":[{"name":"services.hosting.virtual_machine.sdwan_end_point.agent.assurance.received_metrics_count","description":"","unit":"1","data":{"data_points":[{"attributes":{"sdep_dns":"sdep-101-mgmt.shore.dev.prod2.com"},"start_time_unix_nano":1715616341251352820,"time_unix_nano":1715616371249661699,"value":31}],"aggregation_temporality":2,"is_monotonic":true}}],"schema_url":""},{"scope":{"name":"sdep","version":"1.0","schema_url":""},"metrics":[{"name":"system.sdwan.agent.assurance.tx_packet_counter","description":"Transmittedpacketscounter","unit":"bytes","data":{"data_points":[{"attributes":{"interface":"Ethernet_eth2_4032","context":"5000"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":380},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5000"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":411},{"attributes":{"interface":"Ethernet_eth2_4032","context":"5001"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":354},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5001"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":404}],"aggregation_temporality":2,"is_monotonic":true}},{"name":"system.sdwan.agent.assurance.rx_packet_counter","description":"Receivedpacketscounter","unit":"bytes","data":{"data_points":[{"attributes":{"interface":"Ethernet_eth2_4032","context":"5000"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5000"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4032","context":"5001"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5001"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1}],"aggregation_temporality":2,"is_monotonic":true}}],"schema_url":""}],"schema_url":""}]}'
        self.invalid_data = '{"resource_metrics":[{"a_resource":{"attributes":{"app":"sdwan.assurance_agent.prod1_logs_forwarder","pod-id":"achourmouziadis-XPS","system-id":"cc01b3e1-1148-438c-8822-2abc117de430","sdep-dns-name":"sdep-101.shore.prod.prod2.com","kubernetes_namespace":"sdwan","location":"telco-cloud"},"schema_url":""},"scope_metrics":[{"scope":{"name":"rlf","version":"1.0","schema_url":""},"metrics":[{"name":"services.hosting.virtual_machine.sdwan_end_point.agent.assurance.received_metrics_count","description":"","unit":"1","data":{"data_points":[{"attributes":{"sdep_dns":"sdep-101-mgmt.shore.dev.prod2.com"},"start_time_unix_nano":1715616341251352820,"time_unix_nano":1715616371249661699,"value":31}],"aggregation_temporality":2,"is_monotonic":true}}],"schema_url":""},{"scope":{"name":"sdep","version":"1.0","schema_url":""},"metrics":[{"name":"system.sdwan.agent.assurance.tx_packet_counter","description":"Transmittedpacketscounter","unit":"bytes","data":{"data_points":[{"attributes":{"interface":"Ethernet_eth2_4032","context":"5000"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":380},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5000"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":411},{"attributes":{"interface":"Ethernet_eth2_4032","context":"5001"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":354},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5001"},"start_time_unix_nano":1715616346266388366,"time_unix_nano":1715616371249661699,"value":404}],"aggregation_temporality":2,"is_monotonic":true}},{"name":"system.sdwan.agent.assurance.rx_packet_counter","description":"Receivedpacketscounter","unit":"bytes","data":{"data_points":[{"attributes":{"interface":"Ethernet_eth2_4032","context":"5000"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5000"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4032","context":"5001"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1},{"attributes":{"interface":"Ethernet_eth2_4033","context":"5001"},"start_time_unix_nano":1715616346266699332,"time_unix_nano":1715616371249661699,"value":1}],"aggregation_temporality":2,"is_monotonic":true}}],"schema_url":""}],"schema_url":""}]}'
        
        self.mqtt_config = MQTTAdapterConfig(
            mqtt_version=3, broker_port=1883, broker_address="127.0.0.1")
        self.mqtt_exporter = MQTTExporterV3(self.mqtt_config, False)

    def tearDown(self) -> None:
        del self.mqtt_exporter
        del self.mqtt_config

    def test_init(self):
        self.assertIsInstance(self.mqtt_exporter.client, MQTTClient)
        self.assertEqual(
            self.mqtt_exporter.client.connection_status,
            ConnectionStatus.DISCONNECTED)

    @mock.patch("opentelemetry.sdk.metrics._internal.point."
                "MetricsData.to_json")
    @mock.patch("output_unit.mqtt_exporter.logging")
    def test_get_metric_by_topic(self, log_mock, to_json_mock):
        to_json_mock.return_value = self.data
        returned_data = self.mqtt_exporter._MQTTExporterV3__get_metric_by_topic(
            MetricsData(None))
        self.assertNotEqual(returned_data, {})
        self.assertEqual(len(returned_data), 3)

        to_json_mock.return_value = self.invalid_data
        returned_data = self.mqtt_exporter._MQTTExporterV3__get_metric_by_topic(
            MetricsData(None))
        self.assertEqual(returned_data, {})
        log_mock.error.assert_called()

    @mock.patch("output_unit.mqtt_exporter.logging")
    def test_update_store_and_resource(self, log_mock):
        model = OTELDataModel.parse_obj(loads(self.data))
        metric_store: Dict[str, Metric] = {}
        metric = model.resource_metrics[0].scope_metrics[0].metrics[0]

        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics[0].metrics), 1)
        self.mqtt_exporter._MQTTExporterV3__update_store_and_resource(
            metric, (0, 0, 0), metric_store, model)
        self.assertEqual(len(metric_store), 1)
        self.assertIsInstance(
            metric_store["services/hosting/virtual_machine/"
                         "sdwan_end_point/agent/assurance/"
                         "received_metrics_count"],
            Metric)

        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics[0].metrics), 0)

        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics[1].metrics), 2)

        metric = model.resource_metrics[0].scope_metrics[1].metrics[0]
        self.mqtt_exporter._MQTTExporterV3__update_store_and_resource(
            metric, (0, 1, 0), metric_store, model)
        self.assertEqual(len(metric_store), 2)
        self.assertIsInstance(
            metric_store["system/sdwan/agent/assurance/tx_packet_counter"],
            Metric)

        metric = model.resource_metrics[0].scope_metrics[1].metrics[0]
        self.mqtt_exporter._MQTTExporterV3__update_store_and_resource(
            metric, (0, 1, 0), metric_store, model)
        self.assertEqual(len(metric_store), 3)
        self.assertIsInstance(
            metric_store["system/sdwan/agent/assurance/rx_packet_counter"],
            Metric)

        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics[1].metrics), 0)

        self.mqtt_exporter._MQTTExporterV3__update_store_and_resource(
                    metric, (0, 2, 0), metric_store, model)
        log_mock.error.assert_called()

    @mock.patch("output_unit.mqtt_exporter.logging")
    def test_remove_other_scopes(self, log_mock):
        model = OTELDataModel.parse_obj(loads(self.data))
        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics), 2)
        self.mqtt_exporter._MQTTExporterV3__remove_other_scopes(
            (0, 1), model)
        self.assertEqual(
            len(model.resource_metrics[0].scope_metrics), 1)
        self.mqtt_exporter._MQTTExporterV3__remove_other_scopes(
            (0, 1), model)
        log_mock.error.assert_called()

    @mock.patch("output_unit.mqtt_exporter.logging")
    def test_add_to_return_data_dict(self, log_mock):
        model = OTELDataModel.parse_obj(loads(self.data))
        metric_store: Dict[str, Metric] = {}
        metric = model.resource_metrics[0].scope_metrics[0].metrics[0]
        self.mqtt_exporter._MQTTExporterV3__update_store_and_resource(
            metric, (0, 0, 0), metric_store, model)
        self.mqtt_exporter._MQTTExporterV3__remove_other_scopes(
            (0, 1), model)
        ret_data = {}
        ret_data = self.mqtt_exporter._MQTTExporterV3__add_to_return_data_dict(
            metric_store, model, ret_data)
        self.assertNotEqual(ret_data, {})
        data = dumps(loads('{"resource_metrics": [{"resource": {"attributes": {"app":"sdwan.assurance_agent.prod1_logs_forwarder","pod-id":"achourmouziadis-XPS","system-id":"cc01b3e1-1148-438c-8822-2abc117de430","sdep-dns-name":"sdep-101.shore.prod.prod2.com","kubernetes_namespace":"sdwan","location":"telco-cloud"}, "schema_url": ""}, "scope_metrics": [{"scope": {"name": "sdep", "version": "1.0", "schema_url": ""}, "metrics": [{"name": "services.hosting.virtual_machine.sdwan_end_point.agent.assurance.received_metrics_count", "description": "", "unit": "1", "data": {"data_points": [{"attributes": {"sdep_dns": "sdep-101-mgmt.shore.dev.prod2.com"}, "start_time_unix_nano": 1715616341251352820, "time_unix_nano": 1715616371249661699, "value": 31}], "aggregation_temporality": 2, "is_monotonic": true}}], "schema_url": ""}], "schema_url": ""}]}'))
        self.assertEqual(ret_data[
            "services/hosting/virtual_machine/"
            "sdwan_end_point/agent/assurance/"
            "received_metrics_count"], data)
        ret_data = self.mqtt_exporter._MQTTExporterV3__add_to_return_data_dict(
            None, model, ret_data)
        log_mock.error.assert_called()

    def test_shutdown(self):
        self.mqtt_exporter.shutdown()

    def test_force_flush(self):
        val = self.mqtt_exporter.force_flush()
        self.assertEqual(val, True)

    @mock.patch("opentelemetry.sdk.metrics._internal.point."
                "MetricsData")
    def test_export(self, met_mock):
        met_mock.to_json.return_value = self.data

        res = self.mqtt_exporter.export(self.data)
        self.assertEqual(res, MetricExportResult.FAILURE)

        self.mqtt_exporter.client.connection_status = ConnectionStatus.CONNECTED
        res = self.mqtt_exporter.export(met_mock)
        self.assertEqual(res, MetricExportResult.SUCCESS)
