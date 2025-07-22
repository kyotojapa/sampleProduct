"""Module that implements an exporter for mqtt"""


from json import loads, dumps
from typing import Dict, Union, Tuple


import product_common_logging as logging
from opentelemetry.sdk.metrics._internal.aggregation import \
    AggregationTemporality
from opentelemetry.sdk.metrics._internal.point import MetricsData
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult
from opentelemetry.sdk.metrics.view import Aggregation
from paho.mqtt.packettypes import PacketTypes as packettypes  # type: ignore
from paho.mqtt.properties import Properties as properties
from pydantic import ValidationError  # type: ignore

from config_manager.models import MQTTAdapterConfig
from output_unit.mqtt_client import MQTTClient, ConnectionStatus
from output_unit.otel_model import OTELDataModel, Metric


class MQTTExporterV3(MetricExporter):
    """Implementation of MetricExporter that pushes metrics to a mosquitto
    broker.
    """
    def __init__(
            self,
            mqtt_config: MQTTAdapterConfig,
            start_connect_thread: bool = True,
            preferred_temporality: Dict[type, AggregationTemporality] = {},
            preferred_aggregation: Dict[type, Aggregation] = {}) -> None:
        """Class init method"""

        self.client = MQTTClient("prod1_logs_forwarder_mqtt_exporter")
        self.client.configure(
            mqtt_config, start_connect_thread=start_connect_thread)
        super().__init__(
            preferred_temporality=preferred_temporality,
            preferred_aggregation=preferred_aggregation,
        )

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10000,
               **kwargs) -> MetricExportResult:
        """export method of the MQTT metrics exporter"""

        if self.client.connection_status == ConnectionStatus.CONNECTED:

            props = properties(packettypes.PUBLISH)
            props.MessageExpiryInterval = 30
            topic_dict = self.__get_metric_by_topic(
                metrics_data)

            for k, y in topic_dict.items():
                self.client.mqtt_client.publish(
                    k, str(y), qos=2, properties=props)
            return MetricExportResult.SUCCESS
        else:
            logging.debug("MQTT Client is not connected")

        return MetricExportResult.FAILURE

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        del self.client.mqtt_client

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def __get_metric_by_topic(self, metrics_data: MetricsData):

        returned_data: Dict[str, str] = {}

        try:
            model: OTELDataModel = OTELDataModel.parse_obj(
                loads(metrics_data.to_json()))
            for resource in model.resource_metrics:
                for scope_ind, scope in enumerate(resource.scope_metrics):
                    metric_store: Dict[str, Metric] = {}
                    model_cp = OTELDataModel.parse_obj(
                        loads(metrics_data.to_json()))

                    for metric in scope.metrics:
                        self.__update_store_and_resource(
                            metric, (0, scope_ind, 0),
                            metric_store, model_cp)

                    self.__remove_other_scopes((0, scope_ind), model_cp)
                    self.__add_to_return_data_dict(
                        metric_store, model_cp, returned_data)
        except (ValidationError, AttributeError) as err:
            logging.error(err)

        return returned_data

    def __update_store_and_resource(
            self, metric: Metric,  indices: Tuple, store: Dict[str, Metric],
            model: OTELDataModel):
        """Method that sets the topic, the metrics by topic,
        and replaces these from the resource"""

        try:
            topic = metric.name.replace(".", "/")
            store.update({topic: metric})
            model.resource_metrics[
                indices[0]].scope_metrics[indices[1]].metrics.pop(indices[2])
        except (KeyError, IndexError, AttributeError) as err:
            logging.error(err)

    def __remove_other_scopes(
            self,  indices, model: OTELDataModel):
        "Method to remove other scopes"
        try:
            model.resource_metrics[indices[0]].scope_metrics = (
                [model.resource_metrics[indices[0]].scope_metrics[indices[1]]])

        except (IndexError, AttributeError) as err:
            logging.error(err)

    def __add_to_return_data_dict(self, store, model: OTELDataModel, ret_data):
        """Method that adds a metrics data to the resource
        and store the topic as key and the resource with
        the metric as the value"""

        try:
            for k, y in store.items():
                model.resource_metrics[0].scope_metrics[0].metrics = [y]

                ret_data.update(
                    {k: dumps(loads(model.json()))})
        except (IndexError, AttributeError) as err:
            logging.error(err)

        return ret_data
