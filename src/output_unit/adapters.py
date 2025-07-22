"""This module is the Adapters for the RazorLink Logs and Metrics
Forwarder.
"""
import inspect
import json
import logging as logs
import random
import string
import sys
import time
from abc import ABC, abstractmethod
from logging.handlers import RotatingFileHandler
from os import getenv
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Union

import paho.mqtt.client as mqtt  # type: ignore
import product_common_logging as logging  # type: ignore
from fluent import asyncsender
from opentelemetry.exporter.otlp.proto.http.metric_exporter import \
    OTLPMetricExporter
from paho.mqtt.packettypes import PacketTypes as packettypes  # type: ignore
from paho.mqtt.properties import Properties as properties  # type: ignore
from pydantic import ValidationError

from config_manager.models import (FileAdapterConfig, FluentdAdapterConfig,
                                   MetricTransformConfig, MQTTAdapterConfig,
                                   OTELMetricsAdapterConfig,
                                   PrometheusAdapterConfig)
from data_processing.exit_transform_loop_exception import \
    ExitTransformLoopException
from data_processing.models.unified_model import UnifiedModel
from input_unit.shared_data import MessageType
from output_unit import log_data_transforms
from output_unit.metric_storage import MetricStore
from output_unit.mqtt_client import ConnectionStatus, MQTTClient
from output_unit.mqtt_exporter import MQTTExporterV3
from output_unit.otel_metrics import OTELMetrics


def get_transform_functions(module) -> List[Tuple[str, Callable]]:
    """Return a list of tuples. Each tuple has on 0 the name of func and on
    1 the function reference."""
    return [f for f in inspect.getmembers(module, inspect.isfunction)
            if not f[0].startswith("__")]


TRANSFORM_FUNCTIONS_MAP = {
    MessageType.LOG: get_transform_functions(log_data_transforms),
    MessageType.METRIC: [],
}


class Adapter(ABC):
    """Abstract base class for Adapter. This is the interface for
    the following classes: MQTTAdapter, FileAdapter, FluentdAdapter
    and PrometheusAdapter.
    Each of the following classes should implement these methods:
        `configure` and `write_data`.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def configure(self, adapter_config: Any):
        """Abstract method for configuring the Adapter. This will be
        overwritten."""

    @abstractmethod
    def write_data(self, data: Union[str, Any]):
        """Abstract method for writing data. This will be overwritten."""


class MQTTAdapterBase(Adapter):
    """MQTTAdapter will connect to message broker MQTT.
    """

    topic = ""

    def __init__(self) -> None:
        """Class init method"""
        self.trans_config = MetricTransformConfig(
            loss_use_bytes=False,
            include_timedout_in_loss=True,
            include_outbound_bytecount=True,
            include_outbound_pktcount=False,
            include_inbound_pktcount=False,
            include_inbound_bytecount=True,
        )
        self._opts: Dict = {}
        self._srv_name = '-'.join(["prod1_logs_forwarder", ''.join(
            random.SystemRandom().choice(
                string.ascii_uppercase + string.digits) for _ in range(10))])
        self.client = MQTTClient(self._srv_name)
        super().__init__()

    def configure(self, adapter_config: MQTTAdapterConfig):
        """Method that configures the mqtt client. This can raise
        ValueError exceptions that need to be handled. It also
        enables a background thread for trying to connect to
        broker for the first time"""

        self.client.configure(adapter_config)

    def write_data(self, data: Union[str, Any]):
        """Method that writes metric or log entries to the mqtt broker"""

        if isinstance(data, UnifiedModel):
            return
        else:
            data = self.__extract_data(
                data, TRANSFORM_FUNCTIONS_MAP[MessageType.LOG])

        if not data:
            return

        if self.client.connection_status == ConnectionStatus.CONNECTED:
            props = properties(packettypes.PUBLISH)
            props.MessageExpiryInterval = 30
            self.client.mqtt_client.publish(
                self.topic, json.dumps(data), qos=2, properties=props)
        else:
            logging.debug("MQTT Client is not connected")

    def __extract_data(
            self, data, transforms: List[Tuple[str, Callable]]):

        msg = data
        for func_name, func in transforms:
            try:
                msg = func(msg, self.trans_config)
            except ExitTransformLoopException:
                msg = {}
                break
            except Exception as err:
                logging.warning(
                    "Transform function %s raised exception %s", func_name,
                    err
                )
                msg = {}
                break
        return msg


class MQTTLogsAdapter(MQTTAdapterBase):
    """An MQTT Adapter for the logs topic"""

    topic = "log/network/sdwan/uncategorised/"


class MQTTMetricsAdapter(MQTTAdapterBase):
    """An MQTT Adapter for the metrics topic"""

    topic = "network/sdwan/uncategorised/"


class FileAdapterBase(Adapter):
    """FileAdapter will write the stream from output buffers to
    a file stored on Edge.
    """

    file_path = ""

    def __init__(self) -> None:
        self.file_logger = logs.getLogger(self.__class__.__name__)
        super().__init__()

    def configure(self, adapter_config: FileAdapterConfig):
        """Method that configures the logger. This
        can raise exceptions that need to be handled by
        the upper layers"""

        file_path = adapter_config.file_path / self.__class__.file_path
        file_path.parent.mkdir(exist_ok=True, parents=True)
        file_handler = RotatingFileHandler(
            file_path,
            mode='a',
            maxBytes=adapter_config.rolling_file_max_bytes,
            backupCount=adapter_config.rolling_file_count,
            encoding='utf-8',
            delay=False
        )
        formatter = logs.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        self.file_logger = logs.getLogger(self.__class__.__name__)
        self.file_logger.setLevel(logs.INFO)
        self.file_logger.addHandler(file_handler)

    def write_data(self, data: Union[str, Any]):
        """Method that writes to file"""
        if isinstance(data, UnifiedModel):
            data = data.dict()
        if len(data):
            self.file_logger.info(data)


class LogsFileAdapter(FileAdapterBase):
    """Logs file adapter."""

    file_path = "logs/prod1.log"


class MetricsFileAdapter(FileAdapterBase):
    """Metrics file adapter."""

    file_path = "metrics/prod1.log"


class FluentdAdapter(Adapter):
    """FluentdAdapter will write the stream from output buffers to
    Fluentd Data Collector."""

    def __init__(self) -> None:
        self.instance_port = 24224
        self.instance_address = "127.0.0.1"
        self.log_index = 'sdwan'
        self.log_app = 'SDEP.RLF'
        self.overflow_flag = False
        self.buf_max = 10 * 1024**2
        self.queue_circular = False
        self.sender = asyncsender.FluentSender(self.log_app)

    def configure(self, adapter_config: FluentdAdapterConfig):
        """Method that configures the fluentd global sender"""

        self.instance_address = adapter_config.instance_address
        self.instance_port = adapter_config.instance_port
        self.log_index = adapter_config.log_index
        self.log_app = adapter_config.log_app
        self.buf_max = adapter_config.buf_max
        self.queue_circular = adapter_config.queue_circular
        self.__configure_sender()

    def write_data(self, data: Union[str, Any]):
        """Method that writes log data to the fluentd instance"""

        if sys.getsizeof(data) > self.buf_max:
            message = {"message": "Received log exceeding max buf size. Dropping log"}

            logging.warn("Received log exceeding max buf size. Dropping log")
            self.__emit_message(data=message)
        else:
            self.__emit_message(data=data)

        if self.overflow_flag and not self.queue_circular:
            self.overflow_flag = False
            self.sender.close()
            self.__configure_sender()

    def __emit_message(self, data):
        """Helper method to emit fluentd messages"""
        cur_time = int(time.time())

        if not self.sender.emit_with_time(self.log_index, cur_time, data):
            logging.debug(self.sender.last_error)
            self.sender.clear_last_error()
        else:
            logging.debug("data sent with fluentd adapter")

    def __overflow_handler(self, pendings):
        """Method that handles overflow errors when emit is
        not able to transmit them and the buffer overflows
        At the moment it closes the sender thus purging buffer and it
        restarts it"""

        logging.debug("An overflow on the fluentd sender buffer has occured")
        self.overflow_flag = True

    def __configure_sender(self):
        """Method to Initialise sender instance"""

        self.sender = asyncsender.FluentSender(
            self.log_app,
            host=self.instance_address,
            port=self.instance_port,
            nanosecond_precision=True,
            buffer_overflow_handler=self.__overflow_handler,
            bufmax=self.buf_max,
            queue_circular=self.queue_circular)
        logging.info("fluentd adapter configured with: "
                     f"{self.instance_address}/{self.instance_port}")

    def __del__(self):
        """Method to cleanup resources"""

        logging.info("Cleaning up sender resources.")
        self.sender.close()


class PrometheusAdapter(Adapter):
    """PrometheusAdapter will write the stream from output buffers to
    Prometheus."""

    def configure(self, adapter_config: PrometheusAdapterConfig):
        pass

    def write_data(self, data: Union[str, Any]):
        pass


class OTELMetricsAdapter(Adapter):
    """OTELMetricsAdapter will periodically export pre-defined metrics to
    OTEL collector."""
    def __init__(self, path: Union[Path, None] = None):
        self.exporters: Dict = {}
        self.otel_metrics_obj: OTELMetrics

        if path:
            self.metrics_definition_path = path
        else:
            self.metrics_definition_path = (
                Path(getenv("RLF_CONFIG", '/opt/rlf')) / "otel.json")

        self.unconfigured = True
        self.byte_counter = None

    def configure(self, adapter_config: OTELMetricsAdapterConfig,
                  instruments_config: Union[Dict, None] = None,
                  resource_attributes: Union[Dict, None] = None):  # type: ignore
        if resource_attributes:
            adapter_config.system_id = resource_attributes.get("system-id")

        for index, exporter in enumerate(adapter_config.enabled_exporters):
            if exporter == "otlp":
                self.exporters[exporter] = OTLPMetricExporter(
                    endpoint=adapter_config.collector_endpoint,
                    certificate_file=adapter_config.certificate_file,
                    timeout=adapter_config.export_timeout_millis[index],
                )
            elif exporter == "mqtt":
                self.exporters[exporter] = MQTTExporterV3(
                    MQTTAdapterConfig(
                        mqtt_version=adapter_config.mqtt_version,
                        broker_port=adapter_config.mqtt_port,
                        broker_address=adapter_config.mqtt_address)
                    )
        self.otel_metrics_obj = OTELMetrics(
            self.metrics_definition_path,
            adapter_config,
            self.exporters,
            ota_instruments_config=instruments_config,
            ota_resource_attributes=resource_attributes)

        self.unconfigured = False

    def write_data(self, data: Union[str, Any]):
        if self.unconfigured:
            return

        if isinstance(data, dict):
            data = self.convert_to_model_msg(data)

        if not isinstance(data, UnifiedModel):
            return

        for metric in (
                self.otel_metrics_obj.metric_store.get_meter_points(data)):
            if metric.check_if_attributes_match(self.otel_metrics_obj, data):
                metric_values = []
                for path in metric.path.paths:
                    metric_values.append(self.otel_metrics_obj.rec_getattr(
                        data, path, "None"))

                if "None" not in metric_values:
                    met_val_lst = metric.apply_path_helper_func_if_any(
                        metric_values)
                    for met_val in met_val_lst:
                        attributes, attributes_set = (
                            self.otel_metrics_obj.set_metric_attributes(
                                data, metric,
                                att_dct=met_val["att_dct"] if "att_dct" in met_val else {}))

                        MetricStore().set_metric(metric.name, met_val["val"],
                                                 attributes, attributes_set)

        self.__set_rlf_assurance_metrics()
        
    def __set_rlf_assurance_metrics(self):
        MetricStore().set_metric(
           name="services.hosting.virtual_machine.sdwan_end_point.agent."
                "assurance.received_metrics_count",
           value=1,
           attrs={},
           use_default_attributes=True)
        MetricStore().set_metric(
                    name="services.hosting.virtual_machine.sdwan_end_point."
                         "agent.assurance.tcp_log_connection_status",
                    value=1,
                    attrs={},
                    use_default_attributes=True)

    def convert_to_model_msg(
            self, message: Dict) -> Union[UnifiedModel, None]:
        "Method to get an allowed snapshot based on object id"
        msg = None
        try:
            msg = UnifiedModel.parse_obj(message)
        except ValidationError:
            pass

        return msg
