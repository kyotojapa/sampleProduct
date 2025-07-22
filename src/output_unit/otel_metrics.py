"""Module used to read the metrics config and
create the relevant obs or assurance metrics"""
# pylint: disable=W0612,W0212

import functools
import json
import re
import socket
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from packaging.version import Version

import product_common_logging as logging
from opentelemetry.metrics import (Counter, Histogram, Meter,
                                   ObservableCounter, ObservableGauge)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (MetricExporter,
                                              PeriodicExportingMetricReader)
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation
from opentelemetry.sdk.resources import Resource
from pydantic import BaseModel, ValidationError

from config_manager.models import OTELMetricsAdapterConfig
from controller.common import (OTELConfigNotReadException,
                               OTELConfigNotValidException, ReturnCodes)
from output_unit.meter_point import (TRANS_FUNC_MAP, MeterPoint,
                                     StatusConfig, StatusMetricStorage)
from output_unit.metric_storage import MetricStore
from output_unit.otel_conf_model import MetricPoint, OTELConfigModel, VersionConfig
from output_unit.otel_sdk_patch import AugmentedView


class OTELMetrics():
    """Class to read obs metric config and create
    the relevant obs metrics"""

    def __init__(
            self, config_path: Path, adapter_config: OTELMetricsAdapterConfig,
            exporters: Dict[str, MetricExporter], ota_instruments_config: Union[Any, None] = None,
            ota_resource_attributes: Union[Any, None] = None, keep_running: bool = True):
        self.__keep_running = keep_running
        self.meters: Dict = {}
        self.views_dict: Dict = {}
        try:
            if (not adapter_config.use_local_agent_cfg
                    and ota_instruments_config):
                config = ota_instruments_config
            else:
                config = self.read_config(config_path)

            self.otel_cfg: OTELConfigModel = self.parse_config(config)
            self.adapter_config: OTELMetricsAdapterConfig = adapter_config
        except (OTELConfigNotReadException,
                OTELConfigNotValidException) as exc:
            logging.error(
                "Failed to load otel configuration "
                f"with Exception - {exc}"
            )
            sys.exit(ReturnCodes.INVALID_CONFIG)
        else:
            self.__init_store()
            self.__init_otel_metrics(exporters, ota_resource_attributes)

    def __init_store(self):
        self.metric_store = MetricStore()
        MetricStore.purge_unused_entries_timer = (
            self.adapter_config.purge_timeout)

    def __init_otel_metrics(self, exporters, ota_resource_attributes):
        self.views_dict: Dict[str, AugmentedView]
        self.meter_provider: MeterProvider = self.setup_meter_provider(
            exporters, ota_resource_attributes)
        self.configure_metrics()
        self.metric_store.add_views(self.views_dict)

    def read_config(self, file_path: Path) -> Dict:
        """The method is used for reading otlp.json
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config = json.load(file)
        except FileNotFoundError as f_exc:
            logging.error(f"validation_error: {f_exc}")
            raise OTELConfigNotReadException from f_exc

        return config

    def parse_config(self, config: Dict):
        """Method that checks if otld config is valid
        by parsing"""

        model = None
        try:
            model = OTELConfigModel.parse_obj(config)
        except ValidationError as val_exc:
            logging.error(f"validation_error: {val_exc}")
            raise OTELConfigNotValidException from val_exc

        return model

    def setup_meter_provider(
            self, exporters: Dict[str, MetricExporter], ota_attributes) -> MeterProvider:
        """Method that sets the meter provider for otel metrics"""
        hostname_key = "pod_id"
        if self.adapter_config.location == "edge":
            hostname_key = "container_id"

        if not ota_attributes:
            ota_attributes = dict()
        ota_attributes[hostname_key] = socket.gethostname()
        if self.adapter_config.location != "edge":
            ota_attributes["sdep_dns_name"] = self.adapter_config.sdep_dns
        resource = Resource(attributes=ota_attributes)

        readers = []
        for index, exporter in enumerate(exporters.values()):
            readers.append(
                PeriodicExportingMetricReader(
                    exporter=exporter,
                    export_interval_millis=(
                        self.adapter_config.export_interval_millis[index]),
                    export_timeout_millis=(
                        self.adapter_config.export_timeout_millis[index]),
                )
            )

        views, self.views_dict = self.create_views()
        meter_provider = MeterProvider(
            resource=resource, metric_readers=readers,
            views=views
        )

        return meter_provider

    def create_views(self) -> (
            Tuple[List, Union[Dict[str, AugmentedView], Dict]]):
        "Method to create a view for each of the instruments to be used"

        views_dict = {}
        views = []
        for _, meter in enumerate(self.otel_cfg.obs_met):
            for _, point in enumerate(meter.met_points):
                v_matches = self.check_current_version(
                    point.rlf_version.from_v, point.rlf_version.to_v)
                if v_matches:
                    vw = self.get_view_per_type(point, meter)
                    if vw:
                        views_dict[point.otel_name] = vw
                        views.append(vw)

        return views, views_dict

    def get_instrument_type(self, inst_type: str):
        """Method that returns OTEL type per instrument type"""

        if inst_type == "Histogram":
            return Histogram
        if inst_type == "ObservableGauge":
            return ObservableGauge
        if inst_type == "ObservableCounter":
            return ObservableCounter
        if inst_type == "Counter":
            return Counter

        return None

    def get_view_per_type(
            self, point: MetricPoint, meter) -> Union[
                AugmentedView, None]:
        """Method that returns a view per instrument type"""

        if self.get_instrument_type(point.type) is Histogram and (
                point.histogram_boundaries != []):
            return AugmentedView(
                instrument_type=Histogram, instrument_name=point.otel_name,
                meter_name=meter.scope,
                use_values_as_keys=True,
                aggregation=ExplicitBucketHistogramAggregation(
                    tuple(i for i in point.histogram_boundaries)))
        elif self.get_instrument_type(point.type):
            return AugmentedView(
                instrument_type=self.get_instrument_type(point.type),
                instrument_name=point.otel_name,
                meter_name=meter.scope, use_values_as_keys=True)

        return None

    def create_meter(self, name="my.meter.name", version=None,
                     schema_url=None) -> Meter:
        """Method to create a meter through which metrics can be defined"""

        return self.meter_provider.get_meter(name, version, schema_url)

    def configure_metrics(self):
        """Method to create the meters and metric points based on config"""

        for meter_def in self.otel_cfg.obs_met:
            if meter_def.scope not in self.meters:
                self.meters[meter_def.scope] = meter = self.create_meter(
                    name=meter_def.scope, version=meter_def.scope_version)
                for met_point in meter_def.met_points:
                    self.metric_point_factory(meter, met_point)

    def check_current_version(self, from_version: str, to_version: str, otel_name: str = "") -> bool:
        """Check if the incoming config matches the current RLF version."""
        if (
            Version(from_version)
            <= Version(self.adapter_config.running_rlf_version)
            <= Version(to_version)
        ):
            logging.info(f"Current version {self.adapter_config.running_rlf_version} is within the range {from_version} to {to_version} for otel_name {otel_name}")
            return True
        return False

    def metric_point_factory(self, meter: Meter, metric_point: MetricPoint):
        """Factory method to create metrics."""
        v_matches = self.check_current_version(
            metric_point.rlf_version.from_v, metric_point.rlf_version.to_v,
            metric_point.otel_name)
        if v_matches:
            if metric_point.type == 'Counter':
                return self.create_counter(meter, metric_point)
            if metric_point.type == 'ObservableGauge':
                return self.create_obs_gauge(meter, metric_point)
            if metric_point.type == 'ObservableCounter':
                return self.create_obs_counter(meter, metric_point)
            if metric_point.type == 'Histogram':
                return self.create_histogram(meter, metric_point)
        return

    def create_counter(self, meter: Meter, met_point: MetricPoint):
        """Method to create a counter"""

        counter = meter.create_counter(
            met_point.otel_name,
            unit=met_point.unit, description=met_point.description
        )
        meter_data = self.get_meter_point(met_point, counter)
        self.metric_store.add_metric(met_point.otel_name, meter_data)

    def create_obs_gauge(self, meter: Meter, met_point: MetricPoint):
        """Method to create an obs gauge"""

        clbk_data = self.get_meter_point(met_point, 0)
        meter.create_observable_gauge(
            met_point.otel_name, callbacks=[self.metric_callback(
                met_point.otel_name, "ObservableGauge")],
            unit=met_point.unit, description=met_point.description
        )
        self.metric_store.add_metric(met_point.otel_name, clbk_data)

    def create_obs_counter(self, meter: Meter, met_point: MetricPoint):
        """Method to create an obs counter"""

        clbk_data = self.get_meter_point(met_point, 0)
        meter.create_observable_counter(
            met_point.otel_name, callbacks=[self.metric_callback(
                met_point.otel_name, "ObservableCounter")],
            unit=met_point.unit, description=met_point.description
        )
        self.metric_store.add_metric(met_point.otel_name, clbk_data)

    def create_histogram(self, meter: Meter, met_point: MetricPoint):
        """Method to create a histogram"""

        histogram = meter.create_histogram(
            met_point.otel_name,
            unit=met_point.unit, description=met_point.description,
        )
        metric_data = self.get_meter_point(met_point, histogram)
        self.metric_store.add_metric(met_point.otel_name, metric_data)

    def get_meter_point(
        self, met_point: MetricPoint, value: Union[int, float,
                                                   Counter, Histogram]
    ) -> MeterPoint:
        """Method to get a metric dict with a metric name,
        a value to set and its attributes"""

        purge_unused_attributes = True
        is_in_aggr_format = True

        if met_point.is_in_aggr_format.lower() == "false":
            is_in_aggr_format = False

        if met_point.purge_unused_attributes.lower() == "false":
            purge_unused_attributes = False

        status_config = StatusConfig(
            status_store_setup=self.__set_status_setup_if_status_metric(
                met_point),
            status_if_type=met_point.status_cfg.status_if_type,
            status_inst_type=met_point.status_cfg.status_inst_type,
            vrf_id=met_point.status_cfg.vrf_id,
            if_id=met_point.status_cfg.if_id)

        return MeterPoint(
            name=met_point.otel_name,
            path=met_point.metric_path,
            value=value,
            attributes=met_point.attributes,
            trans_func=met_point.trans_func,
            obj_id=met_point.obj_id,
            type=met_point.type,
            is_in_aggr_format=is_in_aggr_format,
            purge_unused_attributes=purge_unused_attributes,
            match_attributes=met_point.match_attributes,
            status_config=status_config,
            attribute_maps=met_point.attribute_maps,  # type: ignore
        )

    def __set_status_setup_if_status_metric(self, met_point: MetricPoint):
        """Initialised the obs store of a meter point if this
        is a status metric

        Args:
            met_point (MetricPoint): a metric point based on otel
            conf model as read by the otel.json config

        Returns:
            None|StatusmetricStorage: Return the setup for status metric
            storage for a status instrument
        """

        if (met_point.status_cfg != StatusConfig() and
                met_point.status_cfg.status_inst_type):

            strg = StatusMetricStorage(
                pos_w_adj_factor=met_point.status_cfg.pos_w_adj_factor,
                neg_w_adj_factor=met_point.status_cfg.neg_w_adj_factor,
                store_size=met_point.status_cfg.store_size,
                full_history_required=(
                    met_point.status_cfg.full_history_required),
                confidence_index=met_point.status_cfg.confidence_index,
                record_interval=met_point.status_cfg.record_interval,
                rtt_cfg=met_point.status_cfg.rtt_cfg,
                rtt_store_size=met_point.status_cfg.rtt_store_size,
                rtt_stop_calc=met_point.status_cfg.rtt_stop_calc,
                status_limits=met_point.status_cfg.status_limits
            )

            return strg

        return StatusMetricStorage()

    def metric_callback(self, metric_name: str, inst_type: str):
        """Method used for obs metrics to yield values"""

        options = yield

        while self.__keep_running:
            try:
                observations = self.metric_store.get_observations(
                    metric_name, inst_type)
                options = yield observations
            except (StopIteration, AttributeError, KeyError) as iter_exc:
                logging.error(iter_exc)

    def rec_getattr(self, model, path: str, *args) -> Any:
        """Gets an attribute from a model by following the dot notation
        Without a default value in args it can raise an
        AttributeError exception if the attribute is not found.
        If a default value is provided it returns the default value

        Args:
            model (BaseModel): The pydantic model object
            path (str): The path to follow in the model

        Returns:
            attr : The value extracted from the pydantic model
        """

        def _getattr(model, path):
            try:
                return getattr(model, path, *args)
            except AttributeError as err:
                logging.error(f"{err}")
        return functools.reduce(_getattr, [model] + path.split('.'))

    def rec_getattr_wlist(self, model, path: str, *args) -> Any:
        """Gets an attribute from a model by following the dot notation
        in the path provided. Without a default value in args it can
        raise anAttributeError exception if the attribute is not found.
        If a default value is provided it returns the default value
        This also supports models that include lists
        in the following format a.b.[xx].c where xx is the index in the
        list object found in the model using the path provided
        Args:
            model (BaseModel): The pydantic model object
            path (str): The path to follow in the model

        Returns:
            attr : The value extracted from the pydantic model
        """

        def _getattr(model, path):
            pattern = re.compile(r"\[(\d+)\]")
            try:
                match = pattern.match(path)
                if match and match.groups() and (
                        int(match.group(1)) < len(model)):
                    return model[int(match.group(1))]

                return getattr(model, path, *args)
            except AttributeError:
                pass
        return functools.reduce(_getattr, [model] + path.split('.'))

    def rec_setattr(self, model, path: str, val):
        """Sets an attribute by following the dot notation of the path
        in the model object provided"""

        pre, _, post = path.rpartition('.')
        return setattr(self.rec_getattr(model, pre) if pre else model,
                       post, val)

    def set_metric_attributes(
            self, validated_message: BaseModel, metric, att_dct={}):
        """Method that sets the attributes that will be used
        as part of an otel metric either by using the static
        values defined in the config or by extracting and
        processing them from the validated message

        Args:
            validated_message (BaseModel): the validated message received
            metric (Gauge, Counter): The gauge or counter that contains
                the attribute config
            att_dct:  Dictionary with fixed attribute values


        Returns:
            attributes (Dict): The dictionary of attributes to configure
            attributes_set (boolean): a boolean flag to indicate if all
            metrics were processed
        """
        attributes = {}
        attributes_set = True
        try:
            for attr_name, attr_val in metric.attributes.items():
                if attr_val.target:  # if attr target exists
                    met_val = self.rec_getattr(
                        validated_message, attr_val.target, "None")
                    if met_val == "None":  # if no attr target value
                        attributes_set = False
                    else:  # if found attribute value
                        if attr_val.trans_func == "":
                            attr_val.value = met_val
                        elif attr_val.trans_func in TRANS_FUNC_MAP:
                            attr_val.value = (
                                TRANS_FUNC_MAP[attr_val.trans_func](
                                    met_val))

                        attributes[attr_name] = attr_val
                elif att_dct:
                    if attr_name in att_dct:
                        attr_val.value = att_dct[attr_name]
                        attributes[attr_name] = attr_val
                    else:
                        attributes_set = False
        except (KeyError, AttributeError):
            attributes = {}
            attributes_set = True

        return attributes, attributes_set
