"""Module to test the OTELMetrics class"""
# pylint: disable-all
import unittest
from json import loads
from pathlib import Path
from unittest import mock

import output_unit.otel_metrics
import output_unit.transforms.otel_trans_func as otel_transforms
from config_manager.models import OTELMetricsAdapterConfig
from data_processing.models.unified_model import UnifiedModel
from controller.common import (
    OTELIngestionModelsNotLoaded,
    OTELTransformationFunctionsNotLoaded,
)

from opentelemetry.metrics import (
    Counter,
    Histogram,
    Meter,
    ObservableCounter,
    ObservableGauge,
)

from output_unit.otel_conf_model import MetricAttribute

from output_unit.meter_point import MeterPoint, get_models, get_trans_functions
from output_unit.metric_storage import MetricStore
from output_unit.otel_conf_model import MetricPath, MetricPoint, OTELConfigModel
from output_unit.otel_metrics import OTELMetrics


@mock.patch('output_unit.otel_metrics.PeriodicExportingMetricReader', mock.MagicMock())
class TestOTELMetrics(unittest.TestCase):
    """Class to perform the tests on the OTELMetrics class"""
    def setUp(self) -> None:
        if hasattr(MetricStore, 'instance'):
            del MetricStore.instance
        self.otel_config_dict = {
            "app": "sdwan.assurance_agent.prod1_logs_forwarder",
            "system_id": "cc01b3e1-1148-438c-8822-2abc117de430",
            "sdep_dns": "sdep-101.shore.prod.prod2.com",
            "collector_endpoint": "http://localhost:4318/v1/metrics",
            "export_interval_millis": "2000, 2000",
            "running_rlf_version": "0.0.111"
        }
        MetricStore.cleanup_thread_timer = 1
        otel_json_path = Path(__file__).parent / "otel.json"
        self.otel_met_obj = OTELMetrics(
            Path(otel_json_path),
            OTELMetricsAdapterConfig.parse_obj(self.otel_config_dict),
            # OTLPMetricExporter(endpoint=self.otel_config_dict['collector_endpoint']),
            mock.MagicMock(),
            False)

        dct_lst_model = {'meters': [{'scope': 'sdep-observability', 'scope_version': '1.0', 'met_points': [{'model': 'IRSnapshotLogModel', 'metric_path': {'paths': ['snapshot.val.sts.tx.t.np']}, 'otel_name': 'system.sdwan.agent.assurance.tx_packet_counter', 'type': 'Counter', 'description': 'Transmitted packets counter', 'unit': 'bytes', 'trans_func': 'parse_int', 'attributes': {'interface': {'target': 'snapshot.val.sta.g.ifid', 'value': 0}, 'context': {'target': 'snapshot.ctx', 'value': 0, 'trans_func': 'get_ir_context_id'}}}]}]}
        self.dict_list_model = OTELConfigModel.parse_obj(dct_lst_model)

    def tearDown(self) -> None:
        if hasattr(MetricStore, 'instance'):
            MetricStore.keep_running = False
            del MetricStore.instance
            del self.otel_met_obj

    def test_transformations_are_loaded(self):
        "test if transofrmation functions are loaded"

        transforms = get_trans_functions([otel_transforms])
        self.assertNotEqual(transforms, {})

    def test_models_not_loaded_raises_exception(self):
        "test if models are not loaded an exception is raised"

        with self.assertRaises(OTELIngestionModelsNotLoaded):
            models = get_models([])
            self.assertEqual(models, {})

    def test_transforms_not_loaded_raises_exception(self):
        "test if models are not loaded an exception is raised"

        with self.assertRaises(OTELTransformationFunctionsNotLoaded):
            models = get_trans_functions([])
            self.assertEqual(models, {})

    def test_rec_getattr_wlist(self):
        "test if we can navigate a dot notation hierarchy with lists"

        attr = self.otel_met_obj.rec_getattr_wlist(self.dict_list_model,
                                                   "obs_met.[0].scope")
        self.assertEqual(attr, "sdep-observability")

    def test_rec_getattr_wlist_no_default(self):
        """test if when we cannot navigate a dot notation hierarchy with lists
        an attribute exception is raised"""

        attr = self.otel_met_obj.rec_getattr_wlist(self.dict_list_model,
                                                   "obs_met.[1].scope")
        self.assertEqual(attr, None)

    def test_otel_metric_config(self):
        """Test if a config is setup properly for all options"""
        cfg_file = Path(__file__).parent / "otel.json"
        MetricStore().clean_store()
        otel_met_obj = OTELMetrics(
            Path(cfg_file),
            OTELMetricsAdapterConfig.parse_obj(self.otel_config_dict),
            mock.MagicMock(),
            # OTLPMetricExporter(endpoint="http://localhost:4318/v1/metrics"),
            False)

        self.assertEqual(len(otel_met_obj.meters), 2)
        self.assertIsInstance(otel_met_obj.meters['rlf-observability'], Meter)
        self.assertIsInstance(otel_met_obj.meters['sdep-observability'], Meter)

        self.assertEqual(len(MetricStore().value_store), 10)
        self.assertEqual(len(otel_met_obj.views_dict.values()), 10)

        self.assertIsInstance(MetricStore().value_store['counter_no_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['counter_stat_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['counter_dyn_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['gauge_no_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['gauge_stat_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['gauge_dyn_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['obs_counter_no_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['obs_counter_stat_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['obs_counter_dyn_attr'],
                              MeterPoint)
        self.assertIsInstance(MetricStore().value_store['sdep_byte_counter'],
                              MeterPoint)

        self.assertEqual(MetricStore().value_store['counter_no_attr'].path, MetricPath())
        self.assertEqual(MetricStore().value_store['counter_stat_attr'].path, MetricPath())
        self.assertEqual(MetricStore().value_store['counter_dyn_attr'].path,
                         MetricPath(paths=["snapshot.val.sts.tx.t.np"]))
        self.assertEqual(MetricStore().value_store['gauge_no_attr'].path, MetricPath())
        self.assertEqual(MetricStore().value_store['gauge_stat_attr'].path, MetricPath())
        self.assertEqual(MetricStore().value_store['gauge_dyn_attr'].path,
                         MetricPath(paths=["snapshot.val.sts.tx.t.nb"]))
        self.assertEqual(MetricStore().value_store['obs_counter_no_attr'].path, MetricPath())
        self.assertEqual(MetricStore().value_store['obs_counter_stat_attr'].path,
                         MetricPath())
        self.assertEqual(MetricStore().value_store['obs_counter_dyn_attr'].path,
                         MetricPath(paths=["snapshot.val.sts.tx.t.nb"]))

        self.assertEqual(MetricStore().value_store['counter_no_attr'].obj_id, "")
        self.assertEqual(MetricStore().value_store['counter_stat_attr'].obj_id, "")
        self.assertEqual(MetricStore().value_store['counter_dyn_attr'].obj_id,
                         "IRSnapshotLogModel")
        self.assertEqual(MetricStore().value_store['gauge_no_attr'].obj_id, "")
        self.assertEqual(MetricStore().value_store['gauge_stat_attr'].obj_id, "")
        self.assertEqual(MetricStore().value_store['gauge_dyn_attr'].obj_id,
                         "IRSnapshotLogModel")
        self.assertEqual(MetricStore().value_store['obs_counter_no_attr'].obj_id,
                         "")
        self.assertEqual(MetricStore().value_store['obs_counter_stat_attr'].obj_id,
                         "")
        self.assertEqual(MetricStore().value_store['obs_counter_dyn_attr'].obj_id,
                         "IRSnapshotLogModel")

        self.assertEqual(
            MetricStore().value_store['counter_no_attr'].trans_func, "parse_int1")
        self.assertEqual(
            MetricStore().value_store['counter_stat_attr'].trans_func, "parse_int2")
        self.assertEqual(
            MetricStore().value_store['counter_dyn_attr'].trans_func, "parse_int3")
        self.assertEqual(
            MetricStore().value_store['gauge_no_attr'].trans_func, "parse_int4")
        self.assertEqual(
            MetricStore().value_store['gauge_stat_attr'].trans_func, "parse_int5")
        self.assertEqual(
            MetricStore().value_store['gauge_dyn_attr'].trans_func,
            "parse_int6")
        self.assertEqual(
            MetricStore().value_store['obs_counter_no_attr'].trans_func,
            "parse_int7")
        self.assertEqual(
            MetricStore().value_store['obs_counter_stat_attr'].trans_func,
            "parse_int8")
        self.assertEqual(
            MetricStore().value_store['obs_counter_dyn_attr'].trans_func,
            "parse_int9")

        self.assertIsInstance(
            MetricStore().value_store['counter_no_attr'].value, Counter)
        self.assertIsInstance(
            MetricStore().value_store['counter_stat_attr'].value, Counter)
        self.assertIsInstance(
            MetricStore().value_store['counter_dyn_attr'].value, Counter)
        self.assertIsInstance(
            MetricStore().value_store['gauge_no_attr'].value, int)
        self.assertIsInstance(
            MetricStore().value_store['gauge_stat_attr'].value, int)
        self.assertIsInstance(
            MetricStore().value_store['gauge_dyn_attr'].value,
            int)
        self.assertIsInstance(
            MetricStore().value_store['obs_counter_no_attr'].value,
            int)
        self.assertIsInstance(
            MetricStore().value_store['obs_counter_stat_attr'].value,
            int)
        self.assertIsInstance(
            MetricStore().value_store['obs_counter_dyn_attr'].value,
            int)

        self.assertEqual(
            MetricStore().value_store['counter_no_attr'].attributes, {})
        self.assertNotEqual(
            MetricStore().value_store['counter_stat_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['counter_stat_attr'].attributes['attribute1'].target, "")
        self.assertEqual(
            MetricStore().value_store['counter_stat_attr'].attributes['attribute1'].value, 300)
        self.assertEqual(
            MetricStore().value_store['counter_stat_attr'].attributes['attribute1'].trans_func, "")
        self.assertNotEqual(
            MetricStore().value_store['counter_dyn_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['counter_dyn_attr'].attributes['context'].target, "snapshot.ctx")
        self.assertEqual(
            MetricStore().value_store['counter_dyn_attr'].attributes['context'].value, 0)
        self.assertEqual(
            MetricStore().value_store['counter_dyn_attr'].attributes['context'].trans_func, "get_ir_context_id")

        self.assertEqual(
            MetricStore().value_store['gauge_no_attr'].attributes, {})
        self.assertNotEqual(
            MetricStore().value_store['gauge_stat_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['gauge_stat_attr'].attributes['attribute1'].target, "")
        self.assertEqual(
            MetricStore().value_store['gauge_stat_attr'].attributes['attribute1'].value, 500)
        self.assertEqual(
            MetricStore().value_store['gauge_stat_attr'].attributes['attribute1'].trans_func, "")
        self.assertNotEqual(
            MetricStore().value_store['gauge_dyn_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['gauge_dyn_attr'].attributes['context'].target, "snapshot.ctx")
        self.assertEqual(
            MetricStore().value_store['gauge_dyn_attr'].attributes['context'].value, 0)
        self.assertEqual(
            MetricStore().value_store['gauge_dyn_attr'].attributes['context'].trans_func, "get_ir_context_id")

        self.assertEqual(
            MetricStore().value_store['obs_counter_no_attr'].attributes, {})
        self.assertNotEqual(
            MetricStore().value_store['obs_counter_stat_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['obs_counter_stat_attr'].attributes['attribute1'].target, "")
        self.assertEqual(
            MetricStore().value_store['obs_counter_stat_attr'].attributes['attribute1'].value, 700)
        self.assertEqual(
            MetricStore().value_store['obs_counter_stat_attr'].attributes['attribute1'].trans_func, "")
        self.assertNotEqual(
            MetricStore().value_store['obs_counter_dyn_attr'].attributes, {})
        self.assertEqual(
            MetricStore().value_store['obs_counter_dyn_attr'].attributes['context'].target, "snapshot.ctx")
        self.assertEqual(
            MetricStore().value_store['obs_counter_dyn_attr'].attributes['context'].value, 0.0)
        self.assertEqual(
            MetricStore().value_store['obs_counter_dyn_attr'].attributes['context'].trans_func, "get_ir_context_id")

    def test_get_instrument_type(self):
        self.assertEqual(self.otel_met_obj.get_instrument_type("Histogram"),
                         Histogram)
        self.assertEqual(
            self.otel_met_obj.get_instrument_type("ObservableGauge"),
            ObservableGauge)
        self.assertEqual(
            self.otel_met_obj.get_instrument_type("Histogram"),
            Histogram)
        self.assertEqual(
            self.otel_met_obj.get_instrument_type("ObservableCounter"),
            ObservableCounter)
        self.assertEqual(
            self.otel_met_obj.get_instrument_type("Counter"),
            Counter)
        self.assertEqual(self.otel_met_obj.get_instrument_type("Any"),
                         None)

    @mock.patch.object(output_unit.otel_metrics,
                       "ExplicitBucketHistogramAggregation")
    @mock.patch.object(output_unit.otel_metrics, "AugmentedView")
    def test_get_view_per_type(self, view_mock: mock.MagicMock,
                               hist_agg_mock: mock.MagicMock):
        met_point = MetricPoint(otel_name="a_name", type="Histogram",
                                histogram_boundaries=[1, 2, 3],
                                metric_path={"paths": ["a_path"]},
                                unit="1")
        meter = mock.MagicMock(scope="a_scope")
        self.otel_met_obj.get_view_per_type(met_point, meter)

        view_mock.assert_called_once_with(
                instrument_type=Histogram,
                instrument_name="a_name",
                meter_name="a_scope",
                use_values_as_keys=True,
                aggregation=hist_agg_mock.return_value)

    def test_set_metric_attributes(self):

        message = '{"Hd":{"Ty":"Log","P":{"Fi":"lw/shared/util/src/public/log2/LoggingObject.cpp","Li":862},"Ct":"lw.comp.ISC.context.snapshot","Ti":"20240406T122512.951206","Cl":{"ownr":"<IscCtx!2437233155>","oid":"<IscChnDgrm!2551584007>","info":"","sinf":"state=200","goid":""},"Fu":"DoSnapshotLog"}, "0":"","snapshot":{"sid":1318167,"ctx":{"ctxId":"836068f4-eb17-4501-b9d5-469ce9236bb6","oid":"<IscCtx!2437233155>"},"obj":"<IscChnDgrm!2551584007>","val":{"Sta":{"g":{"o":1,"sCW":0,"sDW":0,"lrEp":"169.254.192.1:10000","llEp":"169.254.255.254:10000","rlEp":"169.254.255.254:10000","rrEp":"169.254.192.1:10000","DSched":"fvkau3-prod2-be-default@6","CSched":"Context@15","Ch":{"Id":"5","hC":0,"hD":0}},"tx":{"eL":1,"Ch":{"I":{"iBl":1,"eP":72,"qB":0,"nP":72,"wB":0,"hP":72,"hW":0,"WlM":{"F":0,"bL":1,"Lwm":8887,"aH":0,"Hwm":10000,"aC":0,"Cwm":40448},"r":0,"dTo":1000,"Br":{"i":0,"o":0,"g":0,"n":0,"d":0}}}},"rx":{"eL":1,"Ch":{"I":{"eP":1171,"wB":0,"nP":1171,"qB":0,"hP":1171,"hW":0,"WlM":{"F":0,"bL":1,"Lwm":13000000,"aH":0,"Hwm":15000000,"aC":0,"Cwm":9223372036854775806},"Fc":{"c":"1","t":"0","n":"0"}}}}},"Sts":{"tx":{"nUr":2,"Ch":{"I":{"dBy":0,"dBl":0,"ToBy":0,"ToBl":0,"qBy":-1,"qBl":0,"tBy":72,"tFr":1,"rBy":0,"trBy":0,"trFr":0,"tcBy":16,"tcFr":1,"nUr":2,"nDs":0,"nGo":1,"nStop":0,"sFr":1,"fFr":0,"tsFr":1,"tfFr":0,"nrTr":1,"nxTr":1,"WlM":{"nHT":0}}}},"rx":{"nBp":0,"Ch":{"I":{"nF":1,"nxT":1,"WlM":{"nHT":2}}}}}}}}'

        validated_message = UnifiedModel.parse_obj(loads(message))
        context_id = MetricAttribute(
            target="snapshot.ctx.ctxId", trans_func="", value="")
        tenant_id = MetricAttribute(
            target="snapshot.val.Sta.g.DSched", trans_func="get_tenant_id",
            value="")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"context_id": context_id, "tenant_id": tenant_id})

        attributes, attributes_set = (
            self.otel_met_obj.set_metric_attributes(
                validated_message, a_metric, {}))

        self.assertEqual(attributes_set, True)
        self.assertEqual(attributes["context_id"].value,
                         "836068f4-eb17-4501-b9d5-469ce9236bb6")
        self.assertEqual(attributes["tenant_id"].value, "fvkau3")

        tenant_id = MetricAttribute(
            target="snapshot.val.Sta.g.DSched2", trans_func="get_tenant_id",
            value="")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"context_id": context_id, "tenant_id": tenant_id})
        attributes, attributes_set = (
            self.otel_met_obj.set_metric_attributes(
                validated_message, a_metric, {}))
        self.assertEqual(attributes_set, False)

        context_id = MetricAttribute(
            target="", trans_func="", value="")
        tenant_id = MetricAttribute(
            target="", trans_func="get_tenant_id",
            value="")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"context_id": context_id, "tenant_id": tenant_id})
        att_dct = {"context_id": "836068f4-eb17-4501-b9d5-469ce9236bb6",
                   "tenant_id": "fvkau3"}
        attributes, attributes_set = (
            self.otel_met_obj.set_metric_attributes(
                validated_message, a_metric, att_dct))

        self.assertEqual(attributes_set, True)
        self.assertEqual(attributes["context_id"].value,
                         "836068f4-eb17-4501-b9d5-469ce9236bb6")
        self.assertEqual(attributes["tenant_id"].value, "fvkau3")

        att_dct = {"context_id2": "836068f4-eb17-4501-b9d5-469ce9236bb6",
                   "tenant_id": "fvkau3"}
        attributes, attributes_set = (
            self.otel_met_obj.set_metric_attributes(
                validated_message, a_metric, att_dct))

        self.assertEqual(attributes_set, False)

        a_metric = mock.MagicMock(
            name="a_metric_name")
        attributes, attributes_set = (
            self.otel_met_obj.set_metric_attributes(
                validated_message, a_metric, att_dct))
        self.assertEqual(attributes_set, True)
        self.assertEqual(attributes, {})
 
    def test_check_current_version_valid(self):
        response = self.otel_met_obj.check_current_version("0.0.110", "0.0.112")
        self.assertEqual(response, True)       
        response = self.otel_met_obj.check_current_version("0.0.0", "0.0.112")
        self.assertEqual(response, True)
        response = self.otel_met_obj.check_current_version("0.0.110", "999.999.999")
        self.assertEqual(response, True)
        
    def test_check_current_version_invalid(self):
        response = self.otel_met_obj.check_current_version("0.0.112", "0.0.110")
        self.assertEqual(response, False)
        response = self.otel_met_obj.check_current_version("0.0.112", "999.999.999")
        self.assertEqual(response, False)
        response = self.otel_met_obj.check_current_version("0.0.0", "0.0.110")
        self.assertEqual(response, False)
