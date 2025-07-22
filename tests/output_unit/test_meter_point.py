# pylint: disable=missing-docstring
# pylint: disable=W0212
import unittest
import datetime
from pathlib import Path
from unittest import mock

from output_unit.meter_point import (
    AttributeMapStore,
    MapStorage,
    MeterPoint,
    StatusMetricStorage)
from output_unit.metric_storage import MetricStore
from output_unit.otel_conf_model import (
    FuncDefinition,
    MatchAttribute,
    MetricPath,
    MetricPoint,
    OTELConfigModel,
)
from output_unit.otel_metrics import OTELMetrics, OTELMetricsAdapterConfig
from pydantic import ValidationError


class TestMeterPoint(unittest.TestCase):
    def setUp(self) -> None:
        self.w_attr = MeterPoint(
            name="a_meter", path={"paths": ["a_path"]}, type="Counter", attributes={},
            match_attributes=[
                MatchAttribute(target="info1",
                               value_match="Ethernet_eth2_3917"),
                MatchAttribute(target="info2",
                               regex_match="Ethernet_eth2_4033"),
                MatchAttribute(target="info3",
                               helper_func=FuncDefinition(name="gt",
                                                          arguments=[0]))
            ])

        self.w_attr_regex_fails = MeterPoint(
            name="a_meter", path={"paths": ["a_path"]}, type="Counter",
            attributes={},
            match_attributes=[
                MatchAttribute(target="info1",
                               value_match="Ethernet_eth2_3917"),
                MatchAttribute(target="info2",
                               regex_match="("),
                MatchAttribute(target="info3",
                               helper_func=FuncDefinition(name="gt",
                                                          arguments=[0]))
            ])

        self.w_attr_func_fails = MeterPoint(
            name="a_meter", path={"paths": ["a_path"]}, type="Counter",
            attributes={},
            match_attributes=[
                MatchAttribute(target="info1",
                               value_match="Ethernet_eth2_3917"),
                MatchAttribute(target="info2",
                               regex_match="("),
                MatchAttribute(target="info3",
                               helper_func=FuncDefinition(name="gt",
                                                          arguments=[0, 3, 4]))
            ])
        self.no_att = MeterPoint(
            name="a_meter", path={"paths": ["a_path"]}, type="Counter",
            attributes={},
            match_attributes=[])

        self.w_helper_no_args = MeterPoint(
            name="a_meter",
            path={"paths": ["a_path"], "helper_func": {"name": "add"}},
            type="Counter",
            attributes={})

        self.w_helper_and_args = MeterPoint(
            name="a_meter",
            path={"paths": ["a_path"],
                  "helper_func": {"name": "add", "arguments": [1, 2]}},
            type="Counter",
            attributes={})

        self.no_helper = MeterPoint(
            name="a_meter",
            path={"paths": ["a_path"]},
            type="Counter",
            attributes={})

        self.otel_config_dict = {
            "app": "sdwan.assurance_agent.prod1_logs_forwarder",
            "system_id": "cc01b3e1-1148-438c-8822-2abc117de430",
            "sdep_dns": "sdep-101.shore.prod.prod2.com",
            "collector_endpoint": "http://localhost:4318/v1/metrics",
            "export_interval_millis": "2000, 2000",
            "running_rlf_version": "0.0.111"
        }
        self.otel_met_obj = OTELMetrics(
            Path(__file__).parents[2] / "tests" / "output_unit" / "otel.json",
            OTELMetricsAdapterConfig.parse_obj(self.otel_config_dict),
            mock.MagicMock(),
            False)

        self.status_storage = StatusMetricStorage()

    def tearDown(self) -> None:
        del self.w_attr
        del self.no_att
        del self.w_helper_no_args
        del self.no_helper
        del self.w_helper_and_args
        if hasattr(MetricStore, 'instance'):
            MetricStore.keep_running = False
            del MetricStore.instance
            del self.otel_met_obj

    def test_check_if_attributes_match_success_no_att(self):
        self.assertEqual(True,
                         self.no_att.check_if_attributes_match(None, None))

    @mock.patch("output_unit.meter_point.MeterPoint."
                "_MeterPoint__handle_ma_hfunc_case")
    @mock.patch("output_unit.meter_point.MeterPoint."
                "_MeterPoint__handle_ma_regex_case")
    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_all_called(
            self, otel_mock, reg_mock, hfunc_moc):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4033", 4]
        self.w_attr.check_if_attributes_match(self.otel_met_obj, None)
        reg_mock.assert_called_once()
        hfunc_moc.assert_called_once()

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_all_called_is_match(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4033", 4]
        res = self.w_attr.check_if_attributes_match(self.otel_met_obj,
                                                    None)
        self.assertEqual(res, True)

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_fail_at_run_1(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3918",
                                 "Ethernet_eth2_4033", 4]
        res = self.w_attr.check_if_attributes_match(self.otel_met_obj, None)
        self.assertEqual(res, False)

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_fail_at_run_2(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4032", 4]
        res = self.w_attr.check_if_attributes_match(self.otel_met_obj, None)
        self.assertEqual(res, False)

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_fail_at_run_3(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4032", -1]
        res = self.w_attr.check_if_attributes_match(self.otel_met_obj, None)
        self.assertEqual(res, False)

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_if_attributes_match_regex_fails(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4033", 4]
        res = self.w_attr_regex_fails.check_if_attributes_match(
            self.otel_met_obj, None)

        self.assertEqual(res, False)

    @mock.patch("output_unit.otel_metrics.OTELMetrics.rec_getattr")
    def test_check_func_fails(self, otel_mock):

        otel_mock.side_effect = ["Ethernet_eth2_3917",
                                 "Ethernet_eth2_4033", 4]
        res = self.w_attr_func_fails.check_if_attributes_match(
            self.otel_met_obj, None)

        self.assertEqual(res, False)

    def test_path_helper_func_if_any_success_helper_exists(self):
        res = self.w_helper_no_args.apply_path_helper_func_if_any([1, 2, 3])
        self.assertEqual(res, [{'val': 6.0}])
        res = self.w_helper_and_args.apply_path_helper_func_if_any([1, 2, 3])
        self.assertEqual(res, [{'val': 9.0}])
        res = self.no_helper.apply_path_helper_func_if_any([1])
        self.assertEqual(res, [{'val': 1.0}])
        res = self.w_helper_no_args.apply_path_helper_func_if_any({"a": "b"})
        self.assertEqual(res, [{'val': -1}])

    def test_metric_path_validation(self):
        met_path_dict_single_path_no_help = {
            "paths": ["path1"]}
        met_path_dict_multi_path_no_help = {
            "paths": ["path1", "path2"],
            "helper_func": {"name": "", "arguments": []}}
        met_path_dict_single_path_no_help_name = {
            "paths": ["path1"],
            "helper_func": {"name": "", "arguments": []}}
        met_path_dict_sgl_path_no_help_name_and_args = {
            "paths": ["path1"],
            "helper_func": {"name": "", "arguments": [1, 2]}}
        met_path_no_paths_no_helper = {
            "paths": [],
            "helper_func": {"name": "", "arguments": []}}
        met_path_no_paths_w_args = {
            "paths": [],
            "helper_func": {"name": "", "arguments": [1, 2]}}
        met_path_no_paths_w_name = {
            "paths": [],
            "helper_func": {"name": "add"}}
        met_path_no_paths_w_name_w_args = {
            "paths": [],
            "helper_func": {"name": "add", "arguments": [1, 2]}}
        try:
            MetricPath.parse_obj(met_path_dict_single_path_no_help)
            MetricPath.parse_obj(met_path_no_paths_no_helper)
            MetricPath.parse_obj(met_path_dict_single_path_no_help_name)
        except ValidationError:
            self.fail("should not raise error")

        with self.assertRaises(ValidationError):
            MetricPath.parse_obj(met_path_dict_multi_path_no_help)

        with self.assertRaises(ValidationError):
            MetricPath.parse_obj(met_path_dict_sgl_path_no_help_name_and_args)

        with self.assertRaises(ValidationError):
            MetricPath.parse_obj(met_path_no_paths_w_args)

        with self.assertRaises(ValidationError):
            MetricPath.parse_obj(met_path_no_paths_w_name)

        with self.assertRaises(ValidationError):
            MetricPath.parse_obj(met_path_no_paths_w_name_w_args)

    def test_match_attributes_validation(self):
        attr_target_and_valid_formations = [
            {"target": "a_target", "value_match": "a_match"},
            {"target": "a_target", "regex_match": "a_match"},
            {"target": "a_target", "helper_func":
                {"name": "a_name", "arguments": []}},
            {"target": "a_target", "helper_func":
                {"name": "a_name", "arguments": [1]}},
        ]
        attr_mt_empty = {"target": "", "value_match": "", "regex_match": "",
                         "helper_func":  {"name": "", "arguments": []},
                         }
        try:
            for attr in attr_target_and_valid_formations:
                MatchAttribute.parse_obj(attr)
            MatchAttribute.parse_obj(attr_mt_empty)
        except ValidationError:
            self.fail("should not raise error")

        attr_mt_no_target1 = {"target": "", "regex_match": "a_match"}
        attr_mt_no_target2 = {"target": "", "value_match": "a_match"}
        attr_mt_no_target3 = {"target": "", "helper_func":
                              {"name": "a_name", "arguments": []}}
        attr_mt_trg_no_match = {"target": "a_target", "value_match": "",
                                "regex_match": "",
                                "helper_func":  {"": "", "arguments": []}}

        with self.assertRaises(ValidationError):
            MatchAttribute.parse_obj(attr_mt_no_target1)

        with self.assertRaises(ValidationError):
            MatchAttribute.parse_obj(attr_mt_no_target2)

        with self.assertRaises(ValidationError):
            MatchAttribute.parse_obj(attr_mt_no_target3)

        with self.assertRaises(ValidationError):
            MatchAttribute.parse_obj(attr_mt_trg_no_match)

    def test_func_definition_validation(self):
        att_emtpy = {"name": "", "arguments": []}
        att_w_name = {"name": "a_name", "arguments": []}
        att_w_name_w_arg = {"name": "a_name", "arguments": [1]}

        try:
            FuncDefinition.parse_obj(att_emtpy)
            MatchAttribute.parse_obj(att_w_name)
            MatchAttribute.parse_obj(att_w_name_w_arg)
        except ValidationError:
            self.fail("should not raise error")

        att_no_name_w_arg = {"name": "", "arguments": [1]}
        with self.assertRaises(ValidationError):
            FuncDefinition.parse_obj(att_no_name_w_arg)

    def test_should_yield_validation(self):
        try:
            MetricPoint.parse_obj({
                "otel_name": "a_meter",
                "unit": "1",
                "path": {"paths": ["a_path"]},
                "type": "ObservableCounter",
                "attributes": {"attr1": {"should_yield": "False"}}})
            MetricPoint.parse_obj({
                "otel_name": "a_meter",
                "unit": "1",
                "path": {"paths": ["a_path"]},
                "type": "ObservableGauge",
                "attributes": {"attr1": {"should_yield": "False"}}})
        except ValidationError:
            self.fail("should not raise error")
        with self.assertRaises(ValidationError):
            MetricPoint.parse_obj({
                "otel_name": "a_meter",
                "unit": "1",
                "path": {"paths": ["a_path"]},
                "type": "Counter",
                "attributes": {"attr1": {"should_yield": "False"}}})

    def test_should_yield_validation2(self):
        try:
            MeterPoint.parse_obj({
                "name": "a_meter",
                "path": {"paths": ["a_path"]},
                "type": "ObservableCounter",
                "attributes": {"attr1": {"should_yield": "False"}}})
            MeterPoint.parse_obj({
                "name": "a_meter",
                "path": {"paths": ["a_path"]},
                "type": "ObservableGauge",
                "attributes": {"attr1": {"should_yield": "False"}}})
        except ValidationError:
            self.fail("should not raise error")
        with self.assertRaises(ValidationError):
            MeterPoint.parse_obj({
                "name": "a_meter",
                "path": {"paths": ["a_path"]},
                "type": "Counter",
                "attributes": {"attr1": {"should_yield": "False"}}})

    def test_attribute_maps_validator(self):
        """
        Test to check if the 'attribute_maps' attribute gets processed
        correctly using the root validator.
        """
        otel_model_dict = {
            "meters": [
                {
                    "scope": "testing",
                    "scope_version": "1.0",
                    "met_points": [
                        {
                            "otel_name": "metadata.correlation.identities",
                            "type": "ObservableGauge",
                            "description": "Unix timestamp in seconds",
                            "unit": "1",
                            "attribute_maps": {
                                "interface_to_network_path_map": {
                                    "can_purge": True,
                                    "store_map": {
                                        "Ethernet_eth2_3917": "devices.terminal.geo_broadband.gx.3917",
                                        "Ethernet_eth7_4033": "devices.terminal.terrestrial_broadband.ivent_lte.0",
                                        "ppp100": "devices.terminal.geo_narrowband.bgan.3917",
                                    },
                                },
                                "interface_id_to_rem_interface_map": {"store_map": {}},
                            },
                        }
                    ],
                }
            ]
        }

        otel_model = OTELConfigModel(**otel_model_dict)
        metric_point = otel_model.obs_met[0].met_points[0]
        meter_point = MeterPoint(
            name=metric_point.otel_name,
            type=metric_point.type,
            attributes=metric_point.attributes,
            attribute_maps=metric_point.attribute_maps,
        )
        attr_storage, _ = meter_point.attribute_maps.values()
        store_map, *_ = attr_storage.store_map.values()

        self.assertIsInstance(attr_storage, AttributeMapStore)
        self.assertIsInstance(store_map, MapStorage)

    def test_stat_storage_get_status(self):
        self.status_storage.status_limits = [0.001, 0.4, 0.6]
        self.status_storage.confidence_index = 0.0
        status1 = self.status_storage.get_status()
        self.status_storage.confidence_index = 0.39
        status2 = self.status_storage.get_status()
        self.status_storage.confidence_index = 0.59
        status3 = self.status_storage.get_status()
        self.status_storage.confidence_index = 1.0
        status4 = self.status_storage.get_status()
        self.assertEqual(status1, 0)
        self.assertEqual(status2, 1)
        self.assertEqual(status3, 2)
        self.assertEqual(status4, 3)

    def test_set_confidence_index(self):
        self.status_storage.status_limits = [0.4, 0.6]
        self.status_storage.confidence_index = 0.8
        self.status_storage.set_confidence_index([0.1, 0.2])
        self.assertEqual(self.status_storage.confidence_index, 1)
        self.status_storage.set_confidence_index([-0.51, -0.6])
        self.assertEqual(self.status_storage.confidence_index, 0.001)
        self.status_storage.set_confidence_index([0.1, 0.2])
        self.assertAlmostEqual(
            self.status_storage.confidence_index, 0.301, places=5)
        self.status_storage.set_confidence_index([-0.5])
        self.assertEqual(self.status_storage.confidence_index, 0.5)
        self.status_storage.set_confidence_index([-1.0])
        self.assertEqual(self.status_storage.confidence_index, 0.0)

    def test_calculate_rtt_weight(self):
        self.status_storage.rtt_cfg = ({
            "gx": {"normal": 800, "cap": 3200, "pos_w_adj_factor": 20,
                   "neg_w_adj_factor": 20},
            "lte": {"normal": 100, "cap": 400, "pos_w_adj_factor": 20,
                    "neg_w_adj_factor": 20},
            "leo": {"normal": 260, "cap": 1040, "pos_w_adj_factor": 20,
                    "neg_w_adj_factor": 20},
            "bgan": {"normal": 800, "cap": 3200, "pos_w_adj_factor": 20,
                     "neg_w_adj_factor": 20},
        })
        weight1 = self.status_storage.calculate_rtt_weight("gx", 500)
        self.assertAlmostEqual(weight1, 0.05, 4)
        weight2 = self.status_storage.calculate_rtt_weight("gx", 1600)
        self.assertAlmostEqual(weight2, -0.1, 4)
        weight3 = self.status_storage.calculate_rtt_weight("gx", 7200)
        self.assertAlmostEqual(weight3, -0.2, 4)

    def test_calc_ovr_adj_factors(self):
        #happy path
        self.status_storage.full_history_required = True
        store1 = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        store2 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        res1 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 0), res1)
        # unequal store sizes
        store1 = [100, 200, 300, 400, 500, 600, 700, 800, 900]
        store2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        res2 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 1), res2)
        # not enough history
        self.status_storage.full_history_required = True
        store1 = [100, 200, 300, 400, 500, 600, 700, 800, 900]
        store2 = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        res3 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 1), res3)
        # diffs are both zero
        store1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        store2 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        res4 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 1), res4)
        # happy path second tuple element
        store1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        store2 = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        res5 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((0, 1), res5)
        # negative diff first store
        store1 = [100, 200, 300, 400, 500, 600, 700, 800, 900, 0]
        store2 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1100]
        res6 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 1), res6)
        # negative diff second store
        store2 = [100, 200, 300, 400, 500, 600, 700, 800, 900, 0]
        store1 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1100]
        res7 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        self.assertEqual((1, 1), res7)
        # equal weights
        store1 = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        store2 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 100]
        res8 = self.status_storage.calc_ovr_adj_factors(
            store1, store2)
        print(f"res8: {res8}")
        self.assertEqual((0.5, 0.5), res8)

    def test_cal_w_adj_factor(self):
        self.status_storage.full_history_required = True
        store = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        curr_val = 1100
        factor = 2
        self.status_storage.weight_calc_mode = 'Linear'
        res1 = self.status_storage.cal_w_adj_factor(store, curr_val, factor)
        self.assertAlmostEqual(0.05, res1)
        store = [100, 200, 300, 400, 500, 600, 700, 800]
        curr_val = 900
        res2 = self.status_storage.cal_w_adj_factor(store, curr_val, factor)
        self.assertEqual(0, res2)
        store = []
        curr_val = 0
        res3 = self.status_storage.cal_w_adj_factor(store, curr_val, factor)
        self.assertEqual(0, res3)
        store = [100, 200, 300, 400, 500, 600, 700, 800, 900]
        curr_val = 800
        res4 = self.status_storage.cal_w_adj_factor(store, curr_val, factor)
        self.assertEqual(0, res4)
        store = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        curr_val = 0
        factor = 2
        res5 = self.status_storage.cal_w_adj_factor(store, curr_val, factor)
        self.assertEqual(0, res5)

    def test_get_weight(self):
        self.status_storage.weight_calc_mode = 'Linear'
        self.assertAlmostEqual(self.status_storage.get_weight(1, 10, 2), 0.05)
        self.status_storage.weight_calc_mode = 'Parabolic'
        self.assertAlmostEqual(self.status_storage.get_weight(1, 10, 2),
                               0.0826, places=4)
        self.status_storage.weight_calc_mode = 'Exponential'
        self.assertAlmostEqual(self.status_storage.get_weight(1, 10, 2),
                               0.04665, places=4)

    def test_reset_store_values_if_required(self):
        self.status_storage.pos_w_calc_store = [10]
        self.status_storage.pos_w_calc_store = [20]
        self.status_storage.confidence_index = 0.5
        store1 = [10]
        store2 = [20]
        curr_val1 = 0
        curr_val2 = 30
        self.status_storage.reset_store_values_if_required(
            store1, store2, curr_val1, curr_val2)
        self.assertEqual(
            self.status_storage.pos_w_calc_store, [])
        self.assertEqual(
            self.status_storage.neg_w_calc_store, [])

        self.status_storage.pos_w_calc_store = [10]
        self.status_storage.pos_w_calc_store = [20]
        self.status_storage.confidence_index = 0.5
        store1 = [10]
        store2 = [20]
        curr_val1 = 20
        curr_val2 = 0
        self.status_storage.reset_store_values_if_required(
            store1, store2, curr_val1, curr_val2)
        self.assertEqual(
            self.status_storage.pos_w_calc_store, [])
        self.assertEqual(
            self.status_storage.neg_w_calc_store, [])

    def test_get_ir_state(self):
        self.status_storage.ir_store = []
        res = self.status_storage.handle_ir_state(-1)
        self.assertEqual(self.status_storage.ir_store, [])
        self.assertEqual(res, 0)

        self.status_storage.ir_store = []
        self.status_storage.ir_store_timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=181))
        res = self.status_storage.handle_ir_state(-1)
        self.assertEqual(self.status_storage.ir_store, [])
        self.assertEqual(res, 0)

        res = self.status_storage.handle_ir_state(0)
        self.assertEqual(self.status_storage.ir_store, [0])
        self.assertEqual(res, 1)

        res = self.status_storage.handle_ir_state(1)
        self.assertEqual(self.status_storage.ir_store, [0, 1])
        self.assertEqual(res, 2)

        self.status_storage.ir_store = [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        res = self.status_storage.handle_ir_state(0)
        self.assertEqual(self.status_storage.ir_store,
                         [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0])
        self.assertEqual(res, 1)

    def test_get_bgp_state(self):
        self.status_storage.bgp_store = []
        res = self.status_storage.handle_bgp_state("")
        self.assertEqual(self.status_storage.bgp_store, [])
        self.assertEqual(res, 0)

        self.status_storage.bgp_store = []
        self.status_storage.bgp_store_timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=181))
        res = self.status_storage.handle_bgp_state("")
        self.assertEqual(self.status_storage.bgp_store, [])
        self.assertEqual(res, 0)

        res = self.status_storage.handle_bgp_state("it is Established")
        self.assertEqual(self.status_storage.bgp_store, [1])
        self.assertEqual(res, 2)

        res = self.status_storage.handle_bgp_state("it is another state")
        self.assertEqual(self.status_storage.bgp_store, [1, 0])
        self.assertEqual(res, 2)

        self.status_storage.bgp_store = [1, 0, 0]
        res = self.status_storage.handle_bgp_state("it is another state")
        self.assertEqual(self.status_storage.bgp_store, [0, 0, 0])
        self.assertEqual(res, 1)

    def test_handle_pc_state(self):
        self.status_storage.ns_store = []
        res = self.status_storage.handle_pc_state(-1)
        self.assertEqual(self.status_storage.ns_store, [])
        self.assertEqual(res, 0)
        
        self.status_storage.ns_store = []
        self.status_storage.ns_store_timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=181))
        res = self.status_storage.handle_pc_state(-1)
        self.assertEqual(self.status_storage.ns_store, [])
        self.assertEqual(res, 0)

        res = self.status_storage.handle_pc_state(1)
        self.assertEqual(self.status_storage.ns_store, [1])
        self.assertEqual(res, 2)

        self.status_storage.ns_store = [0, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        res = self.status_storage.handle_pc_state(0)
        self.assertEqual(self.status_storage.ns_store,
                         [1, 1, 1, 1, 1, 1, 1, 1, 1, 0])
        self.assertEqual(res, 2)

        self.status_storage.ns_store = [0, 0, 0, 0, 0, 0, 0]
        res = self.status_storage.handle_pc_state(0)
        self.assertEqual(self.status_storage.ns_store,
                         [0, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(res, 1)

    def test_handle_kl_state(self):

        self.status_storage.kl_store = []
        res = self.status_storage.handle_kl_state(-1)
        self.assertEqual(self.status_storage.kl_store, [])
        self.assertEqual(res, 0)

        self.status_storage.kl_store = []
        self.status_storage.kl_store_timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=181))
        res = self.status_storage.handle_kl_state(-1)
        self.assertEqual(self.status_storage.kl_store, [])
        self.assertEqual(res, 0)

        res = self.status_storage.handle_kl_state(1)
        self.assertEqual(self.status_storage.kl_store, [1])
        self.assertEqual(res, 3)

        self.status_storage.kl_store = [0, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        res = self.status_storage.handle_kl_state(0)
        self.assertEqual(self.status_storage.kl_store,
                         [1, 1, 1, 1, 1, 1, 1, 1, 1, 0])
        self.assertEqual(res, 3)

        self.status_storage.kl_store = [0, 0]
        res = self.status_storage.handle_kl_state(0)
        self.assertEqual(self.status_storage.kl_store,
                         [0, 0, 0])
        self.assertEqual(res, 2)

        self.status_storage.kl_store = [0, 0, 0, 0, 0]
        res = self.status_storage.handle_kl_state(0)
        self.assertEqual(self.status_storage.kl_store,
                         [0, 0, 0, 0, 0, 0])
        self.assertEqual(res, 1)

    def test_handle_all_if_kl_state(self):
        self.status_storage.all_if_kl_state = 0
        res = self.status_storage.handle_all_if_kl_state(-1)
        self.assertEqual(self.status_storage.all_if_kl_state, 0)
        self.assertEqual(res, 0)

        res = self.status_storage.handle_all_if_kl_state(1)
        self.assertEqual(self.status_storage.all_if_kl_state, 1)
        self.assertEqual(res, 1)

        res = self.status_storage.handle_all_if_kl_state(2)
        self.assertEqual(self.status_storage.all_if_kl_state, 2)
        self.assertEqual(res, 2)

    def test_handle_all_ir_disabled_state(self):
        self.status_storage.all_ir_disabled_state = 0
        res = self.status_storage.handle_all_ir_disabled_state(-1)
        self.assertEqual(self.status_storage.all_ir_disabled_state, 0)
        self.assertEqual(res, 0)

        res = self.status_storage.handle_all_ir_disabled_state(1)
        self.assertEqual(self.status_storage.all_ir_disabled_state, 1)
        self.assertEqual(res, 1)

        res = self.status_storage.handle_all_ir_disabled_state(2)
        self.assertEqual(self.status_storage.all_ir_disabled_state, 2)
        self.assertEqual(res, 2)
