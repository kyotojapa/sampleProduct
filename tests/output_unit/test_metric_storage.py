# pylint: disable=missing-docstring
# pylint: disable=W0212
import datetime
import unittest
from unittest import mock
from unittest.mock import call


from output_unit import metric_storage
from output_unit.meter_point import OP_TYPE, AttributeMapStore, AttributeStorage, MeterPoint, ObsMetStorage, StatusConfig, StatusMetricStorage, StatusStorage
from output_unit.otel_conf_model import AttributeMaps
from output_unit.otel_conf_model import MetricAttribute
from output_unit.transforms.otel_trans_func import parse_int


class TestMetricStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.metric_store = metric_storage.MetricStore()

    def tearDown(self) -> None:
        if hasattr(metric_storage.MetricStore, "instance"):
            metric_storage.MetricStore.keep_running = False
            del metric_storage.MetricStore.instance
        del self.metric_store

    def test_get_shapshot_id(self):
        """Test the get snapshot id method"""

        an_id = metric_storage.get_snapshot_object_id("<IrIf!1234>")
        self.assertEqual(an_id, "IrIf")

    @mock.patch("output_unit.metric_storage.logging")
    def test_not_get_shapshot_id(self, mock_logs):
        """Test the scenario where u dont get the snapshot id"""

        an_id = metric_storage.get_snapshot_object_id("some string")
        mock_logs.warn.assert_called_with(
            "failed to extract object id from snapshot")
        self.assertEqual(an_id, "")

    def test_get_meter_points(self):
        # no data branch
        r = list(self.metric_store.get_meter_points(None))
        self.assertEqual(r, [])

        # no snapshot branch
        data = mock.MagicMock(snapshot="_sentinel")
        r = list(self.metric_store.get_meter_points(data))
        self.assertEqual(r, [])

        # metric match branch
        metric = mock.MagicMock(obj_id="Test")
        data.snapshot = mock.MagicMock(obj="<Test!123>")
        self.metric_store.value_store["test"] = metric

        mplist = list(self.metric_store.get_meter_points(data))

        self.assertEqual(len(mplist), 1)
        self.assertEqual(mplist[0], metric)

    def test_set_obs_store_attribute(self):
        hashmock = mock.MagicMock(attribute=True)
        metric = mock.MagicMock(obj_id="Test", obs_store={"hash": hashmock})
        data = mock.MagicMock()
        data.snapshot = mock.MagicMock(obj="<Test!123>")

        self.metric_store.value_store["test"] = metric

        self.metric_store.set_obs_store_attribute(
            "test", "hash", "attribute", 1234)

        self.assertEqual(hashmock.attribute, 1234)

    @mock.patch("output_unit.metric_storage.logging")
    def test_set_obs_store_attribute_raise(self, mock_logger):
        mock_name = "name"
        self.metric_store.value_store[mock_name] = type("someobj", (), {})()
        self.metric_store.set_obs_store_attribute(
            mock_name, "hash", "attr", "val")

        mock_logger.error.assert_called_once()

    @mock.patch("output_unit.metric_storage.logging")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__handle_non_obs_metrics")
    @mock.patch(
        "output_unit.metric_storage.MetricStore."
        "_MetricStore__handle_observable_metrics")
    @mock.patch(
        "output_unit.metric_storage.MetricStore."
        "_MetricStore__apply_value_trans_if_any")
    def test_set_metric(
        self,
        value_trans_m: mock.MagicMock,
        handle_obs_m: mock.MagicMock,
        handle_oth_m: mock.MagicMock,
        logging_m: mock.MagicMock,
    ):
        # attributes not set branch
        self.metric_store.set_metric("test", "value", {}, False)
        value_trans_m.assert_not_called()
        handle_obs_m.assert_not_called()
        handle_oth_m.assert_not_called()

        # observable branch
        hashmock = mock.MagicMock(attribute=True)
        metric = mock.MagicMock(
            obj_id="Test", obs_store={"hash": hashmock}, type="Observable"
        )
        self.metric_store.value_store["test"] = metric
        value_trans_m.return_value = 1

        self.metric_store.set_metric("test", "value", {}, True)

        handle_obs_m.assert_called_once_with("test", {}, 1)

        # not observable branch
        metric.type = "Counter"
        value_trans_m.return_value = 2

        self.metric_store.set_metric("test", "value", {}, True)
        handle_oth_m.assert_called_once_with("test", {}, 2)

        # exception branch
        self.metric_store.set_metric("notinstore", "value", {}, True)

        calls = [call("'notinstore'"), call("'notinstore'")]
        logging_m.error.assert_has_calls(calls)

    def test_read_obs_metric(self):
        metric = mock.MagicMock(obj_id="Test", obs_store="expected")

        self.metric_store.value_store["test"] = metric

        result = self.metric_store.read_obs_metric("test")

        self.assertEqual(result, "expected")

    @mock.patch('output_unit.meter_point.TRANS_FUNC_MAP')
    def test_transform_metric_attributes_trans_exists(
            self, mock_trans_func_map: mock.MagicMock):
        """Test the tranform metric attributes function with a
        transformation works"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="parse_int")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute})

        self.metric_store.value_store["a_metric_name"] = a_metric

        mock_trans_func_map['parse_int'] = parse_int
        att_set_res, attr_res = self.metric_store.transform_metric_attributes(
            "a_metric_name", {"an_attribute": "400"})

        self.assertEqual(att_set_res, True)
        self.assertEqual(attr_res["an_attribute"].value, 400)

    def test_transform_metric_attributes_no_trans_exists(self):
        """Test the tranform metric attributes function with
        a transformation works"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute})

        self.metric_store.value_store["a_metric_name"] = a_metric

        att_set_res, attr_res = self.metric_store.transform_metric_attributes(
            "a_metric_name", {"an_attribute": "400"})

        self.assertEqual(att_set_res, True)
        self.assertEqual(attr_res["an_attribute"].value, "400")

    def test_transform_metric_attributes_exception_raised(self):
        """Test the transform metric attributes function
        with a transformation works"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute})

        self.metric_store.value_store["a_metric_name"] = a_metric

        att_set_res, _ = self.metric_store.transform_metric_attributes(
            "a_metric_name", ["an_attribute", "400"])

        self.assertEqual(att_set_res, False)

    def test_hash_func(self):
        "Test to test the hash function of met storage"

        atttributes = {"a_name": MetricAttribute(value="val1"),
                       "a_name2": MetricAttribute(value="val2")}
        at_hash = hash("val1" + "val2")
        res = self.metric_store._MetricStore__calculate_hash(atttributes)
        self.assertEqual(int(res), int(at_hash))
        res = self.metric_store._MetricStore__calculate_hash({})
        self.assertEqual(int(res), hash("default"))

        res = self.metric_store._MetricStore__calculate_hash([])
        self.assertEqual(res, '')

    @mock.patch("output_unit.metric_storage.logging")
    def test_set_obs_store_metric(self, log_m):
        """test set obs store counter no aggr function"""
        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        obs_store_val = mock.MagicMock(
            timestamp=datetime.datetime.utcnow(),
            iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)

        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__set_obs_store_metric(
            "a_metric_name", "a_hash_key", 200)

        self.assertEqual(self.metric_store.value_store[
            "a_metric_name"].obs_store["a_hash_key"].iter_val, 200)

        obs_store_val = mock.MagicMock(
            iter_val=100, timestamp=datetime.datetime.utcnow())
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)

        self.metric_store._MetricStore__set_obs_store_metric(
            "another_metric", "a_hash_key", 100)
        log_m.error.assert_called_once()

    @mock.patch("output_unit.metric_storage.logging")
    def test_init_obs_store_entry(self, log_m):
        """test set obs store counter no aggr function"""

        met_attribute = MetricAttribute(target="a_target", value="a_value",
                                        trans_func="a_func")
        attrs = {"an_attribute": met_attribute}
        at_hash = self.metric_store._MetricStore__calculate_hash(attrs)
        meter_point = MeterPoint(name="a_meter", type="ObservableGauge",
                                 attributes=attrs)

        self.metric_store.value_store["a_metric_name"] = meter_point
        self.metric_store._MetricStore__init_obs_store_entry(
            "a_metric_name", attrs)
        self.assertIsNotNone(self.metric_store.value_store[
            "a_metric_name"].obs_store[at_hash])
        self.assertEqual(self.metric_store.value_store[
            "a_metric_name"].obs_store[at_hash].attr,
                         {x: y.value for x, y in attrs.items()})

        self.metric_store._MetricStore__init_obs_store_entry(
            "a_metric_name", [])
        log_m.error.assert_called()

    @mock.patch("output_unit.metric_storage.logging")
    def test_pop_store_entries(self, log_m):
        """test pop store entries function"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        obs_store_val = mock.MagicMock(
            timestamp=datetime.datetime.utcnow(),
            yielded=True, iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__pop_store_entries(
            ["a_hash_key"], self.metric_store.value_store[
                "a_metric_name"].obs_store)
        self.assertEqual(len(self.metric_store.value_store[
            "a_metric_name"].obs_store), 0)
        log_m.debug.assert_called_once()
        obs_store = {"a_hash_key": obs_store_val}
        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__pop_store_entries(
            ["another_hash_key"], self.metric_store.value_store[
                "a_metric_name"].obs_store)
        log_m.error.assert_called_once()

    @mock.patch("output_unit.metric_storage.logging")
    def test_mark_entries_for_retrieval(self, log_m):
        """test mark entries for retrieval function"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        old_timestamp = (
            datetime.datetime.utcnow() -
            datetime.timedelta(
                seconds=self.metric_store.purge_unused_entries_timer))
        obs_store_val = mock.MagicMock(
            timestamp=old_timestamp,
            yielded=True, iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        hash_list = self.metric_store._MetricStore__mark_entries_for_retrieval(
            self.metric_store.value_store["a_metric_name"].obs_store)
        self.assertEqual(["a_hash_key"], hash_list)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__mark_entries_for_retrieval")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__pop_store_entries")
    def test_cleanup_store(self, pop_m, mark_m):
        """test clean up store function"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        old_timestamp = (
            datetime.datetime.utcnow() -
            datetime.timedelta(
                seconds=self.metric_store.purge_unused_entries_timer))
        obs_store_val = mock.MagicMock(
            timestamp=old_timestamp,
            yielded=True, iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__cleanup_store(
            self.metric_store.value_store["a_metric_name"].obs_store)
        pop_m.assert_called_once()
        mark_m.assert_called_once()

    def test_init_ct_and_ret_diff(self):
        """test init ct and ret diff function"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        ct_store_val = mock.MagicMock(
            val=100, timestamp=datetime.datetime.utcnow())
        sync_ct_store = {"a_hash_key": ct_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            sync_ct_store=sync_ct_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        diff = self.metric_store._MetricStore__init_ct_and_ret_diff(
            "a_hash_key", "a_metric_name", 200)
        self.assertEqual(self.metric_store.value_store[
            "a_metric_name"].sync_ct_store["a_hash_key"].val, 0)
        self.assertEqual(diff, 0)

    def test_et_ct_and_ret_diff(self):
        """test init ct and ret diff function"""

        met_attribute = mock.MagicMock(target="", value="",
                                       trans_func="")
        ct_store_val = mock.MagicMock(
            val=100, timestamp=datetime.datetime.utcnow())
        sync_ct_store = {"a_hash_key": ct_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            sync_ct_store=sync_ct_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        diff = self.metric_store._MetricStore__set_ct_and_ret_diff(
            "a_hash_key", "a_metric_name", 200)
        self.assertEqual(self.metric_store.value_store[
            "a_metric_name"].sync_ct_store["a_hash_key"].val, 200)
        self.assertEqual(diff, 100)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_hash")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_ct_and_ret_diff")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__init_ct_and_ret_diff")
    def test_cleanup_store2(self, init_m, set_m, calc_m):
        """test clean up store function"""

        met_attribute = mock.MagicMock(target="a_target", value="a_value",
                                       trans_func="a_func")

        ct_store_val = mock.MagicMock(
            val=100, timestamp=datetime.datetime.utcnow())

        at_hash = hash("a_value")

        sync_ct_store = {f"{at_hash}": ct_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            sync_ct_store=sync_ct_store,
            is_in_aggr_format=True)

        self.metric_store.value_store["a_metric_name"] = a_metric
        calc_m.return_value = str(at_hash)
        set_m.return_value = 200
        diff = self.metric_store._MetricStore__get_non_obs_counter_diff(
            "a_metric_name", 200, {"an_att_name": met_attribute})

        set_m.assert_called_once()
        self.assertEqual(200, diff)
        calc_m.return_value = str(985932093522407999)
        init_m.return_value = 100
        diff = self.metric_store._MetricStore__get_non_obs_counter_diff(
            "a_metric_name", 200, {"an_att_name": met_attribute})

        init_m.assert_called_once()
        self.assertEqual(calc_m.call_count, 2)
        self.assertEqual(0, diff)

        a_metric = mock.MagicMock(
            name="a_metric_name",
            sync_ct_store=sync_ct_store,
            is_in_aggr_format=False)

        self.metric_store.value_store["a_metric_name"] = a_metric
        diff = self.metric_store._MetricStore__get_non_obs_counter_diff(
            "a_metric_name", 300, {"an_att_name": met_attribute})
        self.assertEqual(300, diff)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_values")
    def test_handle_observable_metrics(self, set_obs_m):
        """test handle observable metrics function"""

        met_attribute = mock.MagicMock(target="a_target", value="a_value",
                                       trans_func="a_func")

        obs_store_val = mock.MagicMock(
            timestamp=datetime.datetime.utcnow(),
            yielded=True, iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attributes={"an_attribute": met_attribute},
            obs_store=obs_store)

        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__handle_observable_metrics(
            "a_metric_name", {}, 200)
        set_obs_m.assert_called_once_with(
            "a_metric_name", 200, {})

    def test_add_views(self):

        a_view = mock.MagicMock(
            _attribute_values=None,
            _drop_unused_datapoints=False
        )
        instrument_views = {}
        instrument_views["a_metric"] = a_view
        views_len = len(self.metric_store.instrument_views.values())
        self.metric_store.add_views(instrument_views)
        self.assertEqual(len(self.metric_store.instrument_views.values()),
                         views_len + 1)

    def test_set_attribute_store(self):
        attributes = {"a_name": MetricAttribute(
            target="a_target", value="a_value")}
        k_hash = str(hash("a_value"))
        attribute_store_val = mock.MagicMock(
            attribute_values=["val1", "val2"],
            attributes_keys=[],
            timestamp=datetime.datetime.utcnow())
        attribute_store = {"123456789": attribute_store_val}
        a_metric = mock.MagicMock(
            name="a_metric_name",
            attribute_store=attribute_store)
        self.metric_store.value_store["a_metric_name"] = a_metric
        self.metric_store._MetricStore__set_attribute_store(
            "a_metric_name", attributes)

        self.assertEqual(len(self.metric_store.value_store[
            "a_metric_name"].attribute_store), 2)
        self.assertEqual(list(self.metric_store.value_store[
            "a_metric_name"].attribute_store[k_hash].attribute_values),
                ["a_value"])

    @mock.patch("output_unit.metric_storage.logging")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__configure_data_point_view")
    @mock.patch(
        "output_unit.metric_storage.MetricStore._MetricStore__cleanup_store")
    def test_clean_stores_apply_view(
            self, cl_store_mock, cfg_dp_v_mock, log_mock):

        obs_store_val = mock.MagicMock(
            timestamp=datetime.datetime.utcnow(),
            yielded=True, iter_val=100)
        obs_store = {"a_hash_key": obs_store_val}

        ct_store_val = mock.MagicMock(
            val=100, timestamp=datetime.datetime.utcnow())

        at_hash = hash("a_value")

        sync_ct_store = {f"{at_hash}": ct_store_val}

        attribute_store_val = mock.MagicMock(
            attribute_values=["val1", "val2"],
            attributes_keys=[],
            timestamp=datetime.datetime.utcnow())
        attribute_store = {"123456789": attribute_store_val}

        attr_dict = {"an_attribute": MetricAttribute(
            target="a_target", value="", trans_func="")}
        m_point = mock.MagicMock(
            name="mpoint",
            attributes=attr_dict,
            type="ObservableCounter",
            obs_store=obs_store,
            sync_ct_store=sync_ct_store,
            attribute_store=attribute_store)
        self.metric_store.clean_store()
        self.metric_store.add_metric("a_metric", m_point)

        self.metric_store._MetricStore__clean_stores_apply_view()
        calls = [call(obs_store), call(attribute_store)]
        cl_store_mock.assert_has_calls(calls)
        cfg_dp_v_mock.assert_called()
        m_point = mock.MagicMock(
            name="mpoint",
            attributes=attr_dict,
            type="Counter",
            obs_store=obs_store,
            sync_ct_store=sync_ct_store,
            attribute_store=attribute_store)
        self.metric_store.clean_store()
        self.metric_store.add_metric("a_metric", m_point)
        self.metric_store._MetricStore__clean_stores_apply_view()
        calls = [call(sync_ct_store), call(attribute_store)]
        cl_store_mock.assert_has_calls(calls)
        cfg_dp_v_mock.assert_called()
        self.metric_store.clean_store()

        m_point = mock.MagicMock(
            name="mpoint",
            attributes=attr_dict,
            obs_store=obs_store,
            sync_ct_store=sync_ct_store,
            attribute_store=attribute_store)
        self.metric_store.add_metric("a_metric", m_point)
        self.metric_store._MetricStore__clean_stores_apply_view()
        log_mock.error.assert_called()

    def test_configure_data_point_view(self):
        """test init ct and ret diff function"""
        k_hashes = ["123456789"]

        attribute_store_val = mock.MagicMock(
            attribute_values=["val1", "val2"],
            attributes_keys=[],
            timestamp=datetime.datetime.utcnow())
        attribute_store = {"123456789": attribute_store_val}
        a_point = mock.MagicMock(
            attribute_store=attribute_store,
            purge_unused_attributes=True
        )
        a_view = mock.MagicMock(
            _attribute_values=None,
            _drop_unused_datapoints=False
        )

        self.metric_store.instrument_views["a_metric"] = a_view
        self.metric_store._MetricStore__configure_data_point_view(
            k_hashes, "a_metric", a_point)
        self.assertEqual(self.metric_store.instrument_views[
            "a_metric"]._attribute_values, [["val1", "val2"]])
        self.assertEqual(self.metric_store.instrument_views[
            "a_metric"]._drop_unused_datapoints, True)

    def test_update_value_in_attribute_maps_key_exists(self):
        """Test to update the attribute maps, where the key exists.
        """

        attribute_maps = {
            "test_map": mock.MagicMock(
                store_map={
                    "test_key": mock.MagicMock(
                        value="test_value", timestamp=datetime.datetime.utcnow()
                    )
                }
            )
        }
        a_metric = mock.MagicMock(name="a_metric_name", attribute_maps=attribute_maps)

        self.metric_store.value_store["a_metric_name"] = a_metric
        map_value = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map["test_key"]
        )
        timestamp = map_value.timestamp

        self.assertIn("a_metric_name", self.metric_store.value_store)
        self.assertIn("test_value", map_value.value)

        self.metric_store.update_attribute_map(
            "a_metric_name", "test_map", "test_key", "some_value"
        )

        updated_map = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map["test_key"]
        )

        self.assertIn("a_metric_name", self.metric_store.value_store)
        self.assertNotEqual(timestamp, updated_map.timestamp)

    def test_update_value_in_attribute_maps_new_entry(self):
        """Test to update the attribute maps, where the key
        does not exists. This should update only the specific store
        """

        attribute_maps = {"test_map": mock.MagicMock(store_map={})}
        a_metric = mock.MagicMock(name="a_metric_name", attribute_maps=attribute_maps)

        self.metric_store.value_store["a_metric_name"] = a_metric

        self.assertIn("a_metric_name", self.metric_store.value_store)
        empty_store = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map
        )
        self.assertEqual({}, empty_store)

        self.metric_store.update_attribute_map(
            "a_metric_name", "test_map", "test_key", "some_value"
        )

        updated_map = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map["test_key"]
        )

        self.assertIn("a_metric_name", self.metric_store.value_store)
        self.assertIn("some_value", updated_map.value)

    def test_get_value_from_attribute_maps_key_exists(self):
        """
        Test to get the value of a particular map in the attribute maps.
        """

        attribute_maps = {"test_map": mock.MagicMock(store_map={})}
        a_metric = mock.MagicMock(name="a_metric_name", attribute_maps=attribute_maps)

        self.metric_store.value_store["a_metric_name"] = a_metric

        self.assertIn("a_metric_name", self.metric_store.value_store)
        empty_store = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map
        )
        self.assertEqual({}, empty_store)

        self.metric_store.update_attribute_map(
            "a_metric_name", "test_map", "test_key", "some_value", True
        )

        updated_map = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map["test_key"]
        )

        self.assertIn("a_metric_name", self.metric_store.value_store)
        self.assertIn("some_value", updated_map.value)

        value = self.metric_store.get_param_from_attribute_map(
            "a_metric_name", "test_map", "test_key", metric_storage.MapParam.VALUE
        )
        self.assertEqual(value, "some_value")
        
        stamp = self.metric_store.get_param_from_attribute_map(
            "a_metric_name", "test_map", "test_key", metric_storage.MapParam.TIMESTAMP_ONLY
        )
        self.assertIsInstance(stamp, datetime.datetime)

        ctx_stamp = self.metric_store.get_param_from_attribute_map(
            "a_metric_name", "test_map", "test_key", metric_storage.MapParam.BORN_TIMESTAMP
        )
        self.assertIsInstance(ctx_stamp, datetime.datetime)

    def test_get_value_from_attribute_maps_key_doesnt_exist(self):
        """
        Test to get the value of a particular map in the attribute maps.
        """

        attribute_maps = {"test_map": mock.MagicMock(store_map={})}
        a_metric = mock.MagicMock(name="a_metric_name", attribute_maps=attribute_maps)

        self.metric_store.value_store["a_metric_name"] = a_metric

        self.assertIn("a_metric_name", self.metric_store.value_store)
        empty_store = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map
        )
        self.assertEqual({}, empty_store)

        self.metric_store.update_attribute_map(
            "a_metric_name", "test_map", "test_key", "some_value"
        )

        updated_map = (
            self.metric_store.value_store["a_metric_name"]
            .attribute_maps["test_map"]
            .store_map["test_key"]
        )

        self.assertIn("a_metric_name", self.metric_store.value_store)
        self.assertIn("some_value", updated_map.value)

        value = self.metric_store.get_param_from_attribute_map(
            "a_metric_name", "test_map", "not_a_key"
        )
        self.assertEqual(value, None)

    def test_get_status_storage(self):
        """Test get status storage"""

        self.metric_store.clean_store()
        status_store_setup = StatusMetricStorage(
            pos_w_adj_factor=1, neg_w_adj_factor=1, store_size=20,
            full_history_required=False, confidence_index=0.5,
            record_interval=120, rtt_cfg={}, status_limits=[0.3, 0.5])
        self.assertEqual(self.metric_store._MetricStore__get_status_storage(
            status_store_setup), status_store_setup)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part1(
            self, calc_w_mock, set_conf_mock, set_obs_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        overlay_value = {
            "ud": 10,
            "ret": 1,
            "ctx_id": "123-123",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="123-123")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="123-123")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            overlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            overlay_value["attributes"]["dp2"])
        status_cfg = StatusConfig(
            vrf_id="an_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123",
                                              "sdp": "123-123"})

        status_stores = {"123-123": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage()}
        entry = MeterPoint(
            name="overlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        self.metric_store.value_store["overlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "overlay_status", overlay_value, "Overlay", "", [k_hash1, k_hash2],
            metric_storage.LOCATION.SDEP, "an_sdp_id")
        calc_w_mock.assert_called_with(overlay_value, ["ud", "ret"],
                                       status_stores["123-123"].status_store)
        set_conf_mock.assert_called_with([0.1])
        calls = [call("overlay_status", k_hash1, 1.0),
                 call("overlay_status", k_hash2, 3)]
        set_obs_mock.assert_has_calls(calls)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_rtt_weight")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part2(
            self, calc_w_mock, calc_rtt_mock, set_conf_mock, set_obs_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        underlay_value = {
            "sB": 10,
            "fB": 1,
            "rtt": 10,
            "ctx_id": "123-123",
            "rem_id": "321-321",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="123-123")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="123-123")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp2"])
        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Underlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="sdep", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_stores = {"123-123_321-321": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage()}
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        calc_rtt_mock.return_value = 0.0
        self.metric_store.value_store["underlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "underlay_status", underlay_value, "Underlay", "",
            [k_hash1, k_hash2], metric_storage.LOCATION.SDEP, "an_sdp_id")
        calc_w_mock.assert_called_with(underlay_value, ["sB", "fB"],
                                       status_stores[
                                           "123-123_321-321"].status_store)
        set_conf_mock.assert_called_with([0.1, 0.0])
        calc_rtt_mock.assert_called()
        calls = [call("underlay_status", k_hash1, 1.0),
                 call("underlay_status", k_hash2, 3)]
        set_obs_mock.assert_has_calls(calls)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "get_param_from_attribute_map")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part3(
            self, calc_w_mock, set_conf_mock, set_obs_mock, get_param_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        underlay_value = {
            "sB": 10,
            "fB": 1,
            "ns": 10,
            "rtt": 10,
            "ctx_id": "123-123",
            "if_id": "an_id",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised")},
                "dp3": {"status": MetricAttribute(
                            value="interface_enabled")},
                "dp4": {"status": MetricAttribute(
                            value="peer_connectivity")},
                "dp5": {"status": MetricAttribute(
                            value="keep_alive")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp2"])
        k_hash3 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp3"])
        k_hash4 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp4"])
        k_hash5 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp5"])
        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Underlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_stores = {"123-123": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ir_store=[1, 1, 1, 1, 1],
                kl_store=[1, 1, 1, 1],
                ns_store=[1, 1, 1, 1, 1, 1, 1, 1],
                was_set=(datetime.datetime.utcnow() -
                         datetime.timedelta(seconds=20)))))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage(),
                     str(k_hash3): ObsMetStorage(),
                     str(k_hash4): ObsMetStorage(),
                     str(k_hash5): ObsMetStorage()}
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "underlay_status", underlay_value, "Underlay", "gx",
            [k_hash1, k_hash2, k_hash3, k_hash4, k_hash5],
            metric_storage.LOCATION.SEEP, "123-123")
        calc_w_mock.assert_called_with(underlay_value, ["sB", "fB"],
                                       status_stores[
                                           "123-123"].status_store)
        get_param_mock.return_value = "1"
        set_conf_mock.assert_called_with([0.1, 0.02])
        calls = [call("underlay_status", k_hash1, 1.0),
                 call("underlay_status", k_hash2, 3),
                 call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 2),
                 call("underlay_status", k_hash5, 3)]
        set_obs_mock.assert_has_calls(calls)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "get_param_from_attribute_map")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part4(
            self, calc_w_mock, set_conf_mock, set_obs_mock, get_param_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        underlay_value = {
            "sB": 10,
            "fB": 1,
            "ns": 10,
            "rtt": 10,
            "ctx_id": "123-123",
            "if_id": "an_id",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised")},
                "dp3": {"status": MetricAttribute(
                            value="interface_enabled")},
                "dp4": {"status": MetricAttribute(
                            value="peer_connectivity")},
                "dp5": {"status": MetricAttribute(
                            value="keep_alive")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp2"])
        k_hash3 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp3"])
        k_hash4 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp4"])
        k_hash5 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp5"])
        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Underlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_stores = {"123-123": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ir_store=[1, 1, 1, 1, 1],
                kl_store=[1, 1, 1, 0, 0, 0],
                ns_store=[1, 1, 1, 1, 1, 1, 1, 1],
                was_set=(datetime.datetime.utcnow() -
                         datetime.timedelta(seconds=20)))))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage(),
                     str(k_hash3): ObsMetStorage(),
                     str(k_hash4): ObsMetStorage(),
                     str(k_hash5): ObsMetStorage()}
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "underlay_status", underlay_value, "Underlay", "gx",
            [k_hash1, k_hash2, k_hash3, k_hash4, k_hash5],
            metric_storage.LOCATION.SEEP, "123-123")
        calc_w_mock.assert_called_with(underlay_value, ["sB", "fB"],
                                       status_stores[
                                           "123-123"].status_store)
        get_param_mock.return_value = "1"
        set_conf_mock.assert_called_with([-0.5])
        calls = [call("underlay_status", k_hash1, 1.0),
                 call("underlay_status", k_hash2, 3),
                 call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 2),
                 call("underlay_status", k_hash5, 2)]
        set_obs_mock.assert_has_calls(calls)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "get_param_from_attribute_map")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part5(
            self, calc_w_mock, set_conf_mock, set_obs_mock, get_param_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        underlay_value = {
            "sB": 10,
            "fB": 1,
            "ns": 10,
            "rtt": 10,
            "ctx_id": "123-123",
            "if_id": "an_id",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised")},
                "dp3": {"status": MetricAttribute(
                            value="interface_enabled")},
                "dp4": {"status": MetricAttribute(
                            value="peer_connectivity")},
                "dp5": {"status": MetricAttribute(
                            value="keep_alive")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp2"])
        k_hash3 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp3"])
        k_hash4 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp4"])
        k_hash5 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp5"])
        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Underlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_stores = {"123-123": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ir_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                kl_store=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                ns_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                was_set=(datetime.datetime.utcnow() -
                         datetime.timedelta(seconds=20)))))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage(),
                     str(k_hash3): ObsMetStorage(),
                     str(k_hash4): ObsMetStorage(),
                     str(k_hash5): ObsMetStorage()}
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "underlay_status", underlay_value, "Underlay", "gx",
            [k_hash1, k_hash2, k_hash3, k_hash4, k_hash5],
            metric_storage.LOCATION.SEEP, "123-123")
        calc_w_mock.assert_called_with(underlay_value, ["sB", "fB"],
                                       status_stores[
                                           "123-123"].status_store)
        get_param_mock.return_value = "1"
        set_conf_mock.assert_called_with([-0.998])
        calls = [call("underlay_status", k_hash1, 1.0),
                 call("underlay_status", k_hash2, 3),
                 call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 2),
                 call("underlay_status", k_hash5, 1)]
        set_obs_mock.assert_has_calls(calls)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "get_param_from_attribute_map")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    @mock.patch("output_unit.meter_point.StatusMetricStorage."
                "set_confidence_index")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__calculate_data_weights")
    def test_process_status_metric_val_part6(
            self, calc_w_mock, set_conf_mock, set_obs_mock, get_param_mock):
        """Test process status metric val function"""

        self.metric_store.clean_store()
        underlay_value = {
            "sB": 10,
            "fB": 1,
            "ns": 10,
            "rtt": 10,
            "ctx_id": "123-123",
            "if_id": "an_id",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised")},
                "dp3": {"status": MetricAttribute(
                            value="interface_enabled")},
                "dp4": {"status": MetricAttribute(
                            value="peer_connectivity")},
                "dp5": {"status": MetricAttribute(
                            value="keep_alive")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp2"])
        k_hash3 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp3"])
        k_hash4 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp4"])
        k_hash5 = self.metric_store._MetricStore__calculate_hash(
            underlay_value["attributes"]["dp5"])
        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Underlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_stores = {"123-123": StatusStorage(status_store=(
            StatusMetricStorage(
                can_calculate=True,
                pos_w_calc_store=[10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                ir_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 0],
                kl_store=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                ns_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                was_set=(datetime.datetime.utcnow() -
                         datetime.timedelta(seconds=20)))))}

        obs_store = {str(k_hash1): ObsMetStorage(),
                     str(k_hash2): ObsMetStorage(),
                     str(k_hash3): ObsMetStorage(),
                     str(k_hash4): ObsMetStorage(),
                     str(k_hash5): ObsMetStorage()}
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        calc_w_mock.return_value = 0.1
        self.metric_store._MetricStore__process_status_metric_val(
            "underlay_status", underlay_value, "Underlay", "gx",
            [k_hash1, k_hash2, k_hash3, k_hash4, k_hash5],
            metric_storage.LOCATION.SEEP, "123-123")
        calc_w_mock.assert_called_with(underlay_value, ["sB", "fB"],
                                       status_stores[
                                           "123-123"].status_store)
        get_param_mock.return_value = "1"
        set_conf_mock.assert_called_with([-0.998])
        calls = [call("underlay_status", k_hash1, 0.002),
                 call("underlay_status", k_hash2, 1),
                 call("underlay_status", k_hash3, 1),
                 call("underlay_status", k_hash4, 2),
                 call("underlay_status", k_hash5, 1)]
        set_obs_mock.assert_has_calls(calls)


    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__can_set_confidence_index")
    def test_calculate_data_weights(self, can_set_mock):
        """Test calculate data weights function"""

        self.metric_store.clean_store()

        can_set_mock.return_value = True

        status_cfg = StatusConfig(vrf_id="an_id", status_inst_type="Overlay",
                                  status_store_setup=StatusMetricStorage(
                                      can_calculate=True),
                                  status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        k_hash = self.metric_store._MetricStore__calculate_hash({})

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
        status_stores = {}
        status_stores[k_hash] = StatusStorage(status_store=status_store)
        entry = MeterPoint(
            name="overlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["overlay_status"] = entry
        value = self.metric_store._MetricStore__calculate_data_weights(
            {"ud": 10, "ret": 10}, ["ud", "ret"],  status_store)
        self.assertEqual(value, 0)
        can_set_mock.return_value = False
        value = self.metric_store._MetricStore__calculate_data_weights(
            {"ud": 10, "ret": 10}, ["ud", "ret"],  status_store)
        self.assertEqual(value, -100)

    def test_can_set_conf_index(self):
        """Test can set conf index function"""

        self.metric_store.clean_store()

        status_store = StatusMetricStorage(
            can_calculate=True,
            record_interval=10,
            pos_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            was_set=(datetime.datetime.utcnow() -
                     datetime.timedelta(seconds=20)))
        data_keys = ["ud", "ret"]
        value = {"ud": 10, "ret": 10}
        value = self.metric_store._MetricStore__can_set_confidence_index(
            value, data_keys,  status_store)
        self.assertEqual(value, True)
        data_keys = []
        value = self.metric_store._MetricStore__can_set_confidence_index(
                    value, data_keys,  status_store)
        self.assertEqual(value, False)
        data_keys = ["ud", "ret"]
        value = {"ud": 10}
        value = self.metric_store._MetricStore__can_set_confidence_index(
                    value, data_keys,  status_store)
        self.assertEqual(value, False)
        data_keys = ["ud", "ret"]
        value = {"ud": -1, "ret": 0}
        value = self.metric_store._MetricStore__can_set_confidence_index(
            value, data_keys,  status_store)
        self.assertEqual(value, False)
        data_keys = ["ud", "ret"]
        value = {"ud": 10, "ret": 10}

        status_store = StatusMetricStorage(
            can_calculate=True,
            record_interval=10,
            pos_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            was_set=(datetime.datetime.utcnow() -
                     datetime.timedelta(seconds=0)))
        value = self.metric_store._MetricStore__can_set_confidence_index(
            value, data_keys,  status_store)
        self.assertEqual(value, False)

    def test_calculate_rtt_weight(self):
        """Test calculate rtt weight function"""
        self.metric_store.clean_store()

        status_store = StatusMetricStorage(
            can_calculate=True,
            record_interval=10,
            pos_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            was_set=(datetime.datetime.utcnow() -
                     datetime.timedelta(seconds=20)))
        value = self.metric_store._MetricStore__calculate_rtt_weight(
            status_store, 200, "gx")
        self.assertEqual(value, 0.02)

        status_store = StatusMetricStorage(
            can_calculate=True,
            record_interval=10,
            pos_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            neg_w_calc_store=[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            was_set=(datetime.datetime.utcnow() -
                     datetime.timedelta(seconds=0)))
        value = self.metric_store._MetricStore__calculate_rtt_weight(
            status_store, 200, "gx")
        self.assertEqual(value, -100)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_status_timeout_th")
    def test_set_status_if_not_receiving_status_updates(
            self, set_status_mock):
        """Test set status if not receiving updates function"""

        self.metric_store.clean_store()

        t_status = {"a_vrf_id": 1}
        ir_status = {"a_ir_id": 1}
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["a_ctx_id"] = StatusStorage(status_store=status_store)

        entry = MeterPoint(
            name="overlay_status1", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["overlay_status1"] = entry
        self.metric_store.set_status_if_not_receiving_status_updates(
            t_status, ir_status, False, "123-123")
        set_status_mock.assert_called_with(
            entry, "123-123", inst_type="Overlay", all_ir_state=2)
        self.metric_store.clean_store()
        status_cfg = StatusConfig(
            if_id="a_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "123-123"})

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["a_ctx_id"] = StatusStorage(status_store=status_store)
        entry = MeterPoint(
            name="underlay_status1", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["underlay_status1"] = entry
        self.metric_store.set_status_if_not_receiving_status_updates(
            t_status, ir_status, False, "123-123")
        set_status_mock.assert_called_with(
            entry, "123-123", ir_state=1)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    def test_set_status_timeout_th2(self, set_obs_met_mock):
        """Test set status to nf function"""

        self.metric_store.clean_store()

        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="an_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "an_sdp_id"})
        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[],
            ir_store=[1],
            kl_store=[1],
            bgp_store=[1]
            )
        status_stores = {}
        status_stores["an_sdp_id"] = StatusStorage(status_store=status_store)
        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="keep_alive")}
        attr5 = {"status": MetricAttribute(value="peer_connectivity")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "keep_alive"}
        v_attr5 = {"status": "peer_connectivity"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        att_storage1 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage2 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage3 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage4 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage5 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_store = {str(k_hash1): att_storage1,
                     str(k_hash2): att_storage2,
                     str(k_hash3): att_storage3,
                     str(k_hash4): att_storage4,
                     str(k_hash5): att_storage5}

        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            attribute_store=att_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store._MetricStore__set_status_timeout_th(
            entry, "an_sdp_id", ir_state=1)
        calls = [call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 3),
                 call("underlay_status", k_hash5, 0),
                 call("underlay_status", k_hash1, 0),
                 call("underlay_status", k_hash2, 0.0)]
        set_obs_met_mock.assert_has_calls(calls)

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    def test_set_status_based_on_ncat_data2(self, set_obs_met_mock):
        """Test set status to nf function"""

        self.metric_store.clean_store()

        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="an_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "an_sdp_id"})
        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[],
            ir_store=[],
            kl_store=[],
            bgp_store=[]
            )
        status_stores = {}
        status_stores["an_sdp_id"] = StatusStorage(status_store=status_store)
        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="keep_alive")}
        attr5 = {"status": MetricAttribute(value="peer_connectivity")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "keep_alive"}
        v_attr5 = {"status": "peer_connectivity"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        att_storage1 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage2 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage3 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage4 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage5 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_store = {str(k_hash1): att_storage1,
                     str(k_hash2): att_storage2,
                     str(k_hash3): att_storage3,
                     str(k_hash4): att_storage4,
                     str(k_hash5): att_storage5}

        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            attribute_store=att_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store._MetricStore__set_status_timeout_th(
            entry, "an_sdp_id", ir_state=1)
        calls = [call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 0),
                 call("underlay_status", k_hash5, 0),
                 call("underlay_status", k_hash1, 0),
                 call("underlay_status", k_hash2, 0)]
        set_obs_met_mock.assert_has_calls(calls)

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    def test_set_status_timeout_th1(self, set_obs_met_mock):
        """Test set status to nf function"""

        self.metric_store.clean_store()

        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="an_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "an_sdp_id"})
        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[],
            ir_store=[],
            kl_store=[],
            bgp_store=[]
            )
        status_stores = {}
        status_stores["an_sdp_id"] = StatusStorage(status_store=status_store)
        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="keep_alive")}
        attr5 = {"status": MetricAttribute(value="peer_connectivity")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "keep_alive"}
        v_attr5 = {"status": "peer_connectivity"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        att_storage1 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage2 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage3 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage4 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage5 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_store = {str(k_hash1): att_storage1,
                     str(k_hash2): att_storage2,
                     str(k_hash3): att_storage3,
                     str(k_hash4): att_storage4,
                     str(k_hash5): att_storage5}

        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            attribute_store=att_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store._MetricStore__set_status_timeout_th(
            entry, "an_sdp_id", ir_state=1)
        calls = [call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 0),
                 call("underlay_status", k_hash5, 0),
                 call("underlay_status", k_hash1, 0),
                 call("underlay_status", k_hash2, 0.0)]
        set_obs_met_mock.assert_has_calls(calls)

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))


    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    def test_set_status_based_on_ncat_data1(self, set_obs_met_mock):
        """Test set status to nf function"""

        self.metric_store.clean_store()

        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="an_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123", "sdp": "an_sdp_id"})
        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[],
            ir_store=[1],
            kl_store=[1],
            ns_store=[1],
            bgp_store=[]
            )
        status_stores = {}
        status_stores["an_sdp_id"] = StatusStorage(status_store=status_store)
        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="keep_alive")}
        attr5 = {"status": MetricAttribute(value="peer_connectivity")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "keep_alive"}
        v_attr5 = {"status": "peer_connectivity"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=181)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        att_storage1 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage2 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage3 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage4 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_storage5 = AttributeStorage(
                        timestamp=(datetime.datetime.utcnow() -
                                   datetime.timedelta(seconds=181)))
        att_store = {str(k_hash1): att_storage1,
                     str(k_hash2): att_storage2,
                     str(k_hash3): att_storage3,
                     str(k_hash4): att_storage4,
                     str(k_hash5): att_storage5}

        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, obs_store=obs_store,
            attribute_store=att_store,
            status_stores=status_stores)
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store._MetricStore__set_status_timeout_th(
            entry, "an_sdp_id", ir_state=1)
        calls = [call("underlay_status", k_hash3, 2),
                 call("underlay_status", k_hash4, 3),
                 call("underlay_status", k_hash5, 2),
                 call("underlay_status", k_hash1, 0),
                 call("underlay_status", k_hash2, 0)]
        set_obs_met_mock.assert_has_calls(calls)

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))
        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash1].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash2].timestamp),
            datetime.timedelta(seconds=1))

        self.assertLessEqual(
            (datetime.datetime.utcnow() - self.metric_store.value_store[
                                  "underlay_status"].attribute_store[
                                      k_hash3].timestamp),
            datetime.timedelta(seconds=1))

    def test_get_store_entries_part1(self):
        """Test to test get store entries method"""

        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="peer_connectivity")}
        attr5 = {"status": MetricAttribute(value="keep_alive")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "peer_connectivity"}
        v_attr5 = {"status": "keep_alive"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        status_store = StatusMetricStorage()

        state_data = metric_storage.StateData(
            inst_type="Underlay", th_type="tout")

        res = self.metric_store._MetricStore__get_store_entries(
            obs_store, status_store, state_data)
        self.assertEqual(len(res), 5)

    def test_get_store_entries_part2(self):
        """Test to test get store entries method"""

        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr2)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2}

        status_store = StatusMetricStorage()

        state_data = metric_storage.StateData(
            inst_type="Overlay", th_type="tout")

        res = self.metric_store._MetricStore__get_store_entries(
            obs_store, status_store, state_data)
        self.assertEqual(len(res), 2)

    def test_get_store_entries_part3(self):
        """Test to test get store entries method"""

        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        attr3 = {"status": MetricAttribute(value="interface_enabled")}
        attr4 = {"status": MetricAttribute(value="peer_connectivity")}
        attr5 = {"status": MetricAttribute(value="keep_alive")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}
        v_attr3 = {"status": "interface_enabled"}
        v_attr4 = {"status": "peer_connectivity"}
        v_attr5 = {"status": "keep_alive"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        k_hash3 = self.metric_store._MetricStore__calculate_hash(attr3)
        k_hash4 = self.metric_store._MetricStore__calculate_hash(attr4)
        k_hash5 = self.metric_store._MetricStore__calculate_hash(attr5)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr2)
        obs_storage3 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr3)
        obs_storage4 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr4)
        obs_storage5 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr5)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2,
                     str(k_hash3): obs_storage3,
                     str(k_hash4): obs_storage4,
                     str(k_hash5): obs_storage5}

        status_store = StatusMetricStorage()
        state_data = metric_storage.StateData(
            inst_type="Underlay", th_type="ncat")

        res = self.metric_store._MetricStore__get_store_entries(
            obs_store, status_store, state_data)
        self.assertEqual(len(res), 3)

    def test_get_store_entries_part4(self):
        """Test to test get store entries method"""

        attr1 = {"sampling_method": MetricAttribute(value="categorised")}
        attr2 = {"sampling_method": MetricAttribute(value="raw")}
        v_attr1 = {"sampling_method": "categorised"}
        v_attr2 = {"sampling_method": "raw"}

        k_hash1 = self.metric_store._MetricStore__calculate_hash(attr1)
        k_hash2 = self.metric_store._MetricStore__calculate_hash(attr2)
        obs_storage1 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr1)
        obs_storage2 = ObsMetStorage(
            timestamp=(datetime.datetime.utcnow() -
                       datetime.timedelta(seconds=61)), attr=v_attr2)
        obs_store = {str(k_hash1): obs_storage1,
                     str(k_hash2): obs_storage2}

        status_store = StatusMetricStorage()

        state_data = metric_storage.StateData(
            inst_type="Overlay", th_type="ncat")

        res = self.metric_store._MetricStore__get_store_entries(
            obs_store, status_store, state_data)
        self.assertEqual(len(res), 0)

    def test_set_status_storage_for_status_metric(self):
        """Test set obs storage for status metric function"""

        self.metric_store.clean_store()

        status_cfg = StatusConfig(
            vrf_id="an_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={})

        entry = MeterPoint(
            name="ovr_status", status_config=status_cfg,
            type="ObservableGauge", attributes={})
        self.metric_store.value_store["ovr_status"] = entry

        self.metric_store._MetricStore__set_status_storage_for_status_metric(
            "ovr_status", {""}, "Underlay", metric_storage.LOCATION.SDEP, "an_sdp_id")
        self.assertEqual(
            self.metric_store.value_store[
                "ovr_status"].status_stores, {})

        self.metric_store._MetricStore__set_status_storage_for_status_metric(
            "ovr_status", {"ctx_id": "a_ctx_id"}, "Overlay",
            metric_storage.LOCATION.SDEP, "an_sdp_id")
        self.assertIsInstance(
            self.metric_store.value_store[
                "ovr_status"].status_stores["a_ctx_id"].status_store,
            StatusMetricStorage)
        self.metric_store._MetricStore__set_status_storage_for_status_metric(
            "ovr_status", {"ctx_id": "a_ctx_id", "rem_id": "a_rem_id"},
            "Underlay", metric_storage.LOCATION.SDEP, "an_sdp_id")
        self.assertIsInstance(
            self.metric_store.value_store[
                "ovr_status"].status_stores["a_ctx_id_a_rem_id"].status_store,
            StatusMetricStorage)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "get_param_from_attribute_map")
    def test_update_status_stores(self, get_param_mock):
        get_param_mock.return_value = "an_sdp_id"
        self.metric_store.clean_store()
        value = {
            "sB": 1,
            "fB": 5,
            "rtt": 4,
            "ctx_id": "12-12",
            "if_id": "abcd123",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(
                            value="raw"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="12-12")},
                "dp2": {"sampling_method": MetricAttribute(
                            value="categorised"),
                        "sdep_remote_interface_id": MetricAttribute(
                            value="12345"),
                        "context_id": MetricAttribute(value="12-12")}
            }
        }
        k_hash1 = self.metric_store._MetricStore__calculate_hash(
            value["attributes"]["dp1"])
        k_hash2 = self.metric_store._MetricStore__calculate_hash(
            value["attributes"]["dp2"])
        entry = MeterPoint(
            name="ovr_status",
            type="ObservableGauge", attributes={}, status_stores={},
            status_config=StatusConfig(
                prim_ctx_data={"ctx": "12-12", "sdp": "an_sdp_id"}))
        self.metric_store.value_store["ovr_status"] = entry
        res = self.metric_store._MetricStore__update_status_stores(
            "ovr_status", value, "Underlay", metric_storage.LOCATION.SEEP)

        self.assertEqual(res, ("an_sdp_id", [k_hash1, k_hash2]))
        get_param_mock.return_value = ""
        res = self.metric_store._MetricStore__update_status_stores(
            "ovr_status", value, "Underlay", metric_storage.LOCATION.SEEP)

        self.assertEqual(res, ("", []))

    def test_set_can_calculate_status(self):
        """Test the set_can_calculate_status method"""

        self.metric_store.clean_store()

        t_status = {"a_vrf_id": 1}
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "321-321", "sdp": "321-321"})

        status_store = StatusMetricStorage(
            can_calculate=False,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["a_ctx_id"] = StatusStorage(status_store=status_store)

        entry = MeterPoint(
            name="overlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["overlay_status"] = entry
        self.metric_store.set_can_calculate_status(t_status)
        self.assertEqual(True, self.metric_store.value_store[
            "overlay_status"].status_stores[
                "a_ctx_id"].status_store.can_calculate)

    def test_set_status_metric_data_part1(self):
        """Test the set_can_calculate_status method"""

        self.metric_store.clean_store()

        ctx_data = {"a_ctx_id": {"sdp": "an_sdp_id"}}
        ir_data = {"an_ir_id": True}
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={})
        attr_map1 = AttributeMapStore()
        attr_map2 = AttributeMapStore()
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={},
            attribute_maps={"ir_map": attr_map1, "ctx_map": attr_map2})
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store.set_status_metric_data(ctx_data, ir_data, True)
        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ctx_map", "a_ctx_id", with_lock=False),
                         "an_sdp_id")

    def test_set_status_metric_data_part2(self):
        """Test the set_can_calculate_status method"""

        self.metric_store.clean_store()

        ctx_data = {"a_ctx_id": {"sdp": "an_sdp_id", "tenants": ["fvkau3"], 'prim_ctx': True}}
        ir_data = {"an_ir_id": True}
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={})
        attr_map1 = AttributeMapStore()
        attr_map2 = AttributeMapStore()
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={},
            attribute_maps={"ir_map": attr_map1, "ctx_map": attr_map2})
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store.set_status_metric_data(ctx_data, ir_data, False)
        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ir_map", "an_ir_id", with_lock=False), 'True')
        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ctx_map", "a_ctx_id", with_lock=False),
                         "an_sdp_id")
        self.assertEqual(self.metric_store.value_store[
            "underlay_status"].status_config.prim_ctx_data, {"ctx": "a_ctx_id", "sdp": "an_sdp_id"})

    def test_set_status_metric_data_part3(self):
        """Test the set_can_calculate_status method"""

        self.metric_store.clean_store()

        ctx_data = {"a_ctx_id": {"sdp": "an_sdp_id", "tenants": ["fvkau3"], 'prim_ctx': False},
                    "another_ctx_id": {"sdp": "another_sdp_id", "tenants": ["fvkau4"], 'prim_ctx': True}}
        ir_data = {"an_ir_id": True}
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={})
        attr_map1 = AttributeMapStore()
        attr_map2 = AttributeMapStore()
        entry = MeterPoint(
            name="underlay_status", status_config=status_cfg,
            type="ObservableGauge", attributes={},
            attribute_maps={"ir_map": attr_map1, "ctx_map": attr_map2})
        self.metric_store.value_store["underlay_status"] = entry
        self.metric_store.set_status_metric_data(ctx_data, ir_data, False)
        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ir_map", "an_ir_id", with_lock=False), 'True')
        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ctx_map", "a_ctx_id", with_lock=False),
                         "an_sdp_id")

        self.assertEqual(self.metric_store.get_param_from_attribute_map(
            "underlay_status", "ctx_map", "another_ctx_id", with_lock=False),
                         "another_sdp_id")
        self.assertEqual(self.metric_store.value_store[
            "underlay_status"].status_config.prim_ctx_data, {"ctx": "another_ctx_id", "sdp": "another_sdp_id"})

    @mock.patch('output_unit.metric_storage.datetime.datetime')
    @mock.patch('builtins.print')
    def test_print_set_to_nf_message(self, mock_print, utc_mock):
        utc_mock.utcnow.return_value = "a_time"
        entry = MeterPoint(name="name", type="Counter", attributes={})
        self.metric_store._MetricStore__print_set_to_nf_message(entry)
        mock_print.assert_called_with(
           "----------------------------------\n"
           f"Setting {entry.name} to Unknown state at "
           "a_time\n"
           "----------------------------------\n")

    @mock.patch('builtins.print')
    def test_print_status(self, mock_print):

        self.metric_store.clean_store()
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "321-321", "sdp": "321-321"})

        status_store = StatusMetricStorage(
            can_calculate=False,
            pos_w_calc_store=[1, 1],
            neg_w_calc_store=[2, 2],
            rtt_store=[3, 3],
            ir_store=[4, 4],
            bgp_store=[5, 5],
            kl_store=[6, 6],
            ns_store=[7, 7])
        status_stores = {}
        status_stores["an_sdp_id"] = StatusStorage(status_store=status_store)

        entry = MeterPoint(
            name="a_status", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["a_status"] = entry
        location = int(metric_storage.LOCATION.SEEP)
        location = int(metric_storage.LOCATION.SEEP)
        self.metric_store._MetricStore__print_status(
            "a_status", 1, "Underlay",
            "a_value", "vals", "an_sdp_id", [0], "gx")
        mock_print.assert_called_with(
            "------------STATUS----------------\n"
            f"name='a_status'\n"
            f"location={location}\n"
            f"metric_type='Underlay'\n"
            f"value='a_value'\n"
            f"vals='vals'\n"
            f"store_key='an_sdp_id'\n"
            f"weights={[0]}\n"
            f"pos_store={[1, 1]}\n"
            f"neg_store={[2, 2]}\n"
            f"rtt_store={[3, 3]}\n"
            f"ir_store={[4, 4]}\n"
            f"kl_store={[6, 6]}\n"
            f"bgp_store={[5, 5]}\n"
            f"ns_store={[7, 7]}\n"
            f"if_type='gx'\n"
            "---------------------------------\n")

    def test_init_status_metric_if_required_part1(self):
        self.metric_store.clean_store()
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "321-321",
                                              "sdp": "an_sdp_id"})

        entry = MeterPoint(
            name="a_status", status_config=status_cfg,
            type="ObservableGauge", attributes={})
        self.metric_store.value_store["a_status"] = entry
        self.metric_store._MetricStore__init_status_metric_if_required(
            entry, "an_sdp_id")
        self.assertEqual("an_sdp_id" in self.metric_store.value_store[
            "a_status"].status_stores, True)
        self.assertEqual(len(self.metric_store.value_store[
            "a_status"].obs_store), 2)
        self.assertEqual(len(self.metric_store.value_store[
            "a_status"].attribute_store), 2)

    def test_init_status_metric_if_required_part2(self):
        self.metric_store.clean_store()
        status_cfg = StatusConfig(
            vrf_id="a_vrf_id", if_id="a_ir_id", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "321-321",
                                              "sdp": "an_sdp_id"})

        entry = MeterPoint(
            name="a_status", status_config=status_cfg,
            type="ObservableGauge", attributes={})
        self.metric_store.value_store["a_status"] = entry
        self.metric_store._MetricStore__init_status_metric_if_required(
            entry, "an_sdp_id")
        self.assertEqual("an_sdp_id" in self.metric_store.value_store[
            "a_status"].status_stores, True)
        self.assertEqual(len(self.metric_store.value_store[
            "a_status"].obs_store), 5)
        self.assertEqual(len(self.metric_store.value_store[
            "a_status"].attribute_store), 5)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_bgp_status_based_on_ncat_data")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_status_based_on_ncat_data")
    def test_set_status_based_on_ncat_updates(self, set_status_mock,
                                              set_bgp_status_mock):
        """Test set status based on ncat updates function"""

        self.metric_store.clean_store()
        nc_data = {'bgp_state': {'fvkau3': '15:37:34.476 Established'},
                   'dig_results': {'ppp100': {'res': 1, 'fqdn': '', 'dns_server': ''},
                                   'eth2.3917': {'res': 1, 'fqdn': '', 'dns_server': ''},
                                   'eth7.4033': {'res': 1, 'fqdn': '', 'dns_server': ''}},
                   'klive_loop_no': 747, 'system_name': '230000000102500'}

        status_cfg = StatusConfig(
            vrf_id="fvkau3", status_inst_type="Overlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123",
                                              "sdp": "230000000102500"})

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["230000000102500"] = StatusStorage(
            status_store=status_store)

        entry = MeterPoint(
            name="3893_overlay", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["3893_overlay"] = entry
        self.metric_store.set_status_based_on_netcat_updates(False, nc_data)
        set_status_mock.assert_called_with(
            entry, "230000000102500", inst_type="Overlay",
            bgp_state="15:37:34.476 Established",
            all_if_kl_state=2, bgp_excluded=False)

        self.metric_store.clean_store()
        status_cfg = StatusConfig(
            if_id="eth2_3917", status_inst_type="Underlay",
            status_store_setup=StatusMetricStorage(can_calculate=True),
            status_if_type="", prim_ctx_data={"ctx": "123-123",
                                              "sdp": "230000000102500"})

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["230000000102500"] = StatusStorage(
            status_store=status_store)
        entry = MeterPoint(
            name="3917_underlay", status_config=status_cfg,
            type="ObservableGauge", attributes={}, status_stores=status_stores)
        self.metric_store.value_store["3917_underlay"] = entry
        self.metric_store.set_status_based_on_netcat_updates(False, nc_data)
        set_status_mock.assert_called_with(
            entry, "230000000102500", inst_type="Underlay", kl_state=1,
            kl_excluded=False)

        self.metric_store.clean_store()

        status_store = StatusMetricStorage(
            can_calculate=True,
            pos_w_calc_store=[],
            neg_w_calc_store=[])
        status_stores = {}
        status_stores["230000000102500"] = StatusStorage(
            status_store=status_store)
        att_map = {"v_map": AttributeMaps(
            can_purge=False, store_map={"id": "fvkau3"})}
        entry = MeterPoint(
            name="3893.bgp_status", attribute_maps=att_map,
            type="ObservableGauge", attributes={})
        self.metric_store.value_store["3893.bgp_status"] = entry
        self.metric_store.set_status_based_on_netcat_updates(False, nc_data)
        set_bgp_status_mock.assert_called_with(
            "3893.bgp_status", "15:37:34.476 Established")

    def test_process_kl_state(self):
        """This function tests the kl state of an interface if its in
        the exclusion list"""

        self.metric_store.clean_store()
        kl_state_before = {
            'ppp100': {'res': 1, 'fqdn': '', 'dns_server': ''},
            'eth2.3917': {'res': 1, 'fqdn': '', 'dns_server': ''},
            'eth7.4033': {'res': 1, 'fqdn': '', 'dns_server': ''}}
        kl_state_after = {
            'ppp100': {'res': 1, 'fqdn': 'EXCLUDED', 'dns_server': 'EXCLUDED'},
            'eth2.3917': {'res': 1, 'fqdn': '', 'dns_server': ''},
            'eth7.4033': {'res': 1, 'fqdn': '', 'dns_server': ''}}
        excluded_ifs = ["ppp100"]
        self.metric_store._MetricStore__process_kl_state(
            kl_state_before, excluded_ifs)
        self.assertEqual(kl_state_before, kl_state_after)

    @mock.patch("output_unit.metric_storage.MetricStore."
                "_MetricStore__set_obs_store_metric")
    def test_set_bgp_status_based_on_ncat_data(self, set_obs_mock):
        self.metric_store.clean_store()
        entry = MeterPoint(
            name="a_metric",
            type="ObservableGauge", attributes={})
        self.metric_store.value_store["a_metric"] = entry
        self.metric_store._MetricStore__set_bgp_status_based_on_ncat_data(
            "a_metric", "15:37:34.476 Established")
        self.assertEqual(len(self.metric_store.value_store[
            "a_metric"].obs_store), 2)
        self.assertEqual(len(self.metric_store.value_store[
            "a_metric"].attribute_store), 2)
        set_obs_mock.assert_called()

    def test_get_state_entry_ncat_thread(self):
        state_data = metric_storage.StateData(
            values={"NF": 1, "DEG": 2}, inst_type="Underlay",
            th_type="ncat", def_ret_data={})
        status_store = StatusMetricStorage(
            kl_store=[1, 1, 1, 0, 0, 0],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 2})

        status_store = StatusMetricStorage(
            kl_store=[0, 0, 0, 0, 0, 0],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[0, 0, 0, 0, 0, 0, 0, 0],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[1, 1],
            ir_store=[1, 1, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {})

        state_data.inst_type = "Overlay"
        status_store = StatusMetricStorage(
            bgp_store=[0, 0, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            bgp_store=[1, 0, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {})

    def test_get_state_entry_timeout_thread(self):
        vals = {"NF": 1, "DEG": 2, "UN": 0}
        state_data = metric_storage.StateData(
            values=vals, inst_type="Underlay", th_type="tout",
            def_ret_data={'en_type': 'categorised', 'to_set': 0})
        status_store = StatusMetricStorage(
            kl_store=[1, 1, 1, 0, 0, 0],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 2})

        status_store = StatusMetricStorage(
            kl_store=[0, 0, 0, 0, 0, 0],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[0, 0, 0, 0, 0, 0, 0, 0],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[1, 1],
            ir_store=[1, 1, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            kl_store=[1, 1],
            ns_store=[1, 2, 3],
            ir_store=[1, 1, 1])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 0})

        state_data.inst_type = "Overlay"
        status_store = StatusMetricStorage(
            bgp_store=[0, 0, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 1})

        status_store = StatusMetricStorage(
            bgp_store=[1, 0, 0])
        res = self.metric_store._MetricStore__get_state_entry(
            status_store, state_data)
        self.assertEqual(res, {"en_type": "categorised", "to_set": 0})

    def test_have_all_if_klives_failed(self):
        kl_state = {'ppp100': {'res': 1},
                    'eth2.3917': {'res': 1},
                    'eth7.4033': {'res': 1}}

        self.assertEqual(
            self.metric_store._MetricStore__have_all_if_klives_failed(
                kl_state), 2)

        kl_state = {'ppp100': {'res': 0},
                    'eth2.3917': {'res': 0},
                    'eth7.4033': {'res': 0}}

        self.assertEqual(
            self.metric_store._MetricStore__have_all_if_klives_failed(
                kl_state), 1)

        kl_state = {'ppp100': {'wrong_res': 0},
                    'eth2.3917': {'res': 0},
                    'eth7.4033': {'res': 0}}
        self.assertEqual(
            self.metric_store._MetricStore__have_all_if_klives_failed(
                kl_state), 0)

        kl_state = {}
        self.assertEqual(
            self.metric_store._MetricStore__have_all_if_klives_failed(
                kl_state), 0)

    @mock.patch('builtins.print')
    def test_print_obs_entry(self, mock_print):
        self.metric_store._MetricStore__print_obs_entries(
            "ncat", "a_name", "obs_entries")
        mock_print.assert_called()
