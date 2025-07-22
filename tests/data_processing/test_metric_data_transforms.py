import unittest
from datetime import datetime, timedelta
from unittest import mock

from data_processing import metric_data_transforms
from data_processing.exit_transform_loop_exception import ExitTransformLoopException
from data_processing.models.unified_model import UnifiedModel
from output_unit.meter_point import ObsMetStorage

from tests.controller.exception_mocks import PySNAPIException


class Messages:
    """Various types of messages"""

    valid = {
        "0": "",
        "Hd": {
            "Cl": {
                "goid": "",
                "info": "ppp0<-><IscCtxRemIf!1549>",
                "oid": "<MbcpDefCC!3464>",
                "ownr": "<IscCtx!1448>",
                "sinf": "",
            },
            "Ct": "lw.comp.ISC.context.snapshot",
            "Fu": "DoSnapshotLog",
            "P": {"Fi": "lw/shared/util/src/public/log2/LoggingObject.cpp", "Li": 862},
            "Ti": "20230831T124141.673414",
            "Ty": "Log",
        },
        "snapshot": {
            "ctx": {
                "ctxId": "481bc2a1-8a96-432b-862b-61b6e1a953cf",
                "oid": "<IscCtx!1448>",
            },
            "obj": "<MbcpDefCC!3464>",
            "sid": 33406,
            "val": {
                "Sta": {
                    "Bu": {"B": 112000},
                    "aBr": 12,
                    "cW": 4480,
                    "cc": {"sst": 4380},
                    "e": 1,
                    "inc": 6144,
                    "lR": 0,
                    "lgW": 4380,
                    "nSP": 0,
                    "oBr": 12,
                    "owB": 0,
                    "owP": 0,
                    "pBr": 35840000,
                    "rtt": {
                        "Sta": {"b": 0, "rrto": 26, "rto": 200, "s": 0, "v": 4},
                        "Sts": {"max": 109, "min": 1, "ns": 8940},
                    },
                    "ss": 0,
                },
                "Sts": {
                    "Bu": {
                        "nTu": 145308,
                        "nooB": 0,
                        "tSto": 0,
                        "tTkn": 647869,
                        "tTu": 759869,
                    },
                    "aI": {
                        "fB": 0,
                        "fP": 0,
                        "sB": 647869,
                        "sP": 10232,
                        "tB": 99,
                        "tP": 1,
                    },
                    "afB": 0,
                    "afE": 0,
                    "afP": 0,
                    "amE": 1,
                    "asE": 9911,
                    "c": 0,
                    "f": 0,
                    "idE": 13915,
                    "ifB": 0,
                    "ifE": 0,
                    "ifP": 0,
                    "nBuTrs": 1,
                    "nETrs": 1,
                    "nFCwndTrs": 0,
                    "p": 0,
                    "t": 0,
                    "wlE": 8940,
                },
            },
        },
    }

    valid_remif_snapshot = {
        "Hd": {
            "Ty": "Log",
            "P": {"Fi": "lw/shared/util/src/public/log2/LoggingObject.cpp", "Li": 862},
            "Ct": "lw.comp.ISC.context.snapshot",
            "Ti": "20240614T075706.237435",
            "Cl": {
                "ownr": "<IscCtx!207923>",
                "oid": "<IscCtxRemIf!207958>",
                "info": "",
                "sinf": "",
                "goid": "",
            },
            "Fu": "DoSnapshotLog",
        },
        "0": "",
        "snapshot": {
            "sid": 12241,
            "ctx": {
                "ctxId": "d4abf65b-327b-443f-9f7a-853466c493a2",
                "oid": "<IscCtx!207923>",
            },
            "obj": "<IscCtxRemIf!207958>",
            "val": {
                "Sta": {
                    "g": {
                        "S": {"cSt": 203, "bfCSC": 0, "svcCost": "1"},
                        "Ept": '{"Candidates":[{"Dom":"private","Ep":"10.200.76.248:4000","EpFmt":"IP","Prio":10001},{"Dom":"private","Ep":"10.200.2.90:4000","EpFmt":"IP"}]}',
                        "brrs": {"nBrrs": 2},
                    },
                    "tx": {"e": 1, "tuBu": 6710887, "Bu": {"B": 6710741}},
                },
                "Sts": {
                    "tx": {
                        "nETrs": 0,
                        "nBuTrs": 1,
                        "Bu": {
                            "tTu": 6715035,
                            "nTu": 86,
                            "nooB": 0,
                            "tTkn": 4294,
                            "tSto": 0,
                        },
                    }
                },
            },
        },
    }
    remif_snapshot_no_ctx = {
        "Hd": {
            "Ty": "Log",
            "P": {"Fi": "lw/shared/util/src/public/log2/LoggingObject.cpp", "Li": 862},
            "Ct": "lw.comp.ISC.context.snapshot",
            "Ti": "20240614T075706.237435",
            "Cl": {
                "ownr": "<IscCtx!207923>",
                "oid": "<IscCtxRemIf!207958>",
                "info": "",
                "sinf": "",
                "goid": "",
            },
            "Fu": "DoSnapshotLog",
        },
        "0": "",
        "snapshot": {
            "sid": 12241,
            "ctx": {
                "ctxId": "",
                "oid": "<IscCtx!207923>",
            },
            "obj": "<IscCtxRemIf!207958>",
            "val": {
                "Sta": {
                    "g": {
                        "S": {"cSt": 203, "bfCSC": 0, "svcCost": "1"},
                        "Ept": '{"Candidates":[{"Dom":"private","Ep":"10.200.76.248:4000","EpFmt":"IP","Prio":10001},{"Dom":"private","Ep":"10.200.2.90:4000","EpFmt":"IP"}]}',
                        "brrs": {"nBrrs": 2},
                    },
                    "tx": {"e": 1, "tuBu": 6710887, "Bu": {"B": 6710741}},
                },
                "Sts": {
                    "tx": {
                        "nETrs": 0,
                        "nBuTrs": 1,
                        "Bu": {
                            "tTu": 6715035,
                            "nTu": 86,
                            "nooB": 0,
                            "tTkn": 4294,
                            "tSto": 0,
                        },
                    }
                },
            },
        },
    }


class TestMetricDataTranforms(unittest.TestCase):
    """Test class for the metric_data_transforms module"""

    def setUp(self) -> None:
        self.valid_remif_msg = Messages.valid_remif_snapshot
        self.valid_msg = Messages.valid
        self.remif_msg_no_ctx = Messages.remif_snapshot_no_ctx
        self.valid_remif_model = UnifiedModel(**self.valid_remif_msg)
        self.remif_model_no_ctx = UnifiedModel(**self.remif_msg_no_ctx)
        self.valid_message_model = UnifiedModel(**self.valid_msg)
        mock_cmd = mock.MagicMock()
        self.attrs = {
            "seep_public_context_id": "16f27826-3c26-4ea6-9f20-5c71ade0bb88",
            "seep_private_context_id": "5db9137a-ac03-4053-a66d-c43acdee2619",
            "value": "2024-05-22 18:04:20",
            "sdep_public_context_id": "0aef00d2-3ac5-4026-85af-b8f91c2ad79b",
            "sdep_system_id": "00000000-0000-0000-0000-000000000002",
            "sdep_system_name": "Dev SDEP 2",
        }
        self.rl_ctx_res = True
        mock_cmd.retrieve_correlation_point_attributes.return_value = self.attrs
        self.correlation_dict = {
            "omt_cmd": mock_cmd,
            "location": "seep",
            "back_off_enabled": False,
            "hold_off": 300,
            "start_at": 60,
            "max": 14400,
            "exp": 1.3,
            "first_ctx_released_at": "",
            "counter": 1,
            "last_ct_chg_event_at": ""
        }

    def _extract_func_and_apply(self, func_name, *args, **kwargs):
        """We use this to test the '__' functions, because, by default,
        it will throw an error if you try to import them and use them
        directly. This will get the specific private function and
        run the function with the arguments or keyword arguments that
        you specify."""

        private_func = getattr(metric_data_transforms, func_name)
        return private_func(*args, **kwargs)

    def test_process_snapshot_generic_valid_msg(self):
        """Test the process_snapshot function"""

        conv_message = metric_data_transforms.process_snapshot(
            UnifiedModel.parse_obj(self.valid_msg), None)
        self.assertEqual(UnifiedModel.parse_obj(self.valid_msg), conv_message)

    def test_process_snapshot_invalid_msg(self):
        """Test the process_snapshot function"""

        with self.assertRaises(ExitTransformLoopException):
            metric_data_transforms.process_snapshot({}, None)

    @mock.patch("data_processing.metric_data_transforms.__process_remif_snapshot")
    def test_process_snapshot_generic_valid_remif(self, process_mock):
        """Test the process_snapshot function"""

        conv_message = metric_data_transforms.process_snapshot(
            message=UnifiedModel.parse_obj(self.valid_remif_msg), sampler=None, correlation_dict=self.correlation_dict
        )
        self.assertEqual(UnifiedModel.parse_obj(self.valid_remif_msg), conv_message)
        process_mock.assert_called_once_with(
            self.valid_remif_model, self.correlation_dict
        )

    def test_is_a_remif_snapshot(self):
        """Test to check if the snapshot contains RemIf"""

        is_valid = self._extract_func_and_apply(
            "__is_a_remif_snapshot", self.valid_remif_model
        )
        self.assertEqual(True, is_valid)

    def test_is_a_remif_snapshot_invalid_msg(self):
        """Test to check if the snapshot contains RemIf"""

        is_valid = self._extract_func_and_apply(
            "__is_a_remif_snapshot", self.valid_message_model
        )
        self.assertEqual(False, is_valid)

    @mock.patch(
        "data_processing.metric_data_transforms.__retrieve_attributes_and_set_metric"
    )
    def test_process_remif_snapshot_no_rem_id(
        self, retrieve_attr_mock
    ):
        """Test the process remif snapshot but no remid"""

        self._extract_func_and_apply(
            "__process_remif_snapshot", self.remif_model_no_ctx, self.correlation_dict
        )
        retrieve_attr_mock.assert_called()

    @mock.patch("data_processing.metric_data_transforms.MetricStore")
    @mock.patch("data_processing.metric_data_transforms.__get_rem_id")
    def test_process_remif_snapshot(self, get_remid_mock, metric_store_mock):
        """Test the process remif snapshot with the store populated and a valid ctx id"""

        attrs = {
            "seep_private_context_id": "d4abf65b-327b-443f-9f7a-853466c493a2",
            "seep_remote_interface_id": 123456,
        }
        get_remid_mock.return_value = 123456
        metric_store_mock.return_value.read_obs_metric.return_value = {
            "key": ObsMetStorage(attr=attrs, iter_val=1686990896000)
        }
        metric_store_mock.return_value.get_param_from_attribute_map.return_value = (
            "some-interface"
        )
        self._extract_func_and_apply(
            "__process_remif_snapshot", self.valid_remif_model, self.correlation_dict
        )
        metric_store_mock.return_value.update_attribute_map.assert_called()

    @mock.patch(
        "data_processing.metric_data_transforms.MetricStore.get_param_from_attribute_map"
    )
    def test_get_remid(self, get_value_mock):
        """Test to get the remid and path"""

        get_value_mock.return_value = "207958"
        location = "seep"
        correlation_dict = {"omt_cmd": mock.MagicMock, "location": "seep"}
        pub_ctx_id = "abcd-1234"
        rem_pub_ctx_id = "abce-1234"
        ctx_born_stamp = datetime.utcnow()
        rem_id = "207958"
        attrs = self._extract_func_and_apply(
            "__get_remid_and_path",
            correlation_dict=correlation_dict,
            local_pub_context_id=pub_ctx_id, ctx_born_stamp=ctx_born_stamp,
            rem_pub_context_id=rem_pub_ctx_id, rem_id=rem_id)
        self.assertEqual("207958", attrs[f"{location}_remote_interface_id"])

    @mock.patch(
        "data_processing.metric_data_transforms.MetricStore.get_param_from_attribute_map"
    )
    @mock.patch(
        "data_processing.metric_data_transforms.__release_ctx_if_permitted"
    )
    def test_get_remid_and_path_no_if_name(self, rl_ctx_mock, get_value_mock):
        """Test to get the remid and path"""

        get_value_mock.return_value = None
        rem_location = "sdep"
        correlation_dict = {"omt_cmd": mock.MagicMock, "location": "seep"}
        pub_ctx_id = "abcd-1234"
        rem_pub_ctx_id = "abce-1234"
        ctx_born_stamp = datetime.utcnow()
        rem_id = "12345"
        attrs = self._extract_func_and_apply(
            "__get_remid_and_path",
            correlation_dict=correlation_dict,
            local_pub_context_id=pub_ctx_id, ctx_born_stamp=ctx_born_stamp,
            rem_pub_context_id=rem_pub_ctx_id, rem_id=rem_id
        )

        self.assertEqual(True, f"{rem_location}_underlay_network_path" not in attrs)
        rl_ctx_mock.assert_called_once()

    @mock.patch(
        "data_processing.metric_data_transforms.MetricStore.get_param_from_attribute_map"
    )
    @mock.patch(
        "data_processing.metric_data_transforms.__release_ctx_if_permitted"
    )
    def test_get_remid_and_path_no_met_path(self, rl_ctx_mock, get_value_mock):
        """Test to get the remid and path"""

        get_value_mock.return_value = None
        rem_location = "sdep"
        correlation_dict = {"omt_cmd": mock.MagicMock, "location": "seep"}
        pub_ctx_id = "abcd-1234"
        rem_pub_ctx_id = "abce-1234"
        ctx_born_stamp = datetime.utcnow()
        rem_id = "12345"
        attrs = self._extract_func_and_apply(
            "__get_remid_and_path",
            correlation_dict=correlation_dict,
            local_pub_context_id=pub_ctx_id, ctx_born_stamp=ctx_born_stamp,
            rem_pub_context_id=rem_pub_ctx_id, rem_id=rem_id
        )
        get_value_mock.assert_called_once()
        rl_ctx_mock.assert_called_once()
        self.assertEqual(True, f"{rem_location}_underlay_network_path" not in attrs)

    def test_get_corr_attrs_from_snapi(self):
        """Test to retrieve the correlation instrument attr and born timestamp
        from snap"""

        snapi_attrs = self._extract_func_and_apply(
            "__get_corr_attrs_from_snapi", self.valid_remif_model, self.correlation_dict
        )
        self.assertEqual(snapi_attrs, self.attrs)

    def test_get_corr_attrs_from_snapi_exc(self):
        """Test to retrieve the correlation instrument attr and born timestamp
        from snap"""

        mock_cmd = mock.MagicMock()
        mock_cmd.retrieve_correlation_point_attributes.side_effect = PySNAPIException()
        correlation_dict = {"omt_cmd": mock_cmd, "location": "seep"}
        with self.assertRaises(PySNAPIException):
            self._extract_func_and_apply(
                "__get_corr_attrs_from_snapi", self.valid_remif_model, correlation_dict
            )

    def test_release_context(self):
        """Test to release context over snapi function"""
        mock_cmd = mock.MagicMock()
        mock_cmd.release_context.return_value = True
        correlation_dict = {"omt_cmd": mock_cmd, "location": "seep"}
        res = self._extract_func_and_apply(
            "__release_context_over_snapi", correlation_dict, "an_id"
        )
        self.assertEqual(res, self.rl_ctx_res)

    def test_release_context_exc(self):
        """Test to cause exception when releasing context over snapi"""

        mock_cmd = mock.MagicMock()
        mock_cmd.release_context.side_effect = PySNAPIException()
        correlation_dict = {"omt_cmd": mock_cmd, "location": "seep"}
        ctx_id = "an_id"
        with self.assertRaises(PySNAPIException):
            self._extract_func_and_apply(
                "__release_context_over_snapi", correlation_dict, ctx_id
            )

    @mock.patch("data_processing.metric_data_transforms.__get_remid_and_path")
    @mock.patch("data_processing.metric_data_transforms.MetricStore.set_metric")
    def test_retrieve_attributes_and_set_metric(self, rem_id_path_mock, set_metric_mock):
        """Test to retrieve attributes and set the metric"""
        rem_id_path_mock.return_value = {"id": "id", "path": "path"}
        rem_id = "12345"
        self._extract_func_and_apply(
            "__retrieve_attributes_and_set_metric",
            self.valid_remif_model,
            self.correlation_dict, rem_id
        )
        set_metric_mock.assert_called_once()

    @mock.patch("data_processing.metric_data_transforms.MetricStore.set_metric")
    def test_retrieve_attributes_and_set_metric_no_snapi_attrs(self, set_metric_mock):
        """Test to retrieve attributes and set the metric"""

        mock_cmd = mock.MagicMock()
        attrs = {}
        mock_cmd.retrieve_correlation_point_attributes.return_value = attrs
        correlation_dict = {"omt_cmd": mock_cmd, "location": "seep"}
        rem_id = "12345"
        self._extract_func_and_apply(
            "__retrieve_attributes_and_set_metric",
            self.valid_remif_model,
            correlation_dict, rem_id
        )
        set_metric_mock.assert_not_called()

    @mock.patch("data_processing.metric_data_transforms.__get_remid_and_path")
    @mock.patch("data_processing.metric_data_transforms.MetricStore.set_metric")
    def test_retrieve_attributes_and_set_metric_no_other_attrs(
        self, set_metric_mock, get_remid_mock
    ):
        """Test to retrieve attributes and set the metric"""

        get_remid_mock.return_value = None
        rem_id = "12345"
        self._extract_func_and_apply(
            "__retrieve_attributes_and_set_metric",
            self.valid_remif_model,
            self.correlation_dict, rem_id
        )
        set_metric_mock.assert_not_called()


    @mock.patch("data_processing.metric_data_transforms.__release_context_over_snapi")
    @mock.patch("data_processing.metric_data_transforms.logging")
    def test_release_ctx_if_permitted_scenario_no_id(self, log_mock, rl_ctx_mock):
        ctx_born_stamp = datetime.utcnow() - timedelta(seconds=60)
        mock_cmd = mock.MagicMock()
        correlation_dict = {
            "omt_cmd": mock_cmd, "location": "seep",
            "rl_ctx_once_period": 60,
            "ctx_active_for_period": 60,
            "start_at": 60,
            "back_off_enabled": False,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 120,
            "counter": 1,
            "last_ct_chg_event_at": datetime.utcnow(),
            "disabled_queries": []}
        pub_context_id = "a_ctx_id"
        rem_pub_context_id = "another_ctx_id"
        if_name = "a_name"
        met_path = "a_path"
        if_id = "an_if_id"
        # rem_id_value = "N/A"
        self._extract_func_and_apply(
            "__release_ctx_if_permitted",
            if_id, correlation_dict,
            pub_context_id, ctx_born_stamp, if_name, met_path,
            rem_pub_context_id)
        log_mock.info.assert_called_with(
            "Invoking release context command to "
            "acquire id to interface map mapping for context "
            f"with local public context id: {pub_context_id} "
            f"with remote public context id: {rem_pub_context_id} "
            f"with interface id: {if_name} "
            f"with path: {met_path} "
            f"with remid: {if_id} "
            f"with born stamp: {ctx_born_stamp} "
            "REASON: NO ID PRESENT")

        rl_ctx_mock.assert_called_with(correlation_dict, pub_context_id)
        self.assertNotEqual(correlation_dict["first_ctx_released_at"], "")

        correlation_dict = {
            "omt_cmd": mock_cmd, "location": "seep",
            "rl_ctx_once_period": 60,
            "ctx_active_for_period": 60,
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": datetime.utcnow() - timedelta(seconds=60),
            "hold_off": 60,
            "max": 120,
            "counter": 1,
            "exp": 1.30,
            "last_ct_chg_event_at": datetime.utcnow()
        }
        self._extract_func_and_apply(
            "__release_ctx_if_permitted",
            if_id, correlation_dict,
            pub_context_id, ctx_born_stamp, if_name, met_path,
            rem_pub_context_id)
        self.assertNotEqual(correlation_dict["counter"], 2)

    @mock.patch("data_processing.metric_data_transforms.MetricStore.get_param_from_attribute_map")
    @mock.patch("data_processing.metric_data_transforms.__release_context_over_snapi")
    @mock.patch("data_processing.metric_data_transforms.MetricStore.update_attribute_map")
    @mock.patch("data_processing.metric_data_transforms.logging")
    def test_release_ctx_if_permitted_scenario_timer_not_expired(
            self, log_mock, update_param_mock, rl_ctx_mock, get_param_mock):
        ctx_born_stamp = datetime.utcnow()
        mock_cmd = mock.MagicMock()
        correlation_dict = {
            "omt_cmd": mock_cmd, "location": "seep",
            "rl_ctx_once_period": 60,
            "ctx_active_for_period": 60,
            "start_at": 60,
            "back_off_enabled": False,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 60,
            "counter": 1}
        pub_context_id = "a_ctx_id"
        rem_pub_context_id = "another_ctx_id"
        if_name = "a_name"
        met_path = "a_path"
        if_id = "an_if_id"
        rem_id_value = "a_rem_id"
        get_param_mock.return_value = rem_id_value
        self._extract_func_and_apply(
            "__release_ctx_if_permitted",
            if_id, correlation_dict,
            pub_context_id, ctx_born_stamp, if_name, met_path,
            rem_pub_context_id)
        rl_ctx_mock.assert_not_called()

    def test_get_context_release_interval(self):
        "Test for exponential back off function"

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": False,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 60,
            "counter": 1
        }
        rl_interval, can_increment = self._extract_func_and_apply(
            "__get_context_release_interval",
            correlation_dict)
        self.assertEqual(rl_interval, 60)
        self.assertEqual(can_increment, False)

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 60,
            "counter": 1
        }
        rl_interval, can_increment = self._extract_func_and_apply(
            "__get_context_release_interval",
            correlation_dict)
        self.assertEqual(rl_interval, 60)
        self.assertEqual(can_increment, False)

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": datetime.utcnow(),
            "hold_off": 60,
            "max": 60,
            "counter": 1
        }
        rl_interval, can_increment = self._extract_func_and_apply(
            "__get_context_release_interval",
            correlation_dict)
        self.assertEqual(rl_interval, 60)
        self.assertEqual(can_increment, False)

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": datetime.utcnow() - timedelta(
                seconds=correlation_dict["hold_off"]),
            "hold_off": 60,
            "max": 120,
            "counter": 1,
            "exp": 1.30
        }
        rl_interval, can_increment = self._extract_func_and_apply(
            "__get_context_release_interval",
            correlation_dict)
        self.assertEqual(rl_interval, 78)
        self.assertEqual(can_increment, True)

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": datetime.utcnow() - timedelta(
                seconds=correlation_dict["hold_off"]),
            "hold_off": 60,
            "max": 120,
            "counter": 10,
            "exp": 1.30
        }
        rl_interval, can_increment = self._extract_func_and_apply(
            "__get_context_release_interval",
            correlation_dict)
        self.assertEqual(rl_interval, 120)
        self.assertEqual(can_increment, False)

    def test__set_backoff_props(self):
        "Method to test set_backoff_props function"

        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 120,
            "counter": 1,
            "exp": 1.30,
            "last_ct_chg_event_at":  ""
        }
        rl_interval = 10
        can_increment = True
        self._extract_func_and_apply(
            "__set_back_off_props",
            correlation_dict, rl_interval, can_increment, True)
        self.assertEqual(correlation_dict["counter"], 2)
        self.assertNotEqual(correlation_dict["first_ctx_released_at"], "")
        chg_event_at = datetime.utcnow() - timedelta(seconds=rl_interval)
        correlation_dict = {
            "start_at": 60,
            "back_off_enabled": True,
            "first_ctx_released_at": "",
            "hold_off": 60,
            "max": 120,
            "counter": 2,
            "exp": 1.30,
            "last_ct_chg_event_at": chg_event_at
        }
        self._extract_func_and_apply(
            "__set_back_off_props",
            correlation_dict, rl_interval, can_increment, False)
        self.assertEqual(correlation_dict["counter"], 1)
        self.assertNotEqual(
            correlation_dict["first_ctx_released_at"], chg_event_at)
