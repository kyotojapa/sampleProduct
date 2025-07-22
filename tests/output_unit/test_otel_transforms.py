"""Module to test the OTELMetrics Transforms"""

import unittest

from output_unit.meter_point import ObsMetStorage
from output_unit.otel_conf_model import MetricAttribute
from output_unit.transforms.otel_trans_func import (
    _parse_pdb_field,
    add,
    categorize_tx_data,
    classify_resp_code,
    classify_rx_dropped_pdus,
    classify_tx_dropped_pdus,
    get_context_id,
    get_context_id_mbcp,
    get_interface_id,
    get_interface_state,
    get_rx_bitrate,
    get_sched_group,
    get_tenant_id,
    get_tx_bitrate,
    gt,
    parse_float,
    parse_int,
    return_mbcp_msg_dict,
    transform_mbcp_data_seep,
    transform_tx_data_seep,
    transform_mbcp_data_sdep,
    transform_tx_data_sdep,
    transform_mbcp_data,
    transform_tx_data,
    get_remid_from_mbcp_msg,
    get_ifid_from_mbcp_msg,
    get_bgp_state_data,
    categorize_demanded_metrics,
    categorize_supplied_overlay_metrics
)


class TestOTELMetrics(unittest.TestCase):
    """Class to perform the tests on the OTELMetrics class"""

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_parse_int(self):
        "Test to check if parse_int is working"

        self.assertEqual(3, parse_int("3"))

    def test_parse_int_returns_default(self):
        "Test to check if parse_int is returning default value"

        self.assertEqual(-1, parse_int("hello"))

    def test_parse_float(self):
        "Test to check if parse_float is working"

        self.assertEqual(3.3, parse_float("3.3"))

    def test_parse_float_returns_default(self):
        "Test to check if parse_float is returning default value"

        self.assertEqual(-1.0, parse_float("hello"))

    def test_get_context_id(self):
        "Test to check if get_context_id is working"

        self.assertEqual("3232", get_context_id("<IrMgr!3232>"))

    def test_get_context_id_is_returing_empty(self):
        "Test to check if get_context_id is returning empty"

        self.assertEqual("", get_context_id("IrMgr!3232"))

    def test_get_interface_id(self):
        "Test to check if get_interface_id is working"

        self.assertEqual("eth2.4033", get_interface_id("Ethernet_eth2.4033"))
        self.assertEqual("ppp100", get_interface_id("ppp100"))

    def test_get_interface_is_returning_na(self):
        "Test to check if get_interface_id is returning na"

        self.assertEqual("N/A", get_interface_id("helloworld"))

    def test_get_interface_state(self):
        "Test to check if get_interface_state is working"

        self.assertEqual(1, get_interface_state(r"eth2.4033(Ethernet:Up{})"))
        self.assertEqual(0, get_interface_state(r"eth2.4033(Ethernet:Down{})"))
        self.assertEqual(0, get_interface_state(r"ppp100(PPP:Down{})"))
        self.assertEqual(1, get_interface_state(r"ppp100(PPP:Up{})"))

    def test_get_interface_state_returns_default(self):
        "Test to check if get_interface_state returns default"

        self.assertEqual(-1, get_interface_state(r"eth2.4033(Eth:isUp{})"))

    def test_parse_pdb_field(self):
        """Test to check if all k-v pairs are returned."""
        data = "<a>1</a><b>2</b><c>3</c>"
        result = _parse_pdb_field(data)
        self.assertListEqual(result, [("a", "1"), ("b", "2"), ("c", "3")])

    def test_get_rx_bitrate(self):
        """Test to check if valid RxBitrate is parsed and returned"""
        expected = 12
        data = f"<a>1</a><b>2</b><c>3</c><RxBitrate>{expected}</RxBitrate>"
        result = get_rx_bitrate(data)
        self.assertEqual(result, expected)

    def test_get_rx_bitrate_invalid_input(self):
        """Test to check if default is returned for invalid data"""
        expected = -1
        data = "<a>1</a><b>2</b><c>3</c>"
        result = get_rx_bitrate(data)
        self.assertEqual(result, expected)

    def test_get_tx_bitrate(self):
        """Test to check if valid TxBitrate is parsed and returned"""
        expected = 12
        data = f"<a>1</a><b>2</b><c>3</c><TxBitrate>{expected}</TxBitrate>"
        result = get_tx_bitrate(data)
        self.assertEqual(result, expected)

    def test_get_tx_bitrate_invalid_input(self):
        """Test to check if default is returned for invalid data"""
        expected = -1
        data = "<a>1</a><b>2</b><c>3</c>"
        result = get_tx_bitrate(data)
        self.assertEqual(result, expected)

    def test_classify_resp_code(self):
        "Test to check if classify resp code returns the write value"

        self.assertEqual("SUCCESS", classify_resp_code("233"))
        self.assertEqual("ERROR", classify_resp_code("433"))
        self.assertEqual("UNKNOWN RESPONSE CODE", classify_resp_code("833"))
        self.assertEqual("UNKNOWN RESPONSE CODE", classify_resp_code("hello"))

    def test_get_tenant_id(self):
        "Test to check if classify resp code returns the write value"

        self.assertEqual("fvkau3", get_tenant_id("fvkau3-class-3"))
        self.assertEqual("N/A", get_tenant_id("-fvkau3-class-3"))

    def test_get_sched_group(self):
        "Test to check if classify resp code returns the write value"

        self.assertEqual("class-3", get_sched_group("fvkau3-class-3@"))
        self.assertEqual("N/A", get_sched_group("fvkau3-class-3-"))

    def test_classify_tx_dropped_pdus(self):
        "Test to check if classify_tx_dropped_pdus returns the right value"
        res1 = [
            {"val": 1, "att_dct": {"reason": "acknowledged_lost"}},
            {"val": 2, "att_dct": {"reason": "timed_out"}},
            {"val": 3, "att_dct": {"reason": "locally"}},
            {"val": 4, "att_dct": {"reason": "in_transit"}},
            {"val": 5, "att_dct": {"reason": "unknown"}},
        ]
        values = [1, 2, 3, 4, 5]
        self.assertEqual(res1, classify_tx_dropped_pdus(values))

    def test_classify_rx_dropped_pdus(self):
        "Test to check if classify_rx_dropped_pdus returns the right value"
        res1 = [
            {"val": 1, "att_dct": {"reason": "lost"}},
            {"val": 2, "att_dct": {"reason": "out_of_order"}},
            {"val": 3, "att_dct": {"reason": "duplicated"}},
            {"val": 4, "att_dct": {"reason": "too_old"}},
        ]
        values = [1, 2, 3, 4]
        self.assertEqual(res1, classify_rx_dropped_pdus(values))

    def test_return_mbcp_msg_dict(self):
        "Test to check if return_mbcp_msg_dict returns the right value"
        res1 = [
            {"val": 1, "att_dct": {"category": "successfully_sent"}},
            {"val": 2, "att_dct": {"category": "failed"}},
            {"val": 3, "att_dct": {"category": "timed_out"}},
        ]
        values = [1, 2, 3]
        self.assertEqual(res1, return_mbcp_msg_dict(values))

    def test_categorize_tx_data(self):
        """Test to check that categorize_tx_data return the right value"""
        attrs = [
            {"val": 5, "att_dct": {"category": "user_data"}},
            {"val": 1, "att_dct": {"category": "retransmissions"}},
            {"val": 2, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [5, 6, 8]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32 - 1000, "att_dct": {"category": "user_data"}},
            {"val": 500, "att_dct": {"category": "retransmissions"}},
            {"val": 1000, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32 - 1000, 2**32 - 500, 2**32 + 500]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32 - 1000, "att_dct": {"category": "user_data"}},
            {"val": 1000, "att_dct": {"category": "retransmissions"}},
            {"val": 500, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32 - 1000, 2**32, 2**32 + 500]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32, "att_dct": {"category": "user_data"}},
            {"val": 500, "att_dct": {"category": "retransmissions"}},
            {"val": 500, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32, 2**32 + 500, 2**32 + 1000]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32 - 1000, "att_dct": {"category": "user_data"}},
            {"val": 500, "att_dct": {"category": "retransmissions"}},
            {"val": 1000, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32 - 1000, 2**32 - 500, 501]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32 - 1000, "att_dct": {"category": "user_data"}},
            {"val": 1500, "att_dct": {"category": "retransmissions"}},
            {"val": 1000, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32 - 1000, 501, 1501]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 1000, "att_dct": {"category": "user_data"}},
            {"val": 500, "att_dct": {"category": "retransmissions"}},
            {"val": 4294967293, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [1000, 1500, 1498]
        self.assertEqual(attrs, categorize_tx_data(values))

        attrs = [
            {"val": 2**32 + 1000, "att_dct": {"category": "user_data"}},
            {"val": 500, "att_dct": {"category": "retransmissions"}},
            {"val": 18446744069414584317, "att_dct": {"category": "channel_overheads"}},
        ]
        values = [2**32 + 1000, 2**32 + 1500, 1498]
        self.assertEqual(attrs, categorize_tx_data(values))

    def test_add(self):
        "Test to check add returns the right value"

        self.assertEqual([{"val": 3}], add([1, 2]))
        self.assertEqual([{"val": -1}], add(["str", 2]))

    def test_gt(self):
        "Test to check gt returns the right value"

        self.assertEqual(True, gt([1, 2]))
        self.assertEqual(False, gt([2, 1]))
        self.assertEqual(False, gt([1, 2, 3]))

    def test_get_context_id_mbcp(self):
        "Test to check get_context_id_mbcp returns the right value"

        self.assertEqual("12345", get_context_id_mbcp("<IscCtx!12345>"))
        self.assertEqual("", get_context_id_mbcp("<IscCtxs!12345>"))

    def test_transform_tx_data_seep(self):
        "Test to check transform_tx_data returns the right value"

        resp1 = [
            {
                "val": {
                    "ud": 1,
                    "ret": 2,
                    "ctx_id": "123-123",
                    "attributes": {
                        "dp1": {
                            "sampling_method": MetricAttribute(value="raw")},
                        "dp2": {
                            "sampling_method": MetricAttribute(
                                value="categorised")}
                    }
                }
            }
        ]
        resp2 = [
            {
                "val": {
                    "ud": -1,
                    "ret": -1,
                    "ctx_id": "",
                    "attributes": {
                        "dp1": {
                            "sampling_method": MetricAttribute(
                                value="raw")},
                        "dp2": {
                            "sampling_method": MetricAttribute(
                                value="categorised")}
                    }
                }
            }
        ]

        self.assertEqual(resp1, transform_tx_data_seep([1, 3, "123-123"]))
        self.assertEqual(resp2, transform_tx_data_seep([1, 2]))

    def test_transform_tx_data_sdep(self):
        "Test to check transform_tx_data returns the right value"

        resp1 = [
            {
                "val": {
                    "ud": 1,
                    "ret": 2,
                    "ctx_id": "123-123",
                    "attributes": {
                        "dp1": {
                            "sampling_method": MetricAttribute(value="raw"),
                            "context_id": MetricAttribute(value="123-123")},
                        "dp2": {
                            "sampling_method": MetricAttribute(
                                value="categorised"),
                            "context_id": MetricAttribute(value="123-123")}
                    }
                }
            }
        ]
        resp2 = [
            {
                "val": {
                    "ud": -1,
                    "ret": -1,
                    "ctx_id": "",
                    "attributes": {
                        "dp1": {
                            "sampling_method": MetricAttribute(value="raw")},
                        "dp2": {
                            "sampling_method": MetricAttribute(
                                value="categorised")}
                    }
                }
            }
        ]

        self.assertEqual(resp1, transform_tx_data_sdep([1, 3, "123-123"]))
        self.assertEqual(resp2, transform_tx_data_sdep([1, 2]))

    def test_transform_mbcp_data_seep(self):
        "Test to check transform_tx_data returns the right value"

        resp1 = [
            {
                "val": {
                    "sB": 1,
                    "fB": 5,
                    "rtt": 4,
                    "ctx_id": "12-12",
                    "if_id": "abcd123",
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
            }
        ]
        resp2 = [
            {
                "val": {
                    "sB": -1,
                    "fB": -1,
                    "rtt": -1,
                    "ctx_id": "",
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
            }
        ]

        self.assertEqual(resp1,
                         transform_mbcp_data_seep(
                             [1, 2, 3, 4, "12-12", "abcd123<->an_id!12345"]))
        self.assertEqual(resp2,
                         transform_mbcp_data_seep([1, 2, 3]))

    def test_transform_mbcp_data_sdep(self):
        "Test to check transform_tx_data returns the right value"

        resp1 = [
            {
                "val": {
                    "sB": 1,
                    "fB": 5,
                    "rtt": 4,
                    "ctx_id": "12-12",
                    "if_id": "abcd123",
                    "rem_id": "12345",
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
            }
        ]
        resp2 = [
            {
                "val": {
                    "sB": -1,
                    "fB": -1,
                    "rtt": -1,
                    "ctx_id": "",
                    "attributes": {
                        "dp1": {"sampling_method": MetricAttribute(
                            value="raw")},
                        "dp2": {"sampling_method": MetricAttribute(
                            value="categorised")}
                    }
                }
            }
        ]

        self.assertEqual(resp1,
                         transform_mbcp_data_sdep(
                             [1, 2, 3, 4, "12-12", "abcd123<->an_id!12345"]))
        self.assertEqual(resp2,
                         transform_mbcp_data_sdep([1, 2, 3]))

    def test_get_remid_from_mbcp_msg(self):
        "Test to check if remid can be extracted"

        self.assertEqual("12345", get_remid_from_mbcp_msg(
            "abcd123<->an_id!12345"))
        self.assertEqual("", get_remid_from_mbcp_msg("abcd123<>an_id!12345"))
        self.assertEqual("", get_remid_from_mbcp_msg("abcd123<->an_id_12345"))

    def test_get_ifid_from_mbcp_msg(self):
        "Test to check if remid can be extracted"

        self.assertEqual("abcd123", get_ifid_from_mbcp_msg(
            "abcd123<->an_id!12345"))
        self.assertEqual("", get_ifid_from_mbcp_msg("abcd123<>an_id!12345"))

    def test_transform_tx_data(self):
        "Test to check transform_tx_data returns the right value"

        self.assertEqual([{"val": {"ud": 1, "ret": 2, "ctx_id": "123-123"},
                         "att_dct": {"sampling_method": "categorised"}}],
                         transform_tx_data([1, 3, "123-123"]))
        self.assertEqual([{"val": {"ud": -1, "ret": -1, "ctx_id": ""},
                         "att_dct": {"sampling_method": "categorised"}}],
                         transform_tx_data([1, 2, 3, ""]))

    def test_transform_mbcp_data(self):
        "Test to check transform_tx_data returns the right value"

        self.assertEqual([{"val": {"sB": 1, "fB": 5, "rtt": 4,
                                   "ctx_id": "123-123"},
                         "att_dct": {"sampling_method": "categorised"}}],
                         transform_mbcp_data([1, 2, 3, 4, "123-123"]))
        self.assertEqual([{"val": {"sB": -1, "fB": -1,
                                   "rtt": -1, "ctx_id": ""},
                         "att_dct": {"sampling_method": "categorised"}}],
                         transform_mbcp_data([1, 2, 3]))

    def test_get_bgp_state_data(self):
        "Test to check get_bgp_state_data returns the right value"
        obs_store = {"key1": ObsMetStorage(attr={"type": "bgp_state"}),
                     "key2": ObsMetStorage(attr={"type": "bgp_state_timer"})}
        res = get_bgp_state_data(
            obs_store, "00:00:01.012 Established")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 6},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 1}})
        res = get_bgp_state_data(
             obs_store, "10:00:01.012 openconfirm state")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 5},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 36001}})
        res = get_bgp_state_data(
             obs_store, "11:00:01.012 opensent is the state")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 4},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 39601}})
        res = get_bgp_state_data(
             obs_store, "11:00:01.012 an active is the state")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 3},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 39601}})
        res = get_bgp_state_data(
             obs_store, "11:00:01.012 a connect is the state")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 2},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 39601}})
        res = get_bgp_state_data(
             obs_store, "11:00:01.012 an idle is the state")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 1},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 39601}})
        res = get_bgp_state_data(
             obs_store, "time and state not present")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 0},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 0}})

    def test_get_bgp_state_data2(self):
        "Test to check get_bgp_state_data returns the right value"
        obs_store = {"key1": ObsMetStorage(attr={"type": "bgp_state"}),
                     "key2": ObsMetStorage(attr={"type": "bgp_state_timer"})}
        res = get_bgp_state_data(
            obs_store, "00:00:01.012 Established")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 6},
                  "key2": {"en_type": "bgp_state_timer", "to_set": 1}})

        res = get_bgp_state_data(
            obs_store, "2024-10-1 Established")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 6},
                  "key2": {"en_type": "bgp_state_timer",
                           "to_set": 1727740800}})

        res = get_bgp_state_data(
            obs_store, "2024-10-1 00:00:01.012 Established")
        self.assertEqual(
            res, {"key1": {"en_type": "bgp_state", "to_set": 6},
                  "key2": {"en_type": "bgp_state_timer",
                           "to_set": 1727740801}})

    def test_categorize_demanded_metrics(self):
        "Test to check if categorize_demanded_overlay_data returns the right value"
        # Scenario when 4 metrics provided:
        data = [1, 2, 3, 4, "test_vrf_id"]
        result = categorize_demanded_metrics(data)
        for k, v in enumerate(result):
            self.assertEqual(v["val"], data[k])

        #Scenario when less than 4 metrics provided:
        data = [1, 2, 3, "test_vrf_id"]
        result = categorize_demanded_metrics(data)
        for _, v in enumerate(result):
            self.assertEqual(v["val"], -1)

        #Scenario when more than 4 metrics provided:
        data = [1, 2, 3, 4, 5, "test_vrf_id"]
        result = categorize_demanded_metrics(data)
        for k, v in enumerate(result):
            self.assertEqual(v["val"], data[k])
        
        self.assertEqual(len(result), 4)

    
    def test_categorize_supplied_overlay_metrics(self):
        "Test to check if categorize_supplied_overlay_metrics returns the right value"
        
        attrs = [
            {"val":5, 
             "att_dct": {
                "type": "transmitted",
                "category": "user_data",
                "sub-category_a": "transmitted_by_peer",
                "source": "scheduler",
                "scope": "overlay_network"
                }
            }
        ]

        self.assertEqual(attrs, categorize_supplied_overlay_metrics([5, "test_vrf_id"]))
        # more than 2 values test
        self.assertEqual(attrs, categorize_supplied_overlay_metrics([5,6,"test_vrf_id"]))
        # test with less than 2 values
        self.assertNotEqual(attrs, categorize_supplied_overlay_metrics([5]))
        self.assertEqual(-1, categorize_supplied_overlay_metrics([5])[0]["val"])
    
