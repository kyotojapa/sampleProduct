# pylint: disable=missing-docstring, W0212

import unittest
from unittest import mock

from opentelemetry.metrics import Counter
from opentelemetry.sdk.metrics._internal.aggregation import (
    AggregationTemporality, _SumAggregation)


from output_unit.otel_sdk_patch import (AugmentedView,
                                        get_data_points_with_cleanup,
                                        get_datapoint_keys_to_keep)


class TestOTELSDKPatch(unittest.TestCase):
    def setUp(self) -> None:
        self.view = AugmentedView(
            instrument_type=Counter,
            instrument_name="a_name",
            meter_name="a_scope",
            use_values_as_keys=False,
            attribute_values=None,
            attribute_keys=None,
            drop_unused_datapoints=False)

    def tearDown(self) -> None:
        del self.view

    def test_get_datapoint_keys_to_keep(self):
        id1 = 10
        id2 = 20
        attr1 = {f"k_{id1}": f"v_{id1}", f"k_{2*id1}": f"v_{2*id1}"}
        attr2 = {f"k_{id2}": f"v_{id2}", f"k_{2*id2}": f"v_{2*id2}"}
        values = []
        values.append(list(attr1.values()))
        values.append(list(attr2.values()))
        view = mock.MagicMock(_attribute_values=values)
        res = get_datapoint_keys_to_keep(view)
        self.assertEqual(res, [frozenset([f"v_{id1}", f"v_{2*id1}"]),
                               frozenset([f"v_{id2}", f"v_{2*id2}"])])

    @mock.patch("opentelemetry.sdk.metrics._internal.aggregation."
                "_SumAggregation.collect")
    def test_get_data_points_with_cleanup(self, mock_collect):
        id1 = 10
        key_set = frozenset([f"v_{id1}", f"v_{2*id1}"])
        attributes = {f"k_{id1}": f"v_{id1}", f"k_{2*id1}": f"v_{2*id1}"}
        attributes_aggregation = {}
        attributes_aggregation[key_set] = _SumAggregation(
            attributes, True, AggregationTemporality.CUMULATIVE, 1710397902)
        collection_aggregation_temporality = AggregationTemporality.CUMULATIVE
        collection_start_nanos = 1710397902
        keys_to_keep = [key_set]
        mock_collect.return_value = "a_data_point"
        points = get_data_points_with_cleanup(
            attributes_aggregation, self.view,
            collection_aggregation_temporality,
            collection_start_nanos, keys_to_keep)

        self.assertEqual(points, ["a_data_point"])

        keys_to_keep = []
        self.view._drop_unused_datapoints = True
        points = get_data_points_with_cleanup(
            attributes_aggregation, self.view,
            collection_aggregation_temporality,
            collection_start_nanos, keys_to_keep)

        self.assertEqual(points, [])
        self.assertEqual(attributes_aggregation, {})
