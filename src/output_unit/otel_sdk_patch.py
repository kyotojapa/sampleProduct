"""Module Used to patch the opentelemetry sdk"""
# pylint: disable=W0212
import functools
from typing import List, Union, Dict

from opentelemetry.sdk.metrics._internal._view_instrument_match import (
    DefaultAggregation,
    _ViewInstrumentMatch,
)
from opentelemetry.sdk.metrics.view import View


class AugmentedView(View):
    """Class used to augment the opentelemetry View class"""

    def __init__(
            self, *args, drop_unused_datapoints: bool = False,
            attribute_values: Union[List[List[Union[int, str]]],
                                    None] = None,
            use_values_as_keys: bool = False, **kwargs):
        self._attribute_values = attribute_values
        self._drop_unused_datapoints = drop_unused_datapoints
        self._use_values_as_keys = use_values_as_keys
        super().__init__(*args, **kwargs)


@functools.wraps(_ViewInstrumentMatch.consume_measurement)
def patched_consume_measurement(self, measurement) -> None:
    """Patched method for consume_measurement"""

    if self._view._attribute_keys is not None:
        attributes = {}
        for key, value in (measurement.attributes or {}).items():
            if key in self._view._attribute_keys:
                attributes[key] = value
        if self._view._attribute_values is not None:
            if not (list((attributes or {}).values())
                    in self._view._attribute_values):
                attributes = {}
    elif self._view._attribute_values is not None:
        attributes = {}
        if (list((measurement.attributes or {}).values())
                in self._view._attribute_values):
            attributes = measurement.attributes
    elif measurement.attributes is not None:
        attributes = measurement.attributes
    else:
        attributes = {}

    if (self._view._attribute_values is not None
            or self._view._use_values_as_keys):
        aggr_key = frozenset(attributes.values())
    else:
        aggr_key = frozenset(attributes.items())

    if aggr_key not in self._attributes_aggregation:
        with self._lock:
            if aggr_key not in self._attributes_aggregation:
                if not isinstance(self._view._aggregation, DefaultAggregation):
                    aggregation = self._view._aggregation._create_aggregation(
                        self._instrument,
                        attributes,
                        self._start_time_unix_nano,
                    )
                else:
                    aggregation = self._instrument_class_aggregation[
                        self._instrument.__class__
                    ]._create_aggregation(
                        self._instrument,
                        attributes,
                        self._start_time_unix_nano,
                    )
                self._attributes_aggregation[aggr_key] = aggregation

    self._attributes_aggregation[aggr_key].aggregate(measurement)


@functools.wraps(_ViewInstrumentMatch.collect)
def patched_collect(
        self,
        collection_aggregation_temporality,
        collection_start_nanos):
    """Patched method for collect"""

    data_points = []

    with self._lock:
        if self._view._attribute_values is not None:

            keys_to_keep = get_datapoint_keys_to_keep(self._view)
            data_points = get_data_points_with_cleanup(
                self._attributes_aggregation, self._view,
                collection_aggregation_temporality,
                collection_start_nanos, keys_to_keep)
        else:
            for aggregation in self._attributes_aggregation.values():

                data_point = aggregation.collect(
                    collection_aggregation_temporality,
                    collection_start_nanos
                )
                if data_point is not None:
                    data_points.append(data_point)

    return data_points or None


def get_data_points_with_cleanup(
    attributes_aggregation, view, collection_aggregation_temporality,
        collection_start_nanos, keys_to_keep):
    """Method to use if attribute_values list is not empty
    and want to cleanup datapoints"""

    found_match_list = []
    data_points = []

    for key_set, aggregation in attributes_aggregation.items():
        for att in keys_to_keep:
            if att == key_set:
                found_match_list.append(key_set)
                data_point = aggregation.collect(
                    collection_aggregation_temporality,
                    collection_start_nanos
                )
                if data_point is not None:
                    data_points.append(data_point)

    if view._drop_unused_datapoints is True:
        for a_set in [set(x) for x in attributes_aggregation.keys()]:
            if a_set not in [set(q) for q in found_match_list]:
                attributes_aggregation.pop(frozenset(a_set))
    return data_points


def get_datapoint_keys_to_keep(view):
    """Method to find attributes to keep if any selected"""

    keys = []
    if view._attribute_values is not None:
        for val in view._attribute_values:
            keys.append(frozenset(val))

    return keys


_ViewInstrumentMatch.consume_measurement = patched_consume_measurement  # type: ignore[assignment]
_ViewInstrumentMatch.collect = patched_collect  # type: ignore[assignment]
