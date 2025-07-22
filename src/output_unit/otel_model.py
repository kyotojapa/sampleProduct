"""Model for OTEL data"""
# pylint: disable=E0611, R0903

from typing import Dict, Optional, Sequence, Union, Mapping
from enum import IntEnum
from pydantic import BaseModel, ConfigDict, StrictStr, Field

AttributeValue = Union[
    StrictStr,
    bool,
    int,
    float,
    Sequence[StrictStr],
    Sequence[bool],
    Sequence[int],
    Sequence[float],
]


class AggregationTemporality(IntEnum):
    """Aggregation Temporality Class"""

    UNSPECIFIED = 0
    DELTA = 1
    CUMULATIVE = 2


class HistogramDataPoint(BaseModel):
    "Histogram Data Point Model"

    attributes:  Optional[Mapping[StrictStr, AttributeValue]]
    start_time_unix_nano: int
    time_unix_nano: int
    count: int
    sum: Union[float, int]
    bucket_counts: Sequence[int]
    explicit_bounds: Sequence[float]
    min: float
    max: float


class NumberDataPoint(BaseModel):
    "Number Data Point Model"

    attributes:  Optional[Dict[StrictStr, AttributeValue]]
    start_time_unix_nano: int
    time_unix_nano: int
    value: Union[float, int]


class Histogram(BaseModel):
    "Histogram Model"

    model_config = ConfigDict(arbitrary_types_allowed=True)
    data_points: Sequence[HistogramDataPoint]
    aggregation_temporality: AggregationTemporality


class Gauge(BaseModel):
    "Gauge Model"

    data_points: Sequence[NumberDataPoint]


class Sum(BaseModel):
    "Sum Model"

    data_points: Sequence[NumberDataPoint]
    aggregation_temporality: AggregationTemporality
    is_monotonic: bool


class BoundedAttributes(BaseModel):
    "Bounded Attributes Model"

    maxlen: Optional[int]
    attributes: Optional[Mapping[StrictStr, AttributeValue]]
    immutable: bool = True
    max_value_len: Optional[int]


class Resource(BaseModel):
    "Resource Model"
    attributes: Optional[Mapping[StrictStr, AttributeValue]]
    schema_url: Optional[StrictStr]


class Metric(BaseModel):
    "Metric Model"

    name: StrictStr
    description: Optional[StrictStr]
    unit: Optional[StrictStr]
    data: Union[Sum, Gauge, Histogram]


class InstrumentationScope(BaseModel):
    "Instrumentation Scope Model"

    name: StrictStr = Field(default="")
    version: Optional[StrictStr]
    schema_url: Optional[StrictStr]


class ScopeMetrics(BaseModel):
    "Scope Metrics Model"

    scope: InstrumentationScope
    metrics: Sequence[Metric]
    schema_url: StrictStr = Field(default="")


class ResourceMetrics(BaseModel):
    "Resource Metrics Model"

    resource: Resource
    scope_metrics: Sequence[ScopeMetrics]
    schema_url: StrictStr


class OTELDataModel(BaseModel):
    "MetricsData Model"

    resource_metrics: Sequence[ResourceMetrics]
