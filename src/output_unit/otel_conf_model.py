"""Model for reading otel.json configuration"""

from typing import Dict, List, Literal, Union
from pydantic import BaseModel, Field, StrictStr, validator

# pylint: disable=E0213, W0613, R0903
Values = Union[float, int, str]


class StatusConfiguration(BaseModel):
    """Storage used to infer confidence index"""

    store_size: int = Field(default=10)
    pos_w_adj_factor: float = Field(default=2.0)
    neg_w_adj_factor: float = Field(default=2.0)
    full_history_required: bool = Field(default=True)
    confidence_index: float = Field(default=1.0)
    record_interval: int = Field(default=10)
    rtt_cfg: Dict = Field(default={})
    rtt_store_size: int = Field(default=20)
    rtt_stop_calc: int = Field(default=5)
    status_limits: List[float] = Field(default=[0.4, 0.6])
    status_inst_type: Literal['Overlay', 'Underlay', ''] = Field(
        default='')
    status_if_type: str = Field(default='')
    vrf_id: str = Field(default='')
    prim_ctx_data: Dict = Field(default={})
    if_id: str = Field(default='')
    weight_calc_mode: Literal[
        'Linear', 'Exponential', 'Parabolic'] = Field(default='Parabolic')


class MetricAttribute(BaseModel):
    "MetricAttribute Model to support attributes in the otel config"
    target: StrictStr = Field(default="")
    value: Union[int, str] = Field(default="")
    trans_func: StrictStr = Field(default="")
    should_yield: Literal['True', 'False'] = Field(default="True")


class AttributeMaps(BaseModel):
    """AttributeMaps Model to support maps in the otel config"""
    can_purge: bool = Field(default=True)
    store_map: Dict[str, str] = Field(default_factory=dict)


class FuncDefinition(BaseModel):
    """FuncDefinition Model to support helper functions for
    match attributes in otel config"""

    name: StrictStr = Field(default="")
    arguments: List[Values] = Field(default=[])

    @validator('arguments')
    def validate_regex_match(cls, v, values, **kwargs):
        """validation that enforces when regex_match
        target should exist"""

        if v and not values['name']:
            raise ValueError('name cannot be empty with arguments')

        return v


class MatchAttribute(BaseModel):
    """MatchAttribute Model used to support functionality of
    match attributes in the otel config"""

    target: StrictStr = Field(default="")
    regex_match: StrictStr = Field(default="")
    helper_func: FuncDefinition = Field(default=FuncDefinition())
    value_match: StrictStr = Field(default="")

    @validator('regex_match')
    def validate_regex_match(cls, v, values, **kwargs):
        """validation that enforces when regex_match
        target should exist"""

        if v and not values['target']:
            raise ValueError('target cannot be empty')

        return v

    @validator('helper_func')
    def validate_helper_func(cls, v, values, **kwargs):
        """validation that enforces allowable cases
        with helper_func field"""

        if v != FuncDefinition() and not values['target']:
            raise ValueError('target cannot be empty')
        elif v != FuncDefinition() and values['regex_match']:
            raise ValueError('only one match attribute allowed')

        return v

    @validator('value_match')
    def validate_value_match(cls, v, values, **kwargs):
        """validation that enforces allowable cases
        with value_match field"""

        if v and not values['target']:
            raise ValueError('target cannot be empty')
        elif v and (values['regex_match'] or
                    values['helper_func'] != FuncDefinition()):
            raise ValueError('only one matching method allowed')
        elif values['target'] and (
                not values['regex_match'] and
                values['helper_func'] == FuncDefinition() and not v):
            raise ValueError('cannot have target without one of'
                             ' the three matching methods')

        return v


class MetricPath(BaseModel):
    """MetricPath Model for supporting the functionality of
    having multiple paths to retrieve values for and a helper
    function to provide an operation on them"""
    paths: List[StrictStr] = Field(default=[])
    helper_func: FuncDefinition = Field(default=FuncDefinition())

    @validator('helper_func')
    def validate_both_fields_present(cls, v: FuncDefinition, values, **kwargs):
        """validation function that enforces the relationship between paths
        and helper_func"""

        if values['paths']:
            if len(values['paths']) > 1:
                if v == FuncDefinition() or not v.name:
                    raise ValueError('cannot have multiple paths'
                                     ' with empty FuncDefition name')
            elif v.arguments and not v.name:
                raise ValueError('cannot have single path'
                                 ' with empty FuncDefition name'
                                 ' and arguments not empty')
        elif v != FuncDefinition():
            raise ValueError('cannot have empty paths list'
                             ' with no default FuncDefinition')

        return v


class VersionConfig(BaseModel):
    """Configuration for a version instrument"""

    from_v: StrictStr = Field(default="0.0.0")
    to_v: StrictStr = Field(default="999.999.999")

    @validator("from_v")
    def validate_from_version(cls, value):
        """Change default values if empty string is an input"""

        if value == "":
            value = "0.0.0"
        return value

    @validator("to_v")
    def validate_to_version(cls, value):
        """Change default values if empty string is an input"""

        if value == "":
            value = "999.999.999"
        return value

    
class MetricPoint(BaseModel):
    "MetricPoint Model"

    obj_id: StrictStr = Field(default="")
    metric_path: MetricPath = Field(default=MetricPath())
    otel_name: StrictStr
    type:  Literal['Counter',
                   'ObservableGauge',
                   'ObservableCounter',
                   'Histogram']
    description: StrictStr = Field(default="")
    unit: StrictStr
    trans_func: StrictStr = Field(default="")
    attributes: Dict[StrictStr, MetricAttribute] = Field(default={})
    histogram_boundaries: List[int] = Field(default=[])
    is_in_aggr_format: Literal['True', 'False'] = Field(default="True")
    purge_unused_attributes: Literal['True', 'False'] = Field(default="True")
    match_attributes: List[MatchAttribute] = Field(default=[])
    attribute_maps: Dict[str, AttributeMaps] = Field(default_factory=dict)
    status_cfg: StatusConfiguration = Field(default=StatusConfiguration())
    rlf_version: VersionConfig = Field(default=VersionConfig())

    @validator('attributes')
    def validate_attributes(cls, v, values, **kwargs):
        """validation that enforces when attributes exist
        that they are used properly"""

        if v:
            if "Observable" not in values['type']:
                for val in v.values():
                    if val.should_yield.lower() == "false":
                        raise ValueError('should_yield cannot be False with'
                                         'non observable types')

        return v 

class MeterConfig(BaseModel):
    "OTLP Metric Config Model"

    scope: StrictStr = Field(..., alias='scope')
    scope_version: StrictStr = Field(..., alias='scope_version')
    met_points:  List[MetricPoint] = Field(..., alias='met_points')


class OTELConfigModel(BaseModel):
    "OTLP config model"

    obs_met: List[MeterConfig] = Field(..., alias='meters')
