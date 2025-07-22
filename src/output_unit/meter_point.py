"""Model for used in otel metrics to store the information required to handle
the gauges and counters"""

import datetime
import re
import math
from enum import Enum, auto
from inspect import getmembers, isclass, isfunction
from typing import Any, Dict, List, Literal, Union

from controller.common import (
    OTELIngestionModelsNotLoaded,
    OTELTransformationFunctionsNotLoaded,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictStr,
    root_validator,
    validator
)

import output_unit.transforms.otel_trans_func as otel_transforms
from output_unit.otel_conf_model import (
    AttributeMaps,
    FuncDefinition,
    MatchAttribute,
    MetricAttribute,
    MetricPath,
)
import product_common_logging as logging

UNKNOWN_STATE_TIMEOUT = 180


class OP_TYPE(int, Enum):
    "OP_TYPE for status data"

    U_GET = auto()
    UPDATE = auto()


class StatusMetricStorage(BaseModel):
    """Storage used to infer confidence index"""

    can_calculate: bool = Field(default=False)
    store_size: int = Field(default=10, ge=10, le=50)
    pos_w_adj_factor: float = Field(default=2.0, ge=0.1, le=4.0)
    neg_w_adj_factor: float = Field(default=2.0, ge=0.1, le=4.0)
    pos_w_calc_store: List = Field(default=[])
    neg_w_calc_store: List = Field(default=[])
    rtt_store: List = Field(default=[])
    rtt_store_size: int = Field(default=20)
    rtt_stop_calc: int = Field(default=5)
    bgp_store: List = Field(default=[])
    bgp_store_size: int = Field(default=3)
    bgp_store_timestamp: Any = datetime.datetime.utcnow()
    kl_store: List = Field(default=[])
    kl_store_size: int = Field(default=6)
    kl_store_timestamp: Any = datetime.datetime.utcnow()
    ns_store: List = Field(default=[])
    ns_store_size: int = Field(default=8)
    ns_store_timestamp: Any = datetime.datetime.utcnow()
    ir_store: List = Field(default=[])
    ir_store_size: int = Field(default=5)
    ir_store_timestamp: Any = datetime.datetime.utcnow()
    all_if_kl_state: int = 0
    all_ir_disabled_state: int = 0
    if_kl_excluded_state: bool = False
    bgp_excluded_state: bool = False
    full_history_required: bool = Field(default=True)
    confidence_index: float = Field(default=1.0)
    was_set: Any = datetime.datetime.utcnow()
    record_interval: int = Field(default=10, ge=10, le=120)
    status_limits: List[float] = Field(default=[0.001, 0.4, 0.6])
    weight_calc_mode: Literal[
        'Linear', 'Exponential', 'Parabolic'] = Field(default='Parabolic')
    rtt_cfg: Dict = Field(default={
        "gx": {"normal": 800, "cap": 3200, "pos_w_adj_factor": 50,
               "neg_w_adj_factor": 100},
        "lte": {"normal": 100, "cap": 400, "pos_w_adj_factor": 50,
                "neg_w_adj_factor": 100},
        "leo": {"normal": 260, "cap": 1040, "pos_w_adj_factor": 50,
                "neg_w_adj_factor": 100},
        "bgan": {"normal": 1600, "cap": 6400, "pos_w_adj_factor": 50,
                 "neg_w_adj_factor": 100},
        "sdep": {"normal": 0, "cap": 0, "pos_w_adj_factor": 50,
                 "neg_w_adj_factor": 100},
    })

    def get_status(self):
        """Method that returns the status based on confidence index
        and limits set"""

        status = 0

        for lm in self.status_limits:
            if self.confidence_index >= lm:
                status += 1
            else:
                break

        return status

    def set_confidence_index(self, weights: List[float]):
        """This function adjust the confidence index by a weight. If
          the value added exceeds 1 set to 1. If the value added is less
          than 0 then set to 0. Otherwise add.

        Args:
            weight (float): The weight to adjust the confidence index by
        """

        for weight in weights:
            if math.isclose(weight, -1.0, abs_tol=0.0001):
                self.confidence_index = 0.0
            elif math.isclose(weight, -0.5, abs_tol=0.0001):
                self.confidence_index = 0.5
            elif self.confidence_index + weight >= 1:
                self.confidence_index = 1.0
            elif self.confidence_index + weight <= 0:
                self.confidence_index = 0.001
            else:
                self.confidence_index += weight

            self.was_set = datetime.datetime.utcnow()

    def handle_kl_state(
            self, current_kl_state: int = -1, op_type=OP_TYPE.U_GET):
        """Method that can be used to store the keepalive state for the
        underlay. It sets the keepalive state to 0 if we were unable to send
        traffic over the interface and 1 if we can. After the relevant store
        gets set we try to detect 2 conditions. If all keep alive state data
        is zero it returns value (1) to set condition interface traffic
        impacted for the interface. If half of the keepalive state data is
        zero it returns a value of (2) to set condition interface traffic
        impacted for the underlay. If none of the above conditions is present
        it returns a value (3) to show keepalive traffic is normal if the
        interface has not been excluded and (4) if it has. Zero is (0)
        unknown state"""

        if current_kl_state >= 0:
            self.kl_store.append(current_kl_state)
            self.kl_store_timestamp = datetime.datetime.utcnow()
            if len(self.kl_store) > (self.kl_store_size):
                self.kl_store.pop(0)

        if op_type == OP_TYPE.UPDATE:
            return
        elif not self.kl_store or self.if_kl_excluded_state:
            return 0
        elif ((datetime.datetime.utcnow() - self.kl_store_timestamp)
                > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT)):
            return 0
        elif (self.value_has_occured_gte_n_times(
                self.kl_store, 0, self.kl_store_size)):
            return 1
        elif (self.value_has_occured_gte_n_times(
                self.kl_store, 0, int(self.kl_store_size/2))):
            return 2
        else:
            return 3


    def handle_pc_state(
            self, current_ns_value: int = -1, op_type=OP_TYPE.U_GET):
        """Method that can be used to save the ns field in underlay data.
        The value for the ns field can be ranging from 0
        to max int. After the relevant store gets set we try to detect a
        condition where the bearer initialisation failed if all samples of
        the ns field are zero. If the above condition is not present it
        retuns a value (1) to show that peer connectivity is normal"""

        if current_ns_value >= 0:
            self.ns_store.append(current_ns_value)
            self.ns_store_timestamp = datetime.datetime.utcnow()
            if len(self.ns_store) > (self.ns_store_size):
                self.ns_store.pop(0)

        if op_type == OP_TYPE.UPDATE:
            return
        elif not self.ns_store:
            return 0
        elif ((datetime.datetime.utcnow() - self.ns_store_timestamp)
                > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT)):
            return 0
        elif (self.value_has_occured_gte_n_times(
                self.ns_store, 0, self.ns_store_size)):
            return 1
        else:
            return 2

    def handle_ir_state(
            self, current_ir_state: int = -1, op_type=OP_TYPE.U_GET):
        """Store the value of the ir and return a >=0 status value if we
        have received an update recently"""

        if current_ir_state >= 0:
            self.ir_store.append(current_ir_state)
            self.ir_store_timestamp = datetime.datetime.utcnow()
            if len(self.ir_store) > (self.ir_store_size):
                self.ir_store.pop(0)

        if op_type == OP_TYPE.UPDATE:
            return
        elif not self.ir_store:
            return 0
        elif (((datetime.datetime.utcnow() - self.ir_store_timestamp)
                > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT)) or
                not self.ir_store):
            return 0
        elif self.ir_store[len(self.ir_store)-1] == 0:
            return 1
        else:
            return 2

    def handle_bgp_state(
            self, current_bgp_state: str = "", op_type=OP_TYPE.U_GET):
        """Method used to store the bgp state in overlays in the bgp store. if
        state is established stores a value of 1 otherwise 0. This detects a
        condition where if bgp is not in established state for n consecutive
        samples it returns a value that sets the overlay in the appropriate
        state. 0 is used of unknown, 1 is used if connection not established
        , 2 is used for established connection"""

        if not current_bgp_state:
            bgp_state = -1
        elif "established" in current_bgp_state.lower():
            bgp_state = 1
        else:
            bgp_state = 0

        if bgp_state >= 0:
            self.bgp_store.append(bgp_state)
            self.bgp_store_timestamp = datetime.datetime.utcnow()
            if len(self.bgp_store) > (self.bgp_store_size):
                self.bgp_store.pop(0)

        if op_type == OP_TYPE.UPDATE:
            return

        if not self.bgp_excluded_state:
            if not self.bgp_store:
                return 0
            elif (((datetime.datetime.utcnow() - self.bgp_store_timestamp)
                    > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT))
                    or not self.bgp_store):
                return 0
            elif (self.all_list_item_equal(self.bgp_store) and
                    self.bgp_store[0] == 0 and
                    len(self.bgp_store) == self.bgp_store_size):
                return 1
            else:
                return 2
        else:
            return 3

    def handle_all_if_kl_state(
            self, all_if_kl_state: int = -1, op_type=OP_TYPE.U_GET):
        """Method that returns the kl state of all underlay
        interfaces. if all interface cannot sustain klives
        returns 1, 2 if an least one can sustain klives,
        0 for unknown"""

        if all_if_kl_state >= 0:
            self.all_if_kl_state = all_if_kl_state
        if op_type == OP_TYPE.UPDATE:
            return
        else:
            return self.all_if_kl_state

    def handle_all_ir_disabled_state(
            self,  all_ir_disabled_state: int = -1, op_type=OP_TYPE.U_GET):
        """Method that returns the ir state of all underlays.
        if all irs are disabled returns 1, 2 if at least one
        ir is active, 0 for unknown"""

        if all_ir_disabled_state >= 0:
            self.all_ir_disabled_state = all_ir_disabled_state
        if op_type == OP_TYPE.UPDATE:
            return
        else:
            return self.all_ir_disabled_state

    def calculate_rtt_weight(self, if_type: str, current_rtt):
        """Method to calculate the weight contribution of the rtt s field
        under the mbcpdefcc message to the confidence index for underlay
        status

        Args:
            if_type (str): the underlay interface to define the weight for

        Returns:
            float: the weight to add to the confidence index
        """
        weight = 0
        if if_type and if_type in self.rtt_cfg:
            if current_rtt > 0:
                self.rtt_store.append(current_rtt)
                if len(self.rtt_store) > (self.rtt_store_size):
                    self.rtt_store.pop(0)
                    if self.all_list_item_equal(self.rtt_store):
                        return -0.999
                if self.are_the_last_samples_the_same(
                        self.rtt_store, current_rtt, self.rtt_stop_calc):
                    return 0.0  # if last 5 rtt are same do not calc weight
                if if_type == "sdep":
                    return 0.0

                if current_rtt < self.rtt_cfg[if_type]["normal"]:
                    weight = 1 / self.rtt_cfg[if_type]["pos_w_adj_factor"]

                elif (current_rtt >= self.rtt_cfg[if_type]["normal"] and
                      current_rtt < self.rtt_cfg[if_type]["cap"]):

                    weight = -((current_rtt) /
                               (self.rtt_cfg[if_type]["normal"] *
                               self.rtt_cfg[if_type]["neg_w_adj_factor"]))
                else:
                    weight = -(
                        self.rtt_cfg[if_type]["cap"] /
                        (self.rtt_cfg[if_type]["normal"] *
                         self.rtt_cfg[if_type]["neg_w_adj_factor"]))
        return weight

    def all_list_item_equal(self, lst):
        "Method that check if all list items are equal"

        return not lst or [lst[0]]*len(lst) == lst

    def are_the_last_samples_the_same(
            self, lst, value, num_of_samples):
        """Method that check if the last number of samples in the store is
        the same"""

        return [value]*num_of_samples == lst[
            len(lst) - num_of_samples: len(lst)]

    def value_has_occured_gte_n_times(
            self, lst: List, value: int, n: int):
        """Method that checks if the number of occurances of a value is greater
        or equal than n"""

        return lst.count(value) >= n

    def calc_ovr_adj_factors(
            self, store1, store2):
        """Method that calculates the overall data adjustment factors that will
        be used in conjuction with the weight adjustment factor to calculate
        the increments to the confidence state per period

        Args:
            store1 (List): the user_data/success_packets historical samples
            in this iteration

            store2 (List): the retransmissions/failed_packets historical
            samples in this iteration

        Returns:
            Tuple[float, float]: A tuple with the overall adjustment factors
            for each type of counter
        """

        if store1 and store2 and len(store1) == len(store2):
            if not (self.full_history_required and
                    len(store1) < self.store_size):
                diff1 = self.__get_diff(store1)
                diff2 = self.__get_diff(store2)

                sum_val = diff1 + diff2
                if diff1 > 0:
                    if diff2 >= 0:
                        return (diff1/sum_val, diff2/sum_val)
                    else:
                        return (1, 1)
                elif diff2 > 0:
                    if diff1 >= 0:
                        return (diff1/sum_val, diff2/sum_val)
                    else:
                        return (1, 1)
                else:
                    return (1, 1)
            else:
                return (1, 1)

        return (1, 1)

    def __get_diff(self, store):
        """Method used to calculate diff between last value
        and previous value in a store"""

        diff = -1
        if store and len(store) >= 2:
            prev_val = store[len(store) - 2]
            curr_val = store[len(store) - 1]
            if curr_val >= prev_val:
                diff = curr_val - prev_val

        return diff

    def cal_w_adj_factor(
            self, store: List, curr_val: int, adj_factor: float):
        """Method that calculates the weight adjustment factor that will
            be used in conjuction with the overall data adjustment factor to
            calculate the increments to the confidence state per period
        Args:
            store (bool):  user_data or retransmissions or successfull
            packets or failed packets store data to calculate the
            adjustment factor

            curr_val (int):  the value of the current iteration
            to add the appropriate store and to use to calculate the
            adjustment factor

            adj_factor (float):  the adjustment factor used to calculate
            the weight. it tweaks the overall weight value up or down
        Returns:
            float: the weight adjustment factor for either user data or
            retransmitted data or successfull packets data or failed packets
            data
        """

        weight = 0
        store.append(curr_val)
        diffs = self.calculate_diffferences(store)
        try:
            if diffs:
                if not (self.full_history_required and
                        len(store) < self.store_size):
                    diff_sum = sum(diffs)
                    if diff_sum > 0:
                        if curr_val >= store[len(store) - 2]:
                            diff = curr_val - store[len(store) - 2]
                            weight = self.get_weight(
                                diff, diff_sum, adj_factor)
                        else:
                            weight = 0
                    else:
                        weight = 0
                else:
                    weight = 0
            else:
                weight = 0

            if len(store) > self.store_size:
                store.pop(0)
        except OverflowError as ovf_exc:
            logging.error("Overflow exception caught: %s", ovf_exc)
        return weight

    def get_weight(self, diff, diff_sum, adj_factor):
        """Method that returns weight based on calculation mode"""

        if self.weight_calc_mode == 'Linear':
            return 1.0*(diff) / (diff_sum * (adj_factor))
        elif self.weight_calc_mode == 'Parabolic':
            return 1.0*(diff) / (
                diff_sum * ((1+diff/diff_sum)**adj_factor))
        else:
            return 1.0*(diff) / (
                diff_sum * (adj_factor**(1+diff/diff_sum)))

    def reset_store_values_if_required(
            self, store1, store2, curr_val1, curr_val2):
        """If the counters have reset and thus current value is
        less than previous value reset the store history and
        this will restart the process"""

        if store1 and len(store1) >= 1 and store2 and len(store2) >= 1:
            if (curr_val1 < store1[len(store1) - 1] or
                    curr_val2 < store2[len(store2) - 1]):
                logging.info("Resetting counter history because these "
                             "have reset for stores")

                self.pos_w_calc_store = []
                self.neg_w_calc_store = []

                # self.confidence_index = 1

    def calculate_diffferences(self, store):
        """Function that calculates the differences between historical values
        kept in stores. This also handles the case where user_data or
        retransmission counters have reset.

        Args:
            store (List): the user_data or retransmission store
            data to use to calculate the differences

        Returns:
            List: returs a list of the differences between historical values
            kept in a store or an empty list if list size is <= 1
        """
        diffs = []

        for index, val in enumerate(store):
            if index > 0:
                if val >= store[index - 1]:
                    diffs.append(val - store[index-1])
                else:
                    diffs.append(0)

        return diffs


class StatusStorage(BaseModel):
    """This is used to hold the individual StatusMetricStorage
    objects for each sdp_id. This is necessary in SDEPs because
    there needs to be one status store for each sdp_id. We cannot
    have this as part of the ObsMetStorage because status metrics
    have from 2 to 3 datapoints for each sdp_id"""

    status_store: StatusMetricStorage = Field(default=StatusMetricStorage())
    timestamp: Any = datetime.datetime.utcnow()


class ObsMetStorage(BaseModel):
    "Storage for observable counters/gauges"

    status_store: StatusMetricStorage = Field(default=StatusMetricStorage())
    iter_val: Union[float, int] = Field(default=0)
    timestamp: Any = datetime.datetime.utcnow()
    updated_st: Any = datetime.datetime.utcnow()
    attr: Dict[str, Union[int, str]] = Field(default={})

    def set_attributes(self, attributes):
        "Attribute setter method"

        for x, y in attributes.items():
            self.attr[x] = y.value


class SyncCtStorage(BaseModel):
    "Storage for synchronous counters"

    val:  Union[float, int] = Field(default=0)
    can_record: bool = Field(default=False)
    timestamp: Any = datetime.datetime.utcnow()


class AttributeStorage(BaseModel):
    "Storage for synchronous counters"

    enable_key_aggregation: bool = Field(default=False)
    attribute_keys: List[str] = Field(default=[])
    attribute_values: List[Union[int, str]] = Field(default=[])
    timestamp: Any = datetime.datetime.utcnow()
    updated_st: Any = datetime.datetime.utcnow()

    def set_attr_keys(self, attributes):
        "Attribute setter method"

        self.attribute_keys = []
        for k, y in attributes.items():
            if y.should_yield.lower() == "true":
                self.attribute_keys.append(k)
            else:
                self.enable_key_aggregation = True

    def set_attr_values(self, attributes):
        "Attribute setter method"

        self.attribute_values = []
        for _, y in attributes.items():
            if y.should_yield.lower() == "true":
                self.attribute_values.append(y.value)


class MapStorage(BaseModel):
    "Model for the value of each row of the map store"

    timestamp: Any = Field(default=datetime.datetime.utcnow())
    born_timestamp: Any = Field(
        default=datetime.datetime.utcnow())
    value: Union[str, int, float] = Field(
        default="")


class AttributeMapStore(BaseModel):
    "Storage for maps"
    store_map: Dict[str, MapStorage] = Field(default_factory=dict)
    can_purge: bool = Field(default=True)


class StatusConfig(BaseModel):
    """Configuration for a status instrument"""

    status_store_setup: StatusMetricStorage = Field(
        default=StatusMetricStorage())
    status_inst_type: Literal['Overlay', 'Underlay', ''] = Field(default="")
    status_if_type: str = Field(default="")
    vrf_id: str = Field(default="")
    prim_ctx_data: Dict = Field(default={})
    if_id: str = Field(default="")
 
class MeterPoint(BaseModel):
    "MeterPoint Model"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: StrictStr
    value: Any = Field(default=0)
    path:  MetricPath = Field(default=MetricPath())
    type:  Literal['Counter',
                   'ObservableGauge',
                   'ObservableCounter',
                   'Histogram']
    trans_func:  StrictStr = Field(default="")
    attributes:  Dict[StrictStr, MetricAttribute]
    obj_id: StrictStr = Field(default="")
    is_in_aggr_format: bool = Field(default=True)
    # only for synchronous counters
    sync_ct_store: Dict[str, SyncCtStorage] = Field(
        default={})
    # only for asynchronous metrics
    obs_store: Dict[str, ObsMetStorage] = Field(
        default={})
    # for all instruments. This is used to change the AugmentedView of
    # an instrument every time we cleanup
    attribute_store: Dict[str, AttributeStorage] = Field(
        default={})
    purge_unused_attributes: bool = Field(default=True)
    attributes_changed: bool = Field(default=False)
    match_attributes: List[MatchAttribute] = Field(default=[])
    attribute_maps: Dict[str, AttributeMapStore] = Field(default_factory=dict)
    status_config: StatusConfig = Field(default=StatusConfig())
    # the key for a status store is the ctx_id
    status_stores: Dict[str, StatusStorage] = Field(default={})

    @root_validator(pre=True)
    def _set_maps(cls, values):
        """
        Modify the value of 'attribute_maps' before the standard parsing
        is performed

        :param values: Fields of the model
        :type values: Dictionary containing the fields and their respective
        values
        :return: Changed fields(returned as a dictionary)
        :rtype: Dictionary containing the fields and their respective values
        """
        if "attribute_maps" in values:
            values["attribute_maps"] = cls.set_maps(values["attribute_maps"])
        return values

    @staticmethod
    def set_maps(
            attr_maps: Dict[str, AttributeMaps]) -> Dict[
                str, AttributeMapStore]:
        """
        Sets the values in 'attribute_maps'.

        :param attr_maps: Data from the local config.
        :type attr_maps: Dict[str, AttributeMaps]
        :return: Retruns a dictionary containing 'map_name' as key
        and a AttributeMapStore object.
        :rtype: Dict[str, AttributeMapStore]
        """
        attribute_maps: Dict[str, AttributeMapStore] = {}
        store_map = {}
        if not attr_maps:
            return attribute_maps
        for map_key, map_value in attr_maps.items():
            if not map_value.store_map:
                attribute_maps[map_key] = AttributeMapStore()
            else:
                for k, v in map_value.store_map.items():
                    store_map[k] = MapStorage(value=v)

                map_store = AttributeMapStore(
                    can_purge=map_value.can_purge, store_map=store_map
                )

                attribute_maps[map_key] = map_store

        return attribute_maps

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

    def apply_path_helper_func_if_any(self, metric_values: List):
        """Method that applies the helper function on the values
        retrieved with each path target if one is defined
        It relies on pydantic validation enforcing the specific
        validation rules applied to the MetricPath class"""

        metric_value: List[Dict[str, Union[float, int,
                                           Dict[str, Union[float, int, str]]]]]
        try:
            if self.path.helper_func != FuncDefinition():
                if self.path.helper_func.name in TRANS_FUNC_MAP:
                    func_args = self.path.helper_func.arguments.copy()
                    for x in metric_values:
                        func_args.append(x)
                    metric_value = TRANS_FUNC_MAP[
                        self.path.helper_func.name](func_args)
                else:
                    metric_value = [{"val": metric_values[0]}]
            else:  # pydantic validation of path forces single value here
                metric_value = [{"val": metric_values[0]}]
        except Exception:
            metric_value = [{"val": -1}]

        return metric_value

    def check_if_attributes_match(
            self, otel_met_obj, data):
        """Method that checks when the match attributes is not an empty list
        to find if the data in the message is a match for an instrument"""
        is_match = True

        for attr in self.match_attributes:
            if is_match:
                if not attr.target:
                    return False

                target_value = otel_met_obj.rec_getattr(
                    data, attr.target, "None")
                if target_value == "None":
                    return False

                if (attr.value_match and attr.value_match != target_value):
                    return False
                elif attr.regex_match:
                    is_match = self.__handle_ma_regex_case(attr, target_value)
                elif attr.helper_func.name:
                    is_match = self.__handle_ma_hfunc_case(attr, target_value)

        return is_match

    def __handle_ma_regex_case(
            self, mt_attr: MatchAttribute, target_value) -> bool:
        """Method to handle the regex match of a MatchAttribute"""
        try:
            mt = re.compile(mt_attr.regex_match)
            if not mt.match(target_value):
                return False
        except re.error:
            return False
        return True

    def __handle_ma_hfunc_case(
            self, mt_attr: MatchAttribute, target_value) -> bool:
        "Method that handle a MatchAttribute helper function"

        is_match = True
        try:
            # make sure to always return bool
            if mt_attr.helper_func.name in TRANS_FUNC_MAP:
                args = mt_attr.helper_func.arguments.copy()
                args.append(target_value)
                is_match = TRANS_FUNC_MAP[mt_attr.helper_func.name](args)
        except Exception:  # gen exc since trans func unknown
            is_match = False
        if isinstance(is_match, bool):
            return is_match

        return False


def get_trans_functions(modules: List) -> Dict[str, Any]:
    """Return a dict with all the transformation functions.
    Each entry has as key the name of the function and then
    the function reference as value."""

    transforms = {}
    for module in modules:
        trans = {f[0]: f[1] for f in getmembers(
            module, isfunction) if not f[0].startswith("__")}
        transforms.update(trans)
    if not transforms:
        raise OTELTransformationFunctionsNotLoaded
    return transforms


def get_models(modules: List) -> Dict[str, Any]:
    """Return a dict with all the ingestion models root nodes.
    Each entry has as key the name of the root node and then
    the class module. Need root nodes to be ending in LogModel
    to distinguish"""

    models = {}
    for module in modules:
        mod = {f[0]: f[1] for f in getmembers(
            module, isclass) if "LogModel" in f[0]}
        models.update(mod)

    if not models:
        raise OTELIngestionModelsNotLoaded
    return models


TRANS_FUNC_MAP = get_trans_functions([otel_transforms])
