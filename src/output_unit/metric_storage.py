"""Module that manages storage for all OTEL instrument points"""
# pylint: disable=W0212
import datetime
import re
import time
from enum import Enum, auto
from threading import Lock, Thread
from typing import Any, Dict, Generator, List, Union

import product_common_logging as logging
from opentelemetry import metrics

from data_processing.models.unified_model import UnifiedModel
from output_unit.meter_point import (OP_TYPE, TRANS_FUNC_MAP, AttributeStorage,
                                     MapStorage, MeterPoint, ObsMetStorage,
                                     StatusConfig, StatusMetricStorage,
                                     StatusStorage, SyncCtStorage)
from output_unit.otel_conf_model import MetricAttribute
from output_unit.otel_sdk_patch import AugmentedView

_oid_pattern = re.compile("<([a-zA-Z0-9]+)!([0-9]+)>", re.IGNORECASE)


def get_snapshot_object_id(oid):
    "Extracts the object id from the data"

    obj_id = ""
    match = _oid_pattern.match(oid)
    if match:
        obj_id = match.group(1)
    else:
        logging.warn('failed to extract object id from snapshot')
    return obj_id


class StateData():
    """Class to hold state data"""

    def __init__(self, values={}, inst_type="Underlay", pcon_state=-1,
                 ir_state=-1, all_ir_state=-1, bgp_state="", kl_state=-1,
                 all_if_kl_state=-1, en_type="categorised", def_ret_data={},
                 th_type="ncat"):
        """Init with default values"""

        self.values = values
        self.inst_type = inst_type
        self.pcon_state = pcon_state
        self.ir_state = ir_state
        self.all_ir_state = all_ir_state
        self.bgp_state = bgp_state
        self.kl_state = kl_state
        self.all_if_kl_state = all_if_kl_state
        self.en_type = en_type
        self.def_ret_data = def_ret_data
        self.th_type = th_type


UNKNOWN_STATE_TIMEOUT = 180


class OTELTypes(int, Enum):
    "OTEL metric types"

    HISTOGRAM = auto()
    OBSERVABLEGAUGE = auto()
    OBSERVABLECOUNTER = auto()
    COUNTER = auto()
    OTHER = auto()


class MapParam(int, Enum):
    "Map Parameter types"

    VALUE = auto()
    TIMESTAMP_ONLY = auto()
    BORN_TIMESTAMP = auto()


class LOCATION(int, Enum):
    "Map Parameter types"

    SEEP = auto()
    SDEP = auto()
    UNKNOWN = auto()


class MetricStore():
    """Class used as storage for gauges, counters that
    are also histograms"""

    lock = Lock()
    value_store: Dict[str, MeterPoint] = {}
    keep_running: bool = True
    cleanup_thread: Thread
    cleanup_thread_timer: float = 5  # 5 seconds
    purge_unused_entries_timer: float = 30  # 30 seconds
    instrument_views: Dict[str, AugmentedView] = {}

    def __new__(cls):
        "New method"
        if not hasattr(cls, 'instance'):
            cls.instance = super(MetricStore, cls).__new__(cls)
            cls.cleanup_thread = Thread(
                target=cls.__purge_unused_data_and_cfg_view, args=())
            cls.cleanup_thread.start()

        return cls.instance

    def clean_store(self):
        "Method to clean up store"
        with self.lock:
            self.value_store.clear()

    def update_attribute_map(
        self, name: str, map_name: str, key: str, value: str,
        update_born_stamp: bool = False, with_lock: bool = False
    ) -> None:
        """
        Example of a meter point:

        {
            "name": {
                "map_name": AttributeMapStore(
                    store_map={
                        "key": MapStorage(
                            timestamp=datetime.datetime(
                                2024, 6, 3, 14, 2, 11, 849925),
                            value=value,
                        )
                    }
                )
            }
        }

        We check first if the key already exists in the store_map. If it does,
        then we insert the new k,v pair, if it's already in there, we just
        update the timestamp so the cleanup thread does not purge the entries.

        :param name: Name of the meter point
        :type name: str
        :param map_name: Name of the map | eg: 'interface_network_to_path_map'
        :type map_name: str
        :param key: The actual key of the dictionary to update the value for
        :type key: str
        :param value: The new value for the specific item
        :type value: str
        """

        if with_lock:
            with self.lock:
                self.__update_map_param(
                    name, map_name, key, value, update_born_stamp)
        else:
            self.__update_map_param(
                name, map_name, key, value, update_born_stamp)

    def __update_map_param(
            self, name: str, map_name: str, key: str, value: str,
            update_born_stamp: bool = False):
        """Method used with update_attribute_map to enforce
        lock or not"""

        try:
            if name in self.value_store:
                meter_point = self.value_store[name]
                if map_name in meter_point.attribute_maps:
                    map_to_change = meter_point.attribute_maps[map_name]
                    if key not in map_to_change.store_map:
                        if isinstance(value, bool):
                            value = str(value)
                        map_to_change.store_map[key] = MapStorage(
                            timestamp=datetime.datetime.utcnow(),
                            born_timestamp=datetime.datetime.utcnow(),
                            value=value)
                        logging.info(
                            "Recorded the following key: %s to value: %s "
                            "for instrument: %s on map: %s",
                            key, value, name, map_name)
                    else:
                        map_to_change.store_map[key].value = value
                        map_to_change.store_map[key].timestamp = (
                            datetime.datetime.utcnow()
                        )
                    if update_born_stamp:
                        map_to_change.store_map[
                            key].born_timestamp = (
                            datetime.datetime.utcnow()
                        )
        except KeyError as err:
            logging.error(
                "Error occured while trying to update values in map: %s",
                err
            )

    def get_param_from_attribute_map(
        self, name: str, map_name: str, key: str,
        param_type: MapParam = MapParam.VALUE,
        reverse_search: bool = False,
        partial_match: bool = False,
        with_lock: bool = True
    ) -> Union[str, int, float, None]:
        """
        Example of a meter point:

        {
            "name": {
                "map_name": AttributeMapStore(
                    store_map={
                        "key": MapStorage(
                            timestamp=datetime.datetime(
                                2024, 6, 3, 14, 2, 11, 849925),
                            value=value,
                        )
                    }
                )
            }
        }

        Returns the value of a particular map stored in 'attribute_maps',
        or None, if the key is not found or the meter point name / map_name
        are not found or mistyped.

        :param name: Name of the meter point
        :type name: str
        :param map_name: Name of the map | eg: 'interface_network_to_path_map'
        :type map_name: str
        :param key: The actual key of the dictionary to get the value from
        :type key: str
        :param reverse_search: If enabled tries to find a substring of the keys
        in the map to the key provided
        :type reverse_search: bool
        :returns : The value for the specific item or `None` if key not present
        :rtype: Union[str, int, float, None, datetime]
        """

        param = None

        if with_lock:
            with self.lock:
                param = self.__get_map_param(
                    name, map_name, key, param_type, reverse_search,
                    partial_match)
        else:
            param = self.__get_map_param(
                name, map_name, key, param_type, reverse_search, partial_match)

        return param

    def __get_map_param(
            self, name: str, map_name: str, key: str,
            param_type: MapParam = MapParam.VALUE,
            reverse_search: bool = False,
            partial_match: bool = False):
        """Method used with get_param_from_attribute_map to enforce
        lock or not"""

        param = None

        try:
            if name in self.value_store:
                meter_point = self.value_store[name]
                if map_name in meter_point.attribute_maps:
                    maps = meter_point.attribute_maps[map_name]

                    if reverse_search:
                        rvs_res = [kk for kk in maps.store_map.keys()
                                   if kk in key]
                        if rvs_res:
                            key = rvs_res[0]
                    elif partial_match:
                        part_res = [kk for kk in maps.store_map.keys()
                                    if key in kk]
                        if part_res:
                            key = part_res[0]

                    if key in maps.store_map:
                        if param_type == MapParam.BORN_TIMESTAMP:
                            param = maps.store_map[
                                key].born_timestamp
                        elif param_type == MapParam.TIMESTAMP_ONLY:
                            param = maps.store_map[key].timestamp
                        else:
                            param = maps.store_map[
                                key].value

        except KeyError as ex:
            logging.error(
                "An error occured while trying to access the value"
                " from a store map: %s", ex)

        return param

    def add_metric(self, name, met_point: MeterPoint):
        "Method to add a new metric to the store"
        with self.lock:
            self.value_store[name] = met_point

    def add_views(self, instrument_views: Dict[str, AugmentedView]):
        """Method to add the views for each instrument which we can modify
        to chose which datapoints will be exported and which will be dropped
        in the opentelemetry sdk"""

        with self.lock:
            for k, v in instrument_views.items():
                self.instrument_views[k] = v

    def get_meter_points(self, data: Union[UnifiedModel, None]) -> (
            Generator[MeterPoint, None, None]):
        """Method to iterate through meter points corresponding to data."""
        if data is None:
            return
        if getattr(data, "snapshot", "_sentinel") == "_sentinel":
            return
        for metric in self.value_store.values():
            if metric.obj_id == get_snapshot_object_id(data.snapshot.obj):
                yield metric

    def get_observations(self, metric_name: str, met_type: str):
        """Method to get observations for observable instruments"""

        observations = []
        with self.lock:
            data: Dict[str, ObsMetStorage] = (
                self.value_store[metric_name].obs_store)

            for _, val in data.items():
                if isinstance(val.iter_val, float) or (
                        isinstance(val.iter_val, int)):

                    observations.append(
                        metrics.Observation(val.iter_val, val.attr))
                else:
                    logging.warn(f"{met_type} should be in int"
                                 "or float format")

        return observations

    def set_obs_store_attribute(self, name, k_hash, attr_name, attr_value):
        """Setting an attribute in the store no locking. Make sure the
        lock is applied in the function that is using this"""

        try:
            if name in self.value_store and (
                    k_hash in self.value_store[name].obs_store):
                if getattr(self.value_store[name].obs_store[k_hash],
                           attr_name) is not None:
                    setattr(self.value_store[name].obs_store[k_hash],
                            attr_name, attr_value)
        except (KeyError, AttributeError) as err:
            logging.error(f"{err}")

    def set_metric(self, name, value, attrs: Dict,
                   attributes_set: bool = True,
                   use_default_attributes: bool = False) -> None:
        "Method to set a metric that is also a histogram"

        if not attributes_set and attrs:
            attributes_set, attrs = self.transform_metric_attributes(
                name, attrs)
        elif use_default_attributes:
            if name in self.value_store:
                attrs = self.value_store[name].attributes
        if attributes_set:
            self.__set_attribute_store(name, attrs)
            value = self.__apply_value_trans_if_any(name, value)
            try:
                if "Observable" in self.value_store[name].type:
                    self.__handle_observable_metrics(
                        name, attrs, value)
                else:
                    self.__handle_non_obs_metrics(
                        name, attrs, value)

            except (KeyError, AttributeError) as err:
                logging.error(f"{err}")

    def read_obs_metric(self, name) -> Any:
        """Method to read a value associated to a metric
            and the attributes for it"""

        with self.lock:
            if name in self.value_store:
                return self.value_store[name].obs_store
            else:
                return {}

    def __apply_value_trans_if_any(self, name, value):
        try:
            if self.value_store[name].trans_func != "":
                return TRANS_FUNC_MAP[self.value_store[name].trans_func](value)
        except (Exception) as err:  # do not know what exc functions can raise
            logging.error(f"{err}")
        return value

    def transform_metric_attributes(
            self, name, attrs: Dict[str, Any]):
        """Method that sets the attributes that will be used
        as part of an otel metric by applying a transformation function
        """
        attributes = {}
        attributes_set = True
        try:
            for attr_name, attr_val in (
                    self.value_store[name].attributes.items()):
                if attr_name in attrs:
                    if attr_val.trans_func != "":
                        if attr_val.trans_func in TRANS_FUNC_MAP:
                            attr_val.value = (
                                TRANS_FUNC_MAP[attr_val.trans_func](
                                    attrs[attr_name]))
                    else:
                        attr_val.value = attrs[attr_name]

                    attributes[attr_name] = attr_val
        except (KeyError, TypeError, AttributeError) as err:
            logging.error(f"{err}")
            attributes_set = False

        return attributes_set, attributes

    def __set_attribute_store(self, name, attributes, with_lock: bool = True):
        """Method that sets for each instrument and each attribute value
        hash combination the attribute_keys and attribute_values recorded.
        These are then used in the cleanup thread whenever a purge occurs and
        an entry expires to remove the particular unused attribute combinations
        We then use the remaining unexpired combinations keys and/or values to
        tell the otel sdk to export only the datatpoints with the an unexpired
        particular value or key"""
        try:
            k_hash = self.__calculate_hash(attributes)
            if with_lock:
                with self.lock:
                    self.__configure_att_storage(name, attributes, k_hash)
            else:
                self.__configure_att_storage(name, attributes, k_hash)
        except (KeyError, TypeError, AttributeError) as err:
            logging.error(f"{err}")

    def __configure_att_storage(self, name, attributes, k_hash):
        if k_hash not in self.value_store[name].attribute_store:
            self.value_store[name].attribute_store[
                k_hash] = AttributeStorage()
            self.value_store[name].attribute_store[
                k_hash].set_attr_keys(attributes)
            self.value_store[name].attribute_store[
                k_hash].set_attr_values(attributes)
            self.value_store[name].attributes_changed = True
        self.value_store[name].attribute_store[
            k_hash].timestamp = datetime.datetime.utcnow()

    def __handle_non_obs_metrics(
            self, name, attrs, value):
        """Function that handles non observalbe metrics without a
        lock """
        try:

            if OTELTypes.HISTOGRAM == (
                    OTELTypes[self.value_store[name].type.upper()]):

                self.value_store[name].value.record(
                    value, {x: y.value for x, y in (attrs.items())})

            if OTELTypes.COUNTER == (
                    OTELTypes[self.value_store[name].type.upper()]):
                diff = self.__get_non_obs_counter_diff(
                    name, value, attrs)
                if diff > 0:
                    self.value_store[name].value.add(
                        diff, {x: y.value for x, y in (attrs.items())})

        except (KeyError, TypeError, AttributeError) as err:
            logging.error(f"{err}")

    def __handle_observable_metrics(
            self, name, attrs, value):
        """Function that handles observalbe counters or gauges with a
        lock """

        self.__set_obs_store_values(name, value, attrs)

    def __get_non_obs_counter_diff(self, name, value, attr):
        """Method used to calculate the diff to be added
        to a non observable synchronous counter"""

        diff = 0
        try:
            if self.value_store[name].is_in_aggr_format:
                k_hash = self.__calculate_hash(attr)
                with self.lock:
                    if k_hash in self.value_store[name].sync_ct_store:
                        if self.value_store[
                                name].sync_ct_store[k_hash].can_record:
                            diff = self.__set_ct_and_ret_diff(
                                k_hash, name, value)
                    else:
                        self.__init_ct_and_ret_diff(k_hash, name, value)
            else:
                diff = value
        except (KeyError, AttributeError) as err:
            logging.error(f"{err}")

        return diff

    def __set_ct_and_ret_diff(self, k_hash, name, value):
        """Sets a counter entry in the store"""
        diff = 0
        try:
            diff = (value -
                    self.value_store[name].sync_ct_store[k_hash].val)
            self.value_store[name].sync_ct_store[k_hash].val = value
            self.value_store[name].sync_ct_store[k_hash].timestamp = (
                datetime.datetime.utcnow())

        except (KeyError, TypeError) as err:
            logging.error(f"{err}")

        return diff

    def __init_ct_and_ret_diff(self, k_hash, name, value):
        """Initialises a counter entry in the store"""

        diff = 0
        try:
            self.value_store[name].sync_ct_store[k_hash] = (
                    SyncCtStorage())
            self.value_store[name].sync_ct_store[k_hash].timestamp = (
                datetime.datetime.utcnow())
        except (KeyError) as err:
            logging.error(f"{err}")

        return diff

    def __set_obs_store_values(self, name, value, attr):
        """Method to handle the case where a received value is already
        in counter format for a counter in the asynchronouse case"""

        try:
            with self.lock:
                is_status = self.value_store[
                    name].status_config != StatusConfig()
                if is_status:
                    met_type = self.value_store[
                        name].status_config.status_inst_type
                    if_type = (self.value_store[
                        name].status_config.status_if_type)
                    loc = (LOCATION.SEEP if self.value_store[
                        name].status_config.prim_ctx_data else LOCATION.SDEP)
                    sdp_id, k_hashes = self.__update_status_stores(
                        name, value, met_type, loc)
                    self.__process_status_metric_val(
                        name, value, met_type, if_type, k_hashes, loc, sdp_id)
                else:
                    key_hash = self.__init_obs_store_entry(
                        name, attr)
                    self.__set_obs_store_metric(name, key_hash, value)

        except (KeyError, AttributeError) as err:
            logging.error(f"{err}")

    @classmethod
    def __purge_unused_data_and_cfg_view(cls):
        """Clean up ct sync store or obs or att store from entries
        that have expired and configure the view of a metric for
        data to be purged from OTEL"""

        while cls.keep_running:
            cls.__clean_stores_apply_view()
            time.sleep(cls.cleanup_thread_timer)

    @classmethod
    def __clean_stores_apply_view(cls):
        with cls.lock:
            for m_point in cls.value_store.values():
                try:
                    if (OTELTypes[m_point.type.upper()] ==
                            OTELTypes.OBSERVABLECOUNTER
                            or OTELTypes[m_point.type.upper()] ==
                            OTELTypes.OBSERVABLEGAUGE):
                        cls.__cleanup_store(m_point.obs_store)
                    if OTELTypes.COUNTER == OTELTypes[m_point.type.upper()]:
                        cls.__cleanup_store(m_point.sync_ct_store)

                    k_hashes = cls.__cleanup_store(m_point.attribute_store)
                    cls.__configure_data_point_view(
                        k_hashes, m_point.name, m_point)
                    for map_storage in m_point.attribute_maps.values():
                        if map_storage.can_purge:
                            cls.__cleanup_store(map_storage.store_map)
                    cls.__cleanup_store(m_point.status_stores)
                except (AttributeError, KeyError) as err:
                    logging.error(f"{err}")

    @classmethod
    def __configure_data_point_view(
            cls, k_hashes: List, name, met_point: MeterPoint):
        try:
            if (k_hashes or met_point.attributes_changed) and (
                    met_point.purge_unused_attributes):
                if k_hashes:
                    logging.debug(f"*** PURGING FROM: {name} ATTRIBUTE HASHES:"
                                  f" {k_hashes} ***")
                if name in cls.instrument_views:
                    hash_keys = cls.__configure_view(name, met_point)
                    met_point.attributes_changed = False
                    cls.__set_record_state(hash_keys, met_point)

        except (AttributeError, KeyError) as err:
            logging.error(f"{err}")

    @classmethod
    def __configure_view(cls, name, met_point: MeterPoint):
        """Method that configures the view of an instrument"""

        attr_values = []
        hash_keys = []
        attr_keys = []
        enable_key_aggregation = False

        for key, val in met_point.attribute_store.items():
            attr_values.append(list(val.attribute_values))
            hash_keys.append(key)
            enable_key_aggregation = val.enable_key_aggregation
            attr_keys = val.attribute_keys
        cls.instrument_views[
            name]._attribute_values = attr_values
        cls.instrument_views[name]._drop_unused_datapoints = (
            met_point.purge_unused_attributes)
        if enable_key_aggregation:
            cls.instrument_views[
                name]._attribute_keys = set(attr_keys)
        return hash_keys

    @classmethod
    def __set_record_state(
            cls, hash_keys: List, met_point: MeterPoint):
        """Method that sets the record state of a counter
        to be able to start adding to it"""

        if OTELTypes.COUNTER == OTELTypes[met_point.type.upper()]:
            if met_point.is_in_aggr_format:
                for key in hash_keys:
                    if key in met_point.sync_ct_store:
                        met_point.sync_ct_store[key].can_record = True

    @classmethod
    def __cleanup_store(
        cls,
        store: Dict[
            str, Union[SyncCtStorage, ObsMetStorage,
                       AttributeStorage, MapStorage, StatusStorage]
        ],
    ):
        """Cleanup store from expired entries"""

        k_hash_to_pop: List[str] = []

        if store:
            k_hash_to_pop = cls.__mark_entries_for_retrieval(store)
            cls.__pop_store_entries(k_hash_to_pop, store)
        return k_hash_to_pop

    @classmethod
    def __mark_entries_for_retrieval(
        cls,
        store: Dict[
            str, Union[SyncCtStorage, ObsMetStorage,
                       AttributeStorage, MapStorage, StatusStorage]
        ],
    ):
        """Method to find entries in a store that timed out"""

        k_hash_to_pop: List[str] = []
        for k_hash, val in store.items():
            try:
                if ((datetime.datetime.utcnow() - val.timestamp) >
                        datetime.timedelta(
                            seconds=cls.purge_unused_entries_timer)):
                    logging.debug(f"hash entry found: {k_hash}")
                    k_hash_to_pop.append(k_hash)
            except (TypeError, AttributeError) as err:
                logging.error(f"{err}")
        return k_hash_to_pop

    @classmethod
    def __pop_store_entries(
        cls,
        k_hash_to_pop,
        store: Dict[
            str, Union[SyncCtStorage, ObsMetStorage,
                       AttributeStorage, MapStorage, StatusStorage]
        ],
    ):
        """Method to pop entries from a store"""

        for hsh in k_hash_to_pop:
            try:
                logging.debug(f"Cleaning up hash entry: {hsh} from store")
                store.pop(hsh)
            except (KeyError, AttributeError) as err:
                logging.error(f"{err}")

    def __init_obs_store_entry(self, name, attr):
        """Intialise an obs store entry based on a key that
        is the hash of its attribute values"""
        key_hash = self.__calculate_hash(attr)
        try:
            if key_hash not in self.value_store[name].obs_store:
                self.value_store[name].obs_store[key_hash] = ObsMetStorage()
            self.value_store[name].obs_store[key_hash].set_attributes(attr)
        except (KeyError, AttributeError) as err:
            logging.error(f"{err}")

        return key_hash

    def set_status_metric_data(
            self, ctx_data: Dict, ir_data: Dict, is_sdep: bool):
        """Method that sets the maps in a status metric to
        be used to populate the attributes such as sdp ids
        that are not known in advance and also the ir status
        for underlays. It also sets the prim ctx id in status
        config

        ctx_data(dict): ctx_id to sdp_id data retrieved over SNAPI
        ir_status(dict): ir id  to status data retrieved over SNAPI
        """

        with self.lock:
            for entry in self.value_store.values():
                stat_cfg = self.value_store[
                    entry.name].status_config
                met_type = stat_cfg.status_inst_type
                if met_type != "":
                    if ctx_data:
                        for ctx_id, val in ctx_data.items():
                            self.update_attribute_map(
                                entry.name, "ctx_map", ctx_id, val["sdp"],
                                with_lock=False)
                            if not is_sdep:
                                if met_type == "Overlay":
                                    if stat_cfg.vrf_id in val["tenants"]:
                                        stat_cfg.prim_ctx_data = {
                                            "ctx": ctx_id,
                                            "sdp": val["sdp"]}
                                else:
                                    if val["prim_ctx"]:
                                        stat_cfg.prim_ctx_data = {
                                            "ctx": ctx_id,
                                            "sdp": val["sdp"]}
                    if met_type == "Underlay":
                        if ir_data:
                            for ir, status in ir_data.items():
                                self.update_attribute_map(
                                    entry.name, "ir_map", ir, status,
                                    with_lock=False)

    def set_can_calculate_status(self, t_status: Dict):
        """This method using tenancy data acquired over snapi
        sets the can calculate state of a store for an overlay
        instrument depending on whether all the checks for tenancy
        have passed or not. Normally this should be done for a
        store with a specific sdp id but since we cannot relate
        a site id to an sdp id at the moment we set the can
        calculate status for all stores. This is ok for a SEEP
        but not ok for an SDEP"""

        with self.lock:
            for entry in self.value_store.values():
                if entry.status_config != StatusConfig():
                    inst_type = entry.status_config.status_inst_type
                    if inst_type == "Overlay":
                        for vrf_id in t_status.keys():
                            if vrf_id == entry.status_config.vrf_id:
                                for strg in entry.status_stores.values():
                                    strg.status_store.can_calculate = (
                                        True if t_status[vrf_id] == 1
                                        else False)

    def set_status_based_on_netcat_updates(
            self, is_sdep: bool, nc_data, excluded_ifs: List = [],
            excluded_vrfs: List = []):
        """This method is used to process the bgp state and ping state data
        received by querying netcat in order to set underlay and overlay
        status"""

        if (not is_sdep and nc_data and "klive_loop_no" in nc_data
                and nc_data["klive_loop_no"] > 0 and
                "system_name" in nc_data and nc_data["system_name"]):
            bgp_state: Dict = nc_data["bgp_state"]
            kl_state: Dict = nc_data["dig_results"]
            self.__process_kl_state(kl_state, excluded_ifs)
            sdp_id: str = nc_data["system_name"]
            all_if_kl_state = self.__have_all_if_klives_failed(kl_state)
            with self.lock:
                for entry in self.value_store.values():
                    if entry.status_config != StatusConfig():
                        inst_type = entry.status_config.status_inst_type
                        if inst_type == "Overlay":
                            for vrf_id, _ in bgp_state.items():
                                if vrf_id == entry.status_config.vrf_id:
                                    bgp_excluded = (
                                        True if vrf_id in excluded_vrfs else
                                        False)
                                    self.__set_status_based_on_ncat_data(
                                        entry, sdp_id, inst_type=inst_type,
                                        bgp_state=bgp_state[vrf_id],
                                        all_if_kl_state=all_if_kl_state,
                                        bgp_excluded=bgp_excluded)
                        elif inst_type == "Underlay":
                            for und_id, kl_data in kl_state.items():
                                kl_excluded = (True if kl_data[
                                    "fqdn"] == "EXCLUDED" else False)
                                if_id = und_id.replace(".", "_")                             
                                if if_id in entry.status_config.if_id:
                                    self.__set_status_based_on_ncat_data(
                                        entry, sdp_id, inst_type=inst_type,
                                        kl_state=kl_data["res"],
                                        kl_excluded=kl_excluded)
                    else:
                        if "bgp_status" in entry.name:
                            name = entry.name
                            for vrf_id, _ in bgp_state.items():
                                if vrf_id == self.get_param_from_attribute_map(
                                        name, "v_map", "id", with_lock=False):
                                    self.__set_bgp_status_based_on_ncat_data(
                                        name, bgp_state[vrf_id])

    def __process_kl_state(self, kl_state: Dict, excluded_ifs: List):
        """This function changes the kl state of an interface if its in
        the exclusion list"""
        if excluded_ifs:
            for exc_if in excluded_ifs:
                if exc_if in kl_state.keys():
                    kl_state[exc_if]["fqdn"] = "EXCLUDED"
                    kl_state[exc_if]["dns_server"] = "EXCLUDED"
                    kl_state[exc_if]["res"] = 1

    def __have_all_if_klives_failed(self, kl_state: Dict):
        """This function determines if all keep alives failed on all underlays
        kl_state (Dict): a dictionnary with the ping results of each underlay

        Returns:
            status: 0 if unknown, 2 if keep alives are passing on at least one
            underlay, 1 if keep alives are not passing on any underlay
        """

        if kl_state:
            try:
                state_only = [v["res"] for k, v in kl_state.items()]
                return self.__get_state(state_only)
            except (KeyError, AttributeError) as kl_exc:
                logging.error(f"{kl_exc}")
        return 0

    def __are_all_irs_disabled(self, ir_status: Dict):
        """This function determines if all irs disabled
        ir_status (Dict): a dictionnary with ir status per ir. 1 for
        enabled ir 0 for disabled

        Returns:
            status: 0 if unknown, 2 if at least one ir is enabled,
            1 if all irs are disabled
        """

        if ir_status:
            try:
                state_only = [v for k, v in ir_status.items()]
                return self.__get_state(state_only)
            except AttributeError as attr_exc:
                logging.error(f"{attr_exc}")
        return 0

    def __get_state(self, state):
        if state:
            if (state[0] == 0 and
                    [state[0]]*len(state) == state):
                return 1
            else:
                return 2
        return 0

    def set_status_if_not_receiving_status_updates(
            self, t_status: Dict, ir_status: Dict, is_sdep: bool, sdp_id: str):
        """This method is used when not receiving log updates for the status
        instruments. This call will set status to not functional.
        This method is called by the main thread when it
        receiveds a reply back from SNAPI

        t_status (Dict): the tenancy status data (vrf_id and whether
        the tenant passed tenancy tests depending whether on seep or sdep)

        ir_status (str): the ir data i.e the ir id and whether it is active
        is_sdep (bool): whether this is executed on seep or sdep
        Returns:
            None: No returns
        """

        if not is_sdep:
            all_ir_state = self.__are_all_irs_disabled(ir_status)
            with self.lock:
                for entry in self.value_store.values():
                    if entry.status_config != StatusConfig():
                        inst_type = entry.status_config.status_inst_type
                        if inst_type == "Overlay":
                            for vrf_id, _ in t_status.items():
                                if vrf_id == entry.status_config.vrf_id:
                                    self.__set_status_timeout_th(
                                        entry, sdp_id, inst_type="Overlay",
                                        all_ir_state=all_ir_state)
                        elif inst_type == "Underlay":
                            for ir_id, ir_state in ir_status.items():
                                if ir_id == entry.status_config.if_id:
                                    self.__set_status_timeout_th(
                                        entry, sdp_id, ir_state=ir_state)

    def __set_bgp_status_based_on_ncat_data(self, name, bgp_state):
        """Method used to set the bgp status instruments

        Args:
            name (str): Name of the instrument to set
            bgp_state (int): Unknown (0) or a value between 1 and 6 for th
            various bgp states
        """
        attr = {
            "attributes": {
                "dp1": {"type": MetricAttribute(value="bgp_state")},
                "dp2": {"type": MetricAttribute(value="bgp_state_timer")},
            }
        }
        for k, _ in attr["attributes"].items():
            self.__init_obs_store_entry(
                name, attr["attributes"][k])
            self.__set_attribute_store(
                name, attr["attributes"][k], with_lock=False)
        obs_store = self.value_store[name].obs_store
        attr_store = self.value_store[name].attribute_store
        obs_entries = TRANS_FUNC_MAP["get_bgp_state_data"](
            obs_store, bgp_state)

        for k_hash, values in obs_entries.items():
            self.__set_obs_store_metric(
                name, k_hash, values["to_set"])

        for k_hash, _ in attr_store.items():
            attr_store[k_hash].timestamp = datetime.datetime.utcnow()

    def __set_status_based_on_ncat_data(
            self, entry: MeterPoint, sdp_id: str, peer_con_state: int = -1,
            bgp_state: str = "", kl_state: int = -1, inst_type="Underlay",
            all_if_kl_state: int = -1, kl_excluded: bool = False,
            bgp_excluded: bool = False):
        """This method is used to set the status of the raw/categorised
        datapoints of an overlay/underlay to unknown state.
        This happens on seep only. On SDEP we let them expire and
        be garbage collected.

        entry (MeterPoint): This is the meterpoint entry data that
        represents the overlay/underlay instrument
        sdp_id (str): this is the key for the underlay/overlay instrument
        store
        peer_con_state (int): value of peer connection state or default
        kl_state (int): value of keep alive state or default
        bgp_state (int): value of bgp connection state  or default
        all_if_kl_state (int): state of whether all klives failed or not
        or unknown
        inst_type (str): the type of instrument Overlay/Underlay

        Returns:
            None: No returns
        """

        ctx_data = self.value_store[entry.name].status_config.prim_ctx_data
        if ctx_data and ctx_data["sdp"]:
            store_key = ctx_data["sdp"]
        elif sdp_id:
            store_key = sdp_id
        else:
            store_key = ""

        self.__init_status_metric_if_required(entry, store_key)

        if store_key and store_key in self.value_store[
                entry.name].status_stores:

            storage = self.value_store[entry.name].status_stores[
                store_key]
            obs_store = self.value_store[entry.name].obs_store
            attr_store = self.value_store[entry.name].attribute_store

            storage.status_store.bgp_excluded_state = bgp_excluded
            storage.status_store.handle_bgp_state(bgp_state, OP_TYPE.UPDATE)
            storage.status_store.handle_pc_state(
                peer_con_state, OP_TYPE.UPDATE)
            storage.status_store.if_kl_excluded_state = kl_excluded
            storage.status_store.handle_kl_state(kl_state, OP_TYPE.UPDATE)
            storage.status_store.handle_all_if_kl_state(
                all_if_kl_state, OP_TYPE.UPDATE)
            s_data = StateData(inst_type=inst_type, th_type="ncat")
            obs_entries = self.__get_store_entries(
                obs_store, storage.status_store, s_data)
            # self.__print_obs_entries("NCAT", entry.name, obs_entries)

            for k_hash, values in obs_entries.items():
                self.__set_obs_store_metric(
                    entry.name, k_hash, values["to_set"])
                storage.timestamp = datetime.datetime.utcnow()

            for k_hash, _ in attr_store.items():
                attr_store[k_hash].timestamp = datetime.datetime.utcnow()

    def __print_obs_entries(self, print_type, name, obs_entries):
        print(f"---------------------{print_type}--------------------------\n"
              f"entry_name={name}\n"
              f"obs_entries={obs_entries}\n"
              "---------------------------------------------------\n")

    def __set_status_timeout_th(
            self, entry: MeterPoint, sdp_id: str, ir_state: int = -1,
            all_ir_state: int = -1, inst_type: str = "Underlay"):
        """This method is used to set the status of the raw/categorised
        datapoints of an overlay/underlay to unknown state.
        This happens on seep only. On SDEP we let them expire and
        be garbage collected.

        entry (MeterPoint): This is the meterpoint entry data that
        represents the overlay/underlay instrument

        Returns:
            None: No returns
        """

        ctx_data = self.value_store[entry.name].status_config.prim_ctx_data
        if ctx_data and ctx_data["sdp"]:
            store_key = ctx_data["sdp"]
        elif sdp_id:
            store_key = sdp_id
        else:
            store_key = ""

        self.__init_status_metric_if_required(entry, store_key)
        if store_key and store_key in self.value_store[
                entry.name].status_stores:

            storage = self.value_store[entry.name].status_stores[
                store_key]
            obs_store = self.value_store[entry.name].obs_store
            attr_store = self.value_store[entry.name].attribute_store

            storage.status_store.handle_ir_state(ir_state, OP_TYPE.UPDATE)
            storage.status_store.handle_all_ir_disabled_state(
                all_ir_state, OP_TYPE.UPDATE)
            s_data = StateData(inst_type=inst_type, th_type="tout")
            obs_entries = self.__get_store_entries(
                obs_store, storage.status_store, s_data)
            # self.__print_obs_entries("TOUT", entry.name, obs_entries)
            for k_hash, values in obs_entries.items():
                if ((datetime.datetime.utcnow() - obs_store[k_hash].timestamp)
                        > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT)):
                    self.__set_obs_store_metric(
                        entry.name, k_hash, values["to_set"])
                    storage.timestamp = datetime.datetime.utcnow()

            for k_hash, _ in attr_store.items():
                if ((datetime.datetime.utcnow() - attr_store[k_hash].timestamp)
                        > datetime.timedelta(seconds=UNKNOWN_STATE_TIMEOUT)):
                    attr_store[k_hash].timestamp = datetime.datetime.utcnow()

    def __print_set_to_nf_message(self, entry: MeterPoint):
        """Used for debugging"""

        print("----------------------------------\n"
              f"Setting {entry.name} to Unknown state at "
              f"{datetime.datetime.utcnow()}\n"
              "----------------------------------\n")

    def __get_store_entries(
            self, obs_store: Dict[str, ObsMetStorage],
            s_store: StatusMetricStorage,  s_data: StateData):
        """This method looks into an obs store and returns them in a format
        that is suitable for setting the datapoint

        obs_store (Dict): This is an obs store
        s_store (StatusMetricStorage): This is a status store for an instrument
        inst_type (str): type of instrument Underlay|Overlay
        th_type (str): type of the thread using this function timeout or ncat
        pcon_state (int): value of peer connection state
        kl_state (int): value of keep alive state
        bgp_state (int): value of bgp connection state
        ir_state (int): value of ir state

        Returns:
            store (Dict): Returns obs entries in a format suitable for setting
            the datapoints. This is seep only.
        """

        entries = {}
        for k, v in obs_store.items():
            if "interface_enabled" in v.attr.values():
                entries[k] = {"en_type": "interface_enabled",
                              "to_set": s_store.handle_ir_state(
                                  s_data.ir_state)}
            elif "peer_connectivity" in v.attr.values():
                entries[k] = {"en_type": "peer_connectivity",
                              "to_set": s_store.handle_pc_state(
                                  s_data.pcon_state)}
            elif "keep_alive" in v.attr.values():
                entries[k] = {"en_type": "keep_alive",
                              "to_set": s_store.handle_kl_state(
                                  s_data.kl_state)}

        for k, v in obs_store.items():
            if "raw" in v.attr.values():
                s_data.values = {"NF": 0.002, "UN": 0.0, "DEG": 0.5}
                s_data.en_type = "raw"
                s_data.def_ret_data = (
                    {} if s_data.th_type == "ncat" else
                    {"en_type": s_data.en_type, "to_set": s_data.values["UN"]})
                if entry := self.__get_state_entry(s_store, s_data):
                    entries[k] = entry
            elif "categorised" in v.attr.values():
                s_data.values = {"NF": 1, "UN": 0, "DEG": 2}
                s_data.en_type = "categorised"
                s_data.def_ret_data = (
                    {} if s_data.th_type == "ncat" else
                    {"en_type": s_data.en_type, "to_set": s_data.values["UN"]})
                if entry := self.__get_state_entry(s_store, s_data):
                    entries[k] = entry
        return entries

    def __get_state_entry(
            self, status_store: StatusMetricStorage, s_data: StateData):
        """Set the state entry accordingly based on conditions for the netcat
        thread"""

        if s_data.inst_type == "Underlay":
            if (status_store.handle_pc_state(s_data.pcon_state) == 1
                    or status_store.handle_ir_state(s_data.ir_state) == 1 or
                    status_store.handle_kl_state(s_data.kl_state) == 1):
                return {"en_type": s_data.en_type,
                        "to_set": s_data.values["NF"]}
            elif status_store.handle_kl_state(s_data.kl_state) == 2:
                return {"en_type": s_data.en_type,
                        "to_set": s_data.values["DEG"]}
        else:
            b_state = status_store.handle_bgp_state(s_data.bgp_state)
            a_k_state = status_store.handle_all_if_kl_state(
                s_data.all_if_kl_state)
            a_i_state = status_store.handle_all_ir_disabled_state(
                s_data.all_ir_state)

            if (b_state == 1 or a_k_state == 1 or a_i_state == 1):
                return {"en_type": s_data.en_type,
                        "to_set": s_data.values["NF"]}

        return s_data.def_ret_data

    def __init_status_metric_if_required(
            self, entry: MeterPoint, store_key):
        """Method used to initialise a status metric in case
        we are not receiving logs in seep"""

        met_type = self.value_store[
            entry.name].status_config.status_inst_type
        if met_type == "Overlay":
            value = {
                "attributes": {
                    "dp1": {"sampling_method": MetricAttribute(value="raw")},
                    "dp2": {"sampling_method": MetricAttribute(
                        value="categorised")},
                }
            }
        else:
            value = {
                "attributes": {
                    "dp1": {"status": MetricAttribute(
                        value="interface_enabled")},
                    "dp2": {"status": MetricAttribute(
                        value="peer_connectivity")},
                    "dp3": {"status": MetricAttribute(value="keep_alive")},
                    "dp4": {"sampling_method": MetricAttribute(value="raw")},
                    "dp5": {"sampling_method": MetricAttribute(
                        value="categorised")},
                }
            }

        location = LOCATION.SEEP
        self.__set_status_storage_for_status_metric(
            entry.name, value, met_type, location, store_key)
        for k, _ in value["attributes"].items():
            self.__init_obs_store_entry(
                entry.name, value["attributes"][k])
            self.__set_attribute_store(
                entry.name, value["attributes"][k], with_lock=False)

    def __update_status_stores(
            self, name: str, value: Dict, met_type: str, location: LOCATION):
        """Initialise or update if required for a status instrument
        the status store entry by sdp_id, the obs store for each combination
        of attributes, and the attribute store for garbage collection

        name (str): The name of the status instrument to use
        value (Dict): This contains in a Dictionary all the required
        information that the path helper function for a status instrument
        has introduced. It contains data such as the values extracted from
        the logs, the attributes required to populate a datapoint under the
        status instrument
        met_type (str): The type of metric Underlay, Overlay, ''
        location (str): If this is executed on seep, or sdep or unknown

        Returns:
            sdp_id, key_hashes (Tuple): If found in the ctx map of the
            status instrument the sdp_id is returned based on the
            the secret ctx id  extracted from the log. Also the
            hashes based on the attributes for the datapoints
            for each status instrument are returned
        """

        key_hashes: List = []
        sdp_id = ""
        try:
            sdp_id = self.get_param_from_attribute_map(
                name, "ctx_map", value["ctx_id"],
                with_lock=False)  # type: ignore
            if location == LOCATION.SEEP:
                if value["ctx_id"] != self.value_store[
                        name].status_config.prim_ctx_data["ctx"]:
                    return sdp_id, key_hashes
            if sdp_id:
                self.__set_status_storage_for_status_metric(
                    name, value, met_type, location, sdp_id)

                for k in value["attributes"].keys():
                    key_hash = self.__init_obs_store_entry(
                        name, value["attributes"][k])
                    key_hashes.append(key_hash)
                    self.__set_attribute_store(
                        name, value["attributes"][k], with_lock=False)
        except (KeyError, AttributeError) as err:
            logging.error(f"{err}")

        return sdp_id, key_hashes

    def __get_status_storage(self, store_setup: StatusMetricStorage):
        """Method that returns a status metric storage object instance to be
        used by each obs store as a status store for instruments that are
        status metrics"""

        return StatusMetricStorage(
            pos_w_adj_factor=store_setup.pos_w_adj_factor,
            neg_w_adj_factor=store_setup.neg_w_adj_factor,
            store_size=store_setup.store_size,
            full_history_required=(
                store_setup.full_history_required),
            confidence_index=store_setup.confidence_index,
            record_interval=store_setup.record_interval,
            rtt_cfg=store_setup.rtt_cfg,
            rtt_store_size=store_setup.rtt_store_size,
            rtt_stop_calc=store_setup.rtt_stop_calc,
            status_limits=store_setup.status_limits
        )

    def __process_status_metric_val(
            self, name, value, met_type: str, if_type: str,
            k_hashes: List, loc: LOCATION, sdp_id):
        """Method used if this is an undelay or overlay status
        metric in order to set the confidence index and get the value
        of the status based on the confidence index at a given iteration"

        Args:
            k_hash (str): the hash key to use to access the obs_store

            name (str): the name of the status insturment

            value (Dict|int|float): If status metric this is a dictionary
            of values used to calculate the status metric. If not a dictionary
            and not a status metric it should be an int or float since it is
            an obs instrument and it should not be touched

            met_type (str): whether this is an overlay or underlay
            metric or neither

            if_type (str): in case of an underlay metric the interface id
            the underlay is implemented upon (i.e. ppp100, Ethernet_ethX.YYYY)

            k_hashes(List): the hashes for the datapoints to use in obs
            store to set the status instrument datapoints in order
        Returns:
            if not a status metric this will return the value untouched.
            If status metric this will returnt the int value representing
            the state of the status metric
        """

        vals = []
        wts = []
        if k_hashes and sdp_id:
            st_key = self.__get_store_key(
                value, met_type, loc, sdp_id)
            if st_key:
                status_store: StatusMetricStorage = self.value_store[
                    name].status_stores[st_key].status_store
                if status_store:
                    if met_type == 'Overlay':
                        vals, wts = self.__perform_olay_status_calculations(
                           value, status_store, met_type, loc)
                    elif met_type == 'Underlay':
                        vals, wts = self.__perform_ulay_status_calculations(
                            value, status_store, if_type, met_type, loc)
                    # self.__print_status(
                    #    name, loc, met_type, value, vals, st_key, wts, if_type)
                    for i, val in enumerate(vals):
                        self.__set_obs_store_metric(name, k_hashes[i], val)

    def __perform_ulay_status_calculations(
            self, value, status_store, if_type, metric_type, location):
        """Helper method to perform all calculatin for an underlay status
        instrument. Returns the weights calculated and the current values
        to set on the datapoints for the status instrument"""
        to_set_vals = []
        weights = []
        if "rtt" in value:
            w1 = self.__calculate_data_weights(
                value, ["sB", "fB"], status_store)
            w2 = self.__calculate_rtt_weight(
                status_store, value["rtt"], if_type)
            if w1 > -100 and w2 > -100:
                peer_state = 0
                kl_state = status_store.handle_kl_state()
                ir_state = status_store.handle_ir_state()
                if "ns" in value:
                    peer_state = status_store.handle_pc_state(value["ns"])
                if kl_state == 1 or peer_state == 1 or ir_state == 1:
                    status_store.set_confidence_index(
                        [-0.998])
                    weights = [-0.998]
                elif kl_state == 2:
                    status_store.set_confidence_index(
                        [-0.5])
                    weights = [-0.5]
                else:
                    status_store.set_confidence_index(
                        [w1, w2])
                    weights = [w1, w2]
                to_set_vals = self.__get_values_to_set(
                    status_store, metric_type, location, kl_status=kl_state,
                    peer_status=peer_state, ir_status=ir_state)
        return to_set_vals, weights

    def __perform_olay_status_calculations(
            self, value, status_store, metric_type, location):
        """Helper method to perform all calculation for an overlay status
        instrument. Returns the weights calculated and the current values
        to set on the datapoints for the status instrument"""

        to_set_vals = []
        weights = []
        if status_store.can_calculate:
            w1 = self.__calculate_data_weights(
                value, ["ud", "ret"], status_store)
            if w1 > -100:
                bgp_state = status_store.handle_bgp_state()
                all_kl_state = status_store.handle_all_if_kl_state()
                all_ir_state = status_store.handle_all_ir_disabled_state()
                if bgp_state == 1 or all_kl_state == 1 or all_ir_state == 1:
                    status_store.set_confidence_index(
                        [-0.998])
                    weights = [-0.998]
                else:
                    status_store.set_confidence_index(
                        [w1])
                    weights = [w1]
                to_set_vals = self.__get_values_to_set(
                    status_store, metric_type, location)
        return to_set_vals, weights

    def __print_status(
            self, name, location, metric_type, value, vals,
            store_key, weights, if_type):
        """Method used to print status data"""

        pos_store = self.value_store[name].status_stores[
            store_key].status_store.pos_w_calc_store
        neg_store = self.value_store[name].status_stores[
            store_key].status_store.neg_w_calc_store
        rtt_store = self.value_store[name].status_stores[
            store_key].status_store.rtt_store
        ir_store = self.value_store[name].status_stores[
            store_key].status_store.ir_store
        kl_store = self.value_store[name].status_stores[
            store_key].status_store.kl_store
        bgp_store = self.value_store[name].status_stores[
            store_key].status_store.bgp_store
        ns_store = self.value_store[name].status_stores[
            store_key].status_store.ns_store
        print("------------STATUS----------------\n"
              f"{name=}\n"
              f"{location=}\n"
              f"{metric_type=}\n"
              f"{value=}\n"
              f"{vals=}\n"
              f"{store_key=}\n"
              f"{weights=}\n"
              f"{pos_store=}\n"
              f"{neg_store=}\n"
              f"{rtt_store=}\n"
              f"{ir_store=}\n"
              f"{kl_store=}\n"
              f"{bgp_store=}\n"
              f"{ns_store=}\n"
              f"{if_type=}\n"
              "---------------------------------\n")

    def __get_values_to_set(
            self, status_store: StatusMetricStorage, metric_type: str,
            location: LOCATION, peer_status: int = 0,
            kl_status: int = 0, ir_status: int = 0):
        """Method that get the values to set for a status intstrument"""

        vals = [status_store.confidence_index, status_store.get_status()]
        if metric_type == "Overlay":
            return vals
        elif metric_type == "Underlay":
            if location == LOCATION.SEEP:
                if ir_status == 1:
                    return [0.002, 1, ir_status, peer_status, kl_status]
                else:
                    vals.append(ir_status)
                    vals.append(peer_status)
                    vals.append(kl_status)
                    return vals
            else:
                return vals
        return []

    def __get_store_key(
            self, value: Dict, met_type: str, location: LOCATION, sdp_id: str):
        """Method that gets the key for the relevant status store
        to use either when creating it or when gettting it"""

        if location == LOCATION.SEEP:
            if sdp_id:
                return sdp_id
        else:
            if value:
                if (met_type == "Overlay" and "ctx_id" in value):
                    return value["ctx_id"]
                elif (met_type == "Underlay" and "rem_id" in value
                        and "ctx_id" in value):
                    return f"{value['ctx_id']}_{value['rem_id']}"
        return ""

    def __calculate_data_weights(self, value, data_keys: List,
                                 status_store: StatusMetricStorage):
        """A method that is used to calculate the weights used in overlay or
        underlay metrics in order to affect the confidence index during an
        iteration

        Args:

            value (Dict): the dictionary of values used to calculate the data
            weights to set the confidence index in the case were the snapi
            query has not set the confidence index to zero (This only applies
            to overlay status)(user_data(ud)/retransmissions(ret),
            success_bytes(sB),failed_bytes(fB))

            data_keys (List): The list of data keys to use and are
            included in the value dictionnary

            status_store (StatusMetricStorage): The status store entry to use
            to set the weights under its status store and thus to set the
            confidence index that will be used to set the status
        """
        if self.__can_set_confidence_index(value, data_keys, status_store):
            status_store.reset_store_values_if_required(
                status_store.pos_w_calc_store,
                status_store.neg_w_calc_store,
                value[data_keys[0]],
                value[data_keys[1]])
            u_w = status_store.cal_w_adj_factor(
                status_store.pos_w_calc_store,
                value[data_keys[0]],
                status_store.pos_w_adj_factor)
            r_w = status_store.cal_w_adj_factor(
                status_store.neg_w_calc_store,
                value[data_keys[1]],
                status_store.neg_w_adj_factor)
            ovr_u_w, ovr_r_w = status_store.calc_ovr_adj_factors(
                status_store.pos_w_calc_store,
                status_store.neg_w_calc_store
            )
            return u_w*ovr_u_w-r_w*ovr_r_w

        return -100

    def __can_set_confidence_index(
            self, value, data_keys, status_store: StatusMetricStorage):
        """Method that does a series of checks to verify if
        confidence index can be set"""

        store = status_store
        if (len(data_keys) == 2 and data_keys[0] in value and
                data_keys[1] in value):
            if ((datetime.datetime.utcnow() - store.was_set) >
                datetime.timedelta(
                    seconds=store.record_interval)):
                if value[data_keys[0]] >= 0 and value[data_keys[1]] >= 0:
                    return True

        return False

    def __calculate_rtt_weight(
            self, status_store: StatusMetricStorage, curr_rtt_val, if_type):
        """Method used to calculate the rtt weight that is going to contribute
        at this iteration in the confidence index for an underlay status metric

        Args:
            status_store (StatusMetricStorage): The status instrument and
            its status store to use to calculate the rtt weight

            if_type (str): The interface name for the underlay to use in order
            to use the right normal, cap, weight values in rtt_cfg to calculate
            rtt weight

            curr_rtt_val (int): The rtt value that is going to be used to
            calculate the rtt weight

        """

        store = status_store
        if ((datetime.datetime.utcnow() - store.was_set) >
                datetime.timedelta(seconds=store.record_interval)):
            weight = store.calculate_rtt_weight(
                if_type, curr_rtt_val)
            return weight
        return -100

    def __set_status_storage_for_status_metric(
            self, name, value: Dict, met_type: str,
            location: LOCATION, sdp_id: str):
        """Method to initialise the  status store of an
        instrument based on its ctx id. It also updates
        timestamp so that the store cannot be reclaimed"""

        store_key = self.__get_store_key(value, met_type, location, sdp_id)
        if store_key:
            if store_key not in self.value_store[name].status_stores.keys():
                status_store = self.__get_status_storage(
                    self.value_store[
                        name].status_config.status_store_setup)
                self.value_store[name].status_stores[
                    store_key] = StatusStorage(status_store=status_store)

            self.value_store[name].status_stores[store_key].timestamp = (
                datetime.datetime.utcnow()
            )

    def __set_obs_store_metric(
            self, name, k_hash, value, update_stamp: bool = True):
        """Set observable metric store values if values arrive in
        aggregated format and thus diff has to be calculated based
        on previous value. For counters we use diff. For gauges we
        use iter value"""

        try:

            self.value_store[name].obs_store[k_hash].iter_val = value
            if update_stamp:
                self.value_store[name].obs_store[k_hash].timestamp = (
                    datetime.datetime.utcnow())

        except (KeyError, TypeError, AttributeError) as err:
            logging.error(f"{err}")

    def __calculate_hash(self, attr):
        "Calculate attribute values hash as the key"
        at_hash = ""

        try:
            for _, y in attr.items():
                at_hash += str(y.value)

            if at_hash == "":
                at_hash = "default"
            at_hash = str(hash(at_hash))
        except (AttributeError) as err:
            logging.error(f"{err}")

        return at_hash
