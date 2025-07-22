"""Module that holds the metric data transformation functions.
This functions will be executed on each message when passing from input unit
to output unit.

Transforms are applied in the order they appear in this file - top down.
The output from transform1 forms the input to transform2 etc.

If a message is found to be unsuitable for processing an
ExitTransformLoopException can be thrown.

input_unit ----> apply transform functions ----> output_unit"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import product_common_logging as logging
import requests
from pysnapi.exceptions import PySNAPIException

from config_manager.models import MetricTransformConfig
from data_processing.exit_transform_loop_exception import \
    ExitTransformLoopException
from data_processing.models.unified_model import UnifiedModel
from data_processing.sub_sampler import SubSampler
from output_unit.metric_storage import MetricStore
from output_unit.meter_point import ObsMetStorage

METRICS_IN_PROC_ACCEPTED = 0


def process_snapshot(
        message: UnifiedModel, sampler: Union[SubSampler, None],
        config: Optional[MetricTransformConfig] = None,
        correlation_dict: Optional[Dict] = None) -> (
        UnifiedModel):
    "Method to get an allowed snapshot based on object id"

    try:
        if sampler:
            if not sampler.should_process(message):
                raise ExitTransformLoopException
            sampler.clean_up_unused_entries()

        if __is_a_remif_snapshot(message):
            __process_remif_snapshot(message, correlation_dict)

    except Exception:
        raise ExitTransformLoopException
    return message


def __is_a_remif_snapshot(model: UnifiedModel) -> bool:
    "Method that checks if RemIF snapshot"

    if "IscCtxRemIf" in model.snapshot.obj:
        return True
    return False


def __get_rem_id(model: UnifiedModel):
    """Method to extract rem id from message"""

    pattern = re.compile("<IscCtxRemIf!([0-9]+)>", re.IGNORECASE)
    remid = ""
    match = pattern.match(model.snapshot.obj)

    if match and match.group() and len(match.group()) >= 1:
        remid = match.group(1)

    return remid


def __process_remif_snapshot(model: UnifiedModel, correlation_dict):
    """Method that process the data of the remif snapshot and set
    or updates the correlation instrument point"""

    obs_store: Dict[str, ObsMetStorage] = MetricStore().read_obs_metric(
        "metadata.correlation.identities")
    location = correlation_dict["location"]
    ctx_id = getattr(model.snapshot.ctx, "ctxId")
    rem_id = __get_rem_id(model)

    __check_and_decrease_back_off_counter_if_neccesary(correlation_dict)

    if obs_store and ctx_id and rem_id:
        for v in obs_store.values():
            if (f"{location}_private_context_id" in v.attr.keys() and
                    f"{location}_remote_interface_id" in v.attr.keys() and
                    v.attr[f"{location}_private_context_id"] == ctx_id and
                    v.attr[f"{location}_remote_interface_id"] == rem_id):
                MetricStore().set_metric(
                    name="metadata.correlation.identities",
                    value=datetime.fromtimestamp(v.iter_val/1000).strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    attrs=v.attr,
                    attributes_set=False)
                rem_id_value = MetricStore().get_param_from_attribute_map(
                    "metadata.correlation.identities",
                    "interface_id_to_rem_interface_map",
                    v.attr[f"{location}_remote_interface_id"])  # type: ignore
                if rem_id_value:
                    MetricStore().update_attribute_map(
                        "metadata.correlation.identities",
                        "interface_id_to_rem_interface_map",
                        rem_id,  # type: ignore
                        rem_id_value)  # type: ignore
                return
    if rem_id:
        __retrieve_attributes_and_set_metric(model, correlation_dict, rem_id)


def __retrieve_attributes_and_set_metric(
        model: UnifiedModel, correlation_dict, rem_id):
    """Method that implements the logic for settting the attributes and
    the value for the correlation instrument"""

    snapi_attrs = __get_corr_attrs_from_snapi(model, correlation_dict)
    if snapi_attrs and len(snapi_attrs) >= 6:
        location = correlation_dict["location"]
        rem_location = "sdep" if location == "seep" else "seep"
        local_pub_ctx_id = snapi_attrs[
            f"{location}_public_context_id"]
        rem_pub_ctx_id = snapi_attrs[
            f"{rem_location}_public_context_id"]
        born_stamp = datetime.strptime(snapi_attrs["value"],
                                       '%Y-%m-%d %H:%M:%S')
        other_attrs = __get_remid_and_path(
            correlation_dict, local_pub_ctx_id, born_stamp,
            rem_pub_ctx_id, rem_id)
        if other_attrs and len(other_attrs) == 2:
            all_attrs = {**snapi_attrs, **other_attrs}
            stamp = all_attrs.pop("value")

            MetricStore().set_metric(
                name="metadata.correlation.identities",
                value=stamp,
                attrs=all_attrs,
                attributes_set=False,
                use_default_attributes=False,
            )
            logging.info(f"Correllation datapoint created wtih attrs: "
                         f"{all_attrs} and born stamp: {stamp}")


def __get_corr_attrs_from_snapi(model, correlation_dict):
    """Method that retrieves the correlation instrument attribute
    data and the born timestamp from SNAPI. It does not handle
    the connection because we are using the instance created by the
    rl_controller"""
    snapi_attrs = {}
    try:
        snapi_attrs = correlation_dict[
            "omt_cmd"].retrieve_correlation_point_attributes(
            private_context_id=model.snapshot.ctx.ctxId,
            is_seep=correlation_dict["location"].lower() == "seep")

    except PySNAPIException as err:
        logging.warning(err)
    except requests.exceptions.RequestException as err:
        logging.warning("RazorLink unreachable")
        logging.debug(err)
    else:
        return snapi_attrs

    return {}


def __release_context_over_snapi(correlation_dict, public_context_id):
    """Method that releases a specific context using public
    context id"""
    res = None
    try:
        res = correlation_dict[
            "omt_cmd"].release_context(public_context_id)
    except PySNAPIException as err:
        logging.warning(err)
    except requests.exceptions.RequestException as err:
        logging.warning("RazorLink unreachable")
        logging.debug(err)
    else:
        return res

    return None


def __get_remid_and_path(
        correlation_dict: Dict, local_pub_context_id: str,
        ctx_born_stamp: Any, rem_pub_context_id: str, rem_id: str):
    """Method that sets the path and rem id attributes for the correlation
    instrument. If it cannot retrieve the path it puts N/A, so in the case
    when we miss a log message we don't lose the whole metric."""

    location = correlation_dict["location"]
    rem_location = "sdep" if location == "seep" else "seep"
    other_attrs = {f"{location}_remote_interface_id": rem_id}
    if_name, met_path = None, None

    if_name = MetricStore().get_param_from_attribute_map(
        "metadata.correlation.identities",
        "interface_id_to_rem_interface_map",
        rem_id)
    if if_name:
        met_path = MetricStore().get_param_from_attribute_map(
            "metadata.correlation.identities",
            "interface_to_network_path_map",
            str(if_name), reverse_search=True)

    if if_name and met_path:
        other_attrs[f"{rem_location}_underlay_network_path"] = str(met_path)
    else:
        __release_ctx_if_permitted(
            rem_id, correlation_dict,
            local_pub_context_id, ctx_born_stamp, if_name, met_path,
            rem_pub_context_id)

    return other_attrs

def __release_ctx_if_permitted(
        rem_id, correlation_dict, local_pub_context_id,
        ctx_born_stamp, if_name, met_path, rem_pub_context_id):
    """Method to release a ctx if time permitted. This Will try to
    release contexts only if the ctx born timestamp is over 60 seconds
    to avoid the situation where the VNF cannot establish a particular
    context (context flapping that normally happens every 10 seconds).
    Also it will try to release a context if we have not realeased
    context for this particular rem id before. For all other situations
    we assume we cannot find a path for"""

    rl_interval, can_increment = __get_context_release_interval(
        correlation_dict)

    if not if_name:
        if_name = "N/A"
    if not met_path:
        met_path = "N/A"
        
    if correlation_dict["hold_off"] == 0:
        timestamp_to_use = (correlation_dict["last_ct_chg_event_at"]
                            if correlation_dict["last_ct_chg_event_at"]
                            else ctx_born_stamp)
    else:
        timestamp_to_use = ctx_born_stamp

    if (datetime.utcnow() - timestamp_to_use) > timedelta(seconds=rl_interval):
        logging.info(
            "Invoking release context command to "
            "acquire id to interface map mapping for context "
            f"with local public context id: {local_pub_context_id} "
            f"with remote public context id: {rem_pub_context_id} "
            f"with interface id: {if_name} "
            f"with path: {met_path} "
            f"with remid: {rem_id} "
            f"with born stamp: {ctx_born_stamp} "
            f"REASON: NO ID PRESENT")

        disabled_queries = correlation_dict.get("disabled_queries")
        if "release_context_over_snapi" not in disabled_queries:
            __release_context_over_snapi(
                correlation_dict, local_pub_context_id)

        __set_back_off_props(
            correlation_dict, rl_interval, can_increment, True)


def __check_and_decrease_back_off_counter_if_neccesary(correlation_dict):
    "Method to check if backoff counter can now be decreased"

    rl_interval, can_increment = __get_context_release_interval(
        correlation_dict)
    # Add +20 seconds to allow ctx release to happen first if necessary
    __set_back_off_props(
        correlation_dict, rl_interval + 20, can_increment, False)


def __set_back_off_props(
        correlation_dict, release_interval, can_increment=False,
        ctx_released=False):
    """Method used to set the counter property of the correlation dictionnary
    that controls the release interval that will be calculated and decides
    when a ctx will be released. When a ctx is released the counter property
    increases up to a max. When a ctx has not been released for period greater
    than or equal to the release interval the counter is decreased.

    Args:
        correlation_dict (Dict): Dictionary with sets of properties controlling
        correlation metric generation and exponential backoff
        release_interval (int): The current ctx release interval active
        can_increment (bool, optional): Only used when ctx was released to
        decide whether to increment the counter property
        ctx_released (bool, optional): If true a ctx was released thus counter
        has to increase. If false then if appropriate time has elasped
        decrement the counter used to calculate the release interval
    """
    if ctx_released:
        if can_increment:
            correlation_dict["counter"] += 1
            correlation_dict["last_ct_chg_event_at"] = datetime.utcnow()

        if not correlation_dict["first_ctx_released_at"]:
            correlation_dict["first_ctx_released_at"] = datetime.utcnow()

    elif correlation_dict["last_ct_chg_event_at"]:
        if ((datetime.utcnow() - correlation_dict["last_ct_chg_event_at"]) >
                timedelta(seconds=release_interval)):
            if correlation_dict["counter"] > 0:
                correlation_dict["counter"] -= 1
                correlation_dict["last_ct_chg_event_at"] = datetime.utcnow()


def __get_context_release_interval(correlation_dict):
    """Method that returns the current release ctx interval.This depends
    on if exponential backoff mechanism is enabled or not
    Args:
        correlation_dict (Dict): Dictionary with sets of properties controlling
        correlation metric generation and exponential backoff
    """

    can_increment = False
    release_interval = correlation_dict["start_at"]
    if correlation_dict["back_off_enabled"]:
        if correlation_dict["first_ctx_released_at"]:
            if ((datetime.utcnow() - correlation_dict["first_ctx_released_at"]) >
                    timedelta(seconds=correlation_dict["hold_off"])):

                release_interval = int(correlation_dict["start_at"] * (
                    correlation_dict["exp"] ** correlation_dict["counter"]))

                if release_interval > correlation_dict["max"]:
                    release_interval = correlation_dict["max"]
                else:
                    can_increment = True
        else:
            release_interval = correlation_dict["start_at"]
            if correlation_dict["hold_off"] == 0:
                can_increment = True

    return release_interval, can_increment

def __ignored_function():
    pass
