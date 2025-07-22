"""Module that contains all the functions
to help with the transformation of observability
data"""

import time
import datetime
import re
from typing import Dict, List, Union

import product_common_logging as logging
from data_processing.models.unified_model import Ctx
from output_unit.otel_conf_model import MetricAttribute


def parse_int(x: str) -> int:
    "Method to parse string to int"

    try:
        return int(x)
    except ValueError as val_exc:
        logging.error(val_exc)
    return -1


def parse_float(x: str) -> float:
    "Method to parse string to float"

    try:
        return float(x)
    except ValueError as val_exc:
        logging.error(val_exc)

    return -1.0


def get_context_id(ctx_field: str) -> str:
    """Method to extract ctx id from message"""

    ctx = ""
    mt = re.compile("<[A-Za-z0-9]+!([0-9]+)>", re.IGNORECASE).match(ctx_field)
    if mt and mt.groups() and len(mt.groups()) >= 1:
        ctx = mt.group(1)

    return ctx


def get_context_id_mbcp(ctx_field: Union[str, Ctx]) -> str:
    """Method to extract ctx id from message"""

    match = None
    ctx = ""
    pattern = re.compile("<IscCtx!([0-9]+)>", re.IGNORECASE)

    if isinstance(ctx_field, str):
        match = pattern.match(ctx_field)
    else:
        match = pattern.match(ctx_field.oid)

    if match and len(match.group()) >= 1:
        ctx = match.group(1)
    else:
        logging.warning(f"failed to extract id from ctx field {ctx_field}")
        ctx = ""
    return ctx


def get_interface_state(netifo: str) -> int:
    """Method to extract interface state based on string
    Otherwise returns -1 which is unknown"""

    state = -1
    mt = re.compile(
        r"([a-zA-Z]+[0-9]+\.?[0-9]+)\s*\(\s*(Ethernet|PPP)\s*\:\s*" r"(Up|Down)\s*",
        re.IGNORECASE,
    ).match(netifo)
    if mt:
        if (mt and mt.groups() and len(mt.groups()) >= 3 and
                mt.group(3).lower() == "up"):
            state = 1
        else:
            state = 0

    return state


def classify_resp_code(code: str):
    "Method used to classify response code to 3 categories"

    int_code = parse_int(code)
    if int_code != -1:
        if int_code >= 100 and int_code <= 399:
            return "SUCCESS"
        elif int_code >= 400 and int_code <= 599:
            return "ERROR"
    return "UNKNOWN RESPONSE CODE"


def get_interface_id(full_id: str) -> str:
    """Method to extract interface id"""
    int_id = "N/A"
    mt = re.compile(
        r"[a-zA-Z\_]*(eth[0-9]+[\_\.][0-9]+|ppp[0-9]+)", re.IGNORECASE
    ).match(full_id)
    if mt and mt.groups() and len(mt.groups()) >= 1:
        int_id = mt.group(1)
    return int_id


def _parse_pdb_field(pdb_field: str):
    pattern = re.compile(r"<(?P<kw>[^>]+)>(?P<val>[^<]+)</(?P=kw)>")
    return pattern.findall(pdb_field)


def get_rx_bitrate(pdb_field: str) -> int:
    """Method to extract the rx bitrate from a pdb field."""
    rx_pattern = re.compile(r"<(?P<kw>RxBitrate)>(?P<val>[0-9]+)</(?P=kw)>")
    m = rx_pattern.search(pdb_field)
    if m:
        return int(m.group("val"))
    return -1


def get_tx_bitrate(pdb_field: str) -> int:
    """Method to extract the rx bitrate from a pdb field."""
    rx_pattern = re.compile(r"<(?P<kw>TxBitrate)>(?P<val>[0-9]+)</(?P=kw)>")
    m = rx_pattern.search(pdb_field)
    if m:
        return int(m.group("val"))
    return -1


def gt(args: List) -> bool:
    """Method to test if arg[1] is > arg[0]"""

    if len(args) == 2:
        if args[1] > args[0]:
            return True
    return False


def add(args: List) -> List[Dict[str, Union[float, int]]]:
    """Method to add arguments"""
    arg_sum: Union[float, int] = 0.0
    for arg in args:
        if isinstance(arg, int) or isinstance(arg, float):
            arg_sum += arg
        else:
            return [{"val": -1}]
    return [{"val": arg_sum}]


def return_mbcp_msg_dict(values) -> List[Dict[str, object]]:
    """Method used to classify return data for mbcp messages"""

    res = [
        {"val": 0, "att_dct": {"category": "successfully_sent"}},
        {"val": 0, "att_dct": {"category": "failed"}},
        {"val": 0, "att_dct": {"category": "timed_out"}},
    ]
    if values and "None" not in values and len(values) == 3:
        for x, _ in enumerate(values):
            res[x]["val"] = values[x]

    return res


def classify_tx_dropped_pdus(values) -> List[Dict[str, object]]:
    """classifies the reason of underlay tx dropped pdus."""

    res = [
        {"val": 0, "att_dct": {"reason": "acknowledged_lost"}},
        {"val": 0, "att_dct": {"reason": "timed_out"}},
        {"val": 0, "att_dct": {"reason": "locally"}},
        {"val": 0, "att_dct": {"reason": "in_transit"}},
        {"val": 0, "att_dct": {"reason": "unknown"}},
    ]
    if values and "None" not in values and len(values) == 5:
        for x, _ in enumerate(values):
            res[x]["val"] = values[x]

    return res


def classify_rx_dropped_pdus(values) -> List[Dict[str, object]]:
    """classifies the reason of underlay rx dropped pdus."""

    res = [
        {"val": 0, "att_dct": {"reason": "lost"}},
        {"val": 0, "att_dct": {"reason": "out_of_order"}},
        {"val": 0, "att_dct": {"reason": "duplicated"}},
        {"val": 0, "att_dct": {"reason": "too_old"}},
    ]
    if values and "None" not in values and len(values) == 4:
        for x, _ in enumerate(values):
            res[x]["val"] = values[x]

    return res


def get_tenant_id(sched_str: str):
    """Extracts tenant id from classification group"""
    tenant_id = "N/A"
    mt = re.compile(r"^([a-zA-Z0-9]+)", re.IGNORECASE).match(sched_str)
    if mt and mt.groups() and len(mt.groups()) >= 1:
        tenant_id = mt.group(1)
    return tenant_id


def get_sched_group(sched_str: str):
    """Extracts scheduling group from classification group"""
    sched_grp = "N/A"
    mt = re.compile(
        r"\-([a-zA-Z0-9\-]+[a-zA-Z0-9]+|[a-zA-Z0-9]+)\@", re.IGNORECASE
    ).search(sched_str)
    if mt and mt.groups() and len(mt.groups()) >= 1:
        sched_grp = mt.group(1)
    return sched_grp


def convert_timestamp_to_ms(datetime_str):
    """Method to convert timestamp to ms"""
    try:
        if datetime_str:
            date_time = datetime.datetime.strptime(
                datetime_str, "%Y-%m-%d %H:%M:%S")

            # Calculate the difference from unix epoch
            a_timedelta = date_time - datetime.datetime(
                year=1970, month=1, day=1, hour=0, minute=0, second=0
            )
            m_seconds = int(a_timedelta.total_seconds() * 1000)

            return m_seconds
    except ValueError as val_err:
        logging.warn(f"failed to convert timestamp: {val_err}")

    return 0


def categorize_tx_data(data):
    """Method to categorize tx data"""
    attrs = [
        {"val": 0, "att_dct": {"category": "user_data"}},
        {"val": 0, "att_dct": {"category": "retransmissions"}},
        {"val": 0, "att_dct": {"category": "channel_overheads"}},
    ]
    try:
        assert len(data) == 3, f"Data len expected 3, got {len(data)}"

        exp = 32
        if any(i > (2**exp-1) for i in data):
            exp = 64
        for k, v in enumerate(data):
            if k == 0:
                attrs[k]["val"] = v
            else:
                if v >= data[k-1]:
                    attrs[k]["val"] = v - data[k-1]
                else:
                    if (2**exp - 1) >= data[k-1] and (2**exp - 1) >= v:
                        attrs[k]["val"] = 2**exp - 1 - data[k-1] + v
                    else:
                        attrs[k]["val"] = -1
    except AssertionError as asrt_err:
        logging.debug(asrt_err)

    return attrs


def categorize_demanded_metrics(data):
    """Method to send demand data"""
    types = ["input_from_user", "discarded", "discarded", "discarded"]
    sub_categories_a = ["received_from_user", "timed_out", "overflowed", "other"]
    attrs = []

    values_to_process = data[0:4] if data and len(data) >= 5 else [-1] * 4

    for k, v in enumerate(values_to_process):
        attrs.append({
            "val": v,
            "att_dct": {
                "type": types[k],
                "category": "user_data",
                "sub-category_a": sub_categories_a[k],
                "source": "scheduler",
                "scope": "overlay_network"

            }
        })

    return attrs

def transform_tx_data_seep(data):
    """Method to transform tx data fields for overlay status metrics"""
    ret_data = [{
        "val": {
            "ud": -1,
            "ret": -1,
            "ctx_id": "",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                    value="categorised")}
            }
        }
    }]
    if data and len(data) >= 3:
        ret_data[0]["val"]["ud"] = data[0]
        if data[1] >= data[0]:
            ret_data[0]["val"]["ret"] = data[1] - data[0]
        else:
            if (2**32 - 1) >= data[0] and (2**32 - 1) > data[1]:
                ret_data[0]["val"]["ret"] = 2**32 - 1 - data[0] + data[1]
            else:
                ret_data[0]["val"]["ret"] = -1
        ret_data[0]["val"]["ctx_id"] = data[2]

    return ret_data


def transform_tx_data_sdep(data):
    """Method to transform tx data fields for overlay status metrics"""
    ret_data = [
        {
            "val": {
                "ud": -1,
                "ret": -1,
                "ctx_id": "",
                "attributes": {
                    "dp1": {
                       "sampling_method": MetricAttribute(value="raw")},
                    "dp2": {
                       "sampling_method": MetricAttribute(value="categorised")}
                }
            }
        }
    ]

    if data and len(data) >= 3:
        ret_data[0]["val"]["ud"] = data[0]
        if data[1] >= data[0]:
            ret_data[0]["val"]["ret"] = data[1] - data[0]
        else:
            if (2**32 - 1) >= data[0] and (2**32 - 1) > data[1]:
                ret_data[0]["val"]["ret"] = 2**32 - 1 - data[0] + data[1]
            else:
                ret_data[0]["val"]["ret"] = -1
        ret_data[0]["val"]["ctx_id"] = data[2]
        for k, _ in ret_data[0]["val"]["attributes"].items():
            ret_data[0]["val"]["attributes"][k][
                "context_id"] = MetricAttribute(value=data[2])

    return ret_data


def transform_mbcp_data_seep(data):
    """Method to transform mbcp data fields for underlays status metrics"""
    ret_data = [{
        "val": {
            "sB": -1,
            "fB": -1,
            "rtt": -1,
            "ctx_id": "",
            "attributes": {
                "dp1": {"sampling_method": MetricAttribute(value="raw")},
                "dp2": {"sampling_method": MetricAttribute(
                    value="categorised")},
                "dp3": {"status": MetricAttribute(value="interface_enabled")},
                "dp4": {"status": MetricAttribute(value="peer_connectivity")},
                "dp5": {"status": MetricAttribute(value="keep_alive")}
            }
        }
    }]
    if data and len(data) >= 6:
        ret_data[0]["val"]["sB"] = data[0]
        ret_data[0]["val"]["fB"] = data[1] + data[2]
        ret_data[0]["val"]["rtt"] = data[3]
        ret_data[0]["val"]["ctx_id"] = data[4]
        if_id = get_ifid_from_mbcp_msg(data[5])
        ret_data[0]["val"]["if_id"] = if_id
        if len(data) >= 7:
            ret_data[0]["val"]["ns"] = data[6]

    return ret_data


def transform_mbcp_data_sdep(data):
    """Method to transform mbcp data fields for underlays status metrics"""
    ret_data = [
        {
            "val": {
                "sB": -1,
                "fB": -1,
                "rtt": -1,
                "ctx_id": "",
                "attributes": {
                    "dp1": {"sampling_method": MetricAttribute(value="raw")},
                    "dp2": {"sampling_method": MetricAttribute(
                        value="categorised")}
                }
            }
        }
    ]

    if data and len(data) >= 6:
        ret_data[0]["val"]["sB"] = data[0]
        ret_data[0]["val"]["fB"] = data[1] + data[2]
        ret_data[0]["val"]["rtt"] = data[3]
        ret_data[0]["val"]["ctx_id"] = data[4]
        rem_id = get_remid_from_mbcp_msg(data[5])
        if_id = get_ifid_from_mbcp_msg(data[5])
        ret_data[0]["val"]["if_id"] = if_id
        ret_data[0]["val"]["rem_id"] = rem_id
        for k, _ in ret_data[0]["val"]["attributes"].items():
            ret_data[0]["val"]["attributes"][k][
                "sdep_remote_interface_id"] = MetricAttribute(
                    value=str(rem_id))
            ret_data[0]["val"]["attributes"][k][
                "context_id"] = MetricAttribute(value=data[4])

    return ret_data


def get_remid_from_mbcp_msg(info):
    """Method to extract the rem interface id from
    an mbcp message for correlation"""
    rem_id = ""
    info_parts = info.split('<->')
    if len(info_parts) >= 2:
        info_parts2 = info_parts[1].replace(
            "<", "").replace(">", "").split('!')
        if len(info_parts2) >= 2:
            rem_id = info_parts2[1]
    return rem_id


def get_ifid_from_mbcp_msg(info):
    """Method to extract the rem interface id from
    an mbcp message for correlation"""
    if_id = ""
    info_parts = info.split('<->')
    if len(info_parts) >= 2:
        if_id = info_parts[0]
    return if_id


def transform_mbcp_data(data):
    """Method to transform mbcp data fields for underlays status metrics"""
    ret_data = [
        {"val": {"sB": -1, "fB": -1, "rtt": -1, "ctx_id": ""},
         "att_dct": {"sampling_method": "categorised"}}
    ]
    if data and len(data) == 5:
        ret_data[0]["val"]["sB"] = data[0]
        ret_data[0]["val"]["fB"] = data[1] + data[2]
        ret_data[0]["val"]["rtt"] = data[3]
        ret_data[0]["val"]["ctx_id"] = data[4]

    return ret_data


def transform_tx_data(data):
    """Method to transform tx data fields for overlay status metrics"""
    ret_data = [
        {"val": {"ud": -1, "ret": -1, "ctx_id": ""},
         "att_dct": {"sampling_method": "categorised"}}
    ]
    if data and len(data) == 3:
        ret_data[0]["val"]["ud"] = data[0]
        if data[1] >= data[0]:
            ret_data[0]["val"]["ret"] = data[1] - data[0]
        else:
            if (2**32 - 1) >= data[0] and (2**32 - 1) > data[1]:
                ret_data[0]["val"]["ret"] = 2**32 - 1 - data[0] + data[1]
            else:
                ret_data[0]["val"]["ret"] = -1
        ret_data[0]["val"]["ctx_id"] = data[2]
        ret_data[0]["val"]["ctx_id"] = data[2]

    return ret_data


def get_bgp_state_data(obs_store, current_bgp_state: str = "", ):
    """Method used to extract time in seconds and bgp state for overlays"""

    timer = __extract_bgp_time(current_bgp_state)
    bgp_state = __extract_bgp_state(current_bgp_state)

    entries = {}
    for k, v in obs_store.items():
        if "bgp_state" in v.attr.values():
            entries[k] = {"en_type": "bgp_state",
                          "to_set": bgp_state}
        elif "bgp_state_timer" in v.attr.values():
            entries[k] = {"en_type": "bgp_state_timer",
                          "to_set": timer}
    return entries


def __extract_bgp_time(current_bgp_state: str):
    """Method used to extract time from bgp state"""

    timer: int = 0
    mt = re.compile(
        r"(([0-9]+\-[0-9]+\-[0-9]+)*\s*([0-9]+\:[0-9]+\:[0-9]+\.[0-9]+)+"
        r"|([0-9]+\-[0-9]+\-[0-9]+)+\s*([0-9]+\:[0-9]+\:[0-9]+\.[0-9]+)*)",
        re.IGNORECASE).match(current_bgp_state)
    dt_formats = ['%H:%M:%S', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
    if mt and mt.groups() and len(mt.groups()) >= 1:
        for frm in dt_formats:
            try:
                x = time.strptime(mt.group(1).strip().split('.')[0], frm)
            except ValueError:
                pass
            else:
                if frm == '%H:%M:%S':
                    timer = int(datetime.timedelta(
                        hours=x.tm_hour, minutes=x.tm_min,
                        seconds=x.tm_sec).total_seconds())
                else:
                    curr_time = datetime.datetime.fromtimestamp(time.mktime(x))
                    print(curr_time)
                    delta = curr_time - datetime.datetime(
                        year=1970, month=1, day=1,
                        hour=0, minute=0, second=0)
                    timer = int(delta.total_seconds())
                break
    return timer


def __extract_bgp_state(current_bgp_state: str):
    """Method used to extract bgp state"""

    bgp_state = 0

    if "established" in current_bgp_state.lower():
        bgp_state = 6
    elif "openconfirm" in current_bgp_state.lower():
        bgp_state = 5
    elif "opensent" in current_bgp_state.lower():
        bgp_state = 4
    elif "active" in current_bgp_state.lower():
        bgp_state = 3
    elif "connect" in current_bgp_state.lower():
        bgp_state = 2
    elif "idle" in current_bgp_state.lower():
        bgp_state = 1

    return bgp_state

def categorize_supplied_overlay_metrics(data):
    "Method to extract QoSM.Stor.User.BlPo.nBl or QoSM.Stor.User.BlPo.nBy from IscSchedGrp"

    attrs = []

    values_to_process = data[0:1] if data and len(data) >= 2 else [-1] * 1
    for k, v in enumerate(values_to_process):
        attrs.append({
            "val": v,
            "att_dct": {
                "type": "transmitted",
                "category": "user_data",
                "sub-category_a": "transmitted_by_peer",
                "source": "scheduler",
                "scope": "overlay_network"
            }
        })

    return attrs
