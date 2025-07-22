"""Module that holds the log data transformation functions.
This functions will be executed on each message when passing from input unit
to output unit.

Transforms are applied in the order they appear in this file - top down.
The output from transform1 forms the input to transform2 etc.

If a message is found to be unsuitable for processing an
ExitTransformLoopException can be thrown.

input_unit ----> apply transform functions ----> output_unit"""

import re
from typing import Dict, Optional

from data_processing.exit_transform_loop_exception import (
    ExitTransformLoopException)
from output_unit.metric_storage import MetricStore
import product_common_logging as logging


def process_log_messages(message: Dict, config: Optional[Dict] = None) -> Dict:
    """Function that handles processing of log messages"""
    try:
        if (message["Hd"]["Ct"] == "lw.comp.ISC.context.interface" and
                message["Hd"]["Fu"] == "UpdateFromMsgFromRemote"):
            __process_remote_interface_message(message)
    except Exception:
        raise ExitTransformLoopException
    return message


def __process_remote_interface_message(message: Dict):
    """Process log message that we used ot extract the interface
    id to interface map binding"""

    oid = message["Hd"]["Cl"]["oid"]
    txt = message["txt"]

    hd_cl_oid = re.match(r"<IscCtxRemIf!(.*?)>", oid)
    rem_ig_uid = re.match(r"^.*remIfUID=([a-zA-Z\_]*(eth[0-9]+"
                          r"[\_\.\-][0-9]+[\_\-\.]*[0-9]*|ppp[0-9]+"
                          r"[\_\-\.]*[0-9]*))\s*$", txt, re.M | re.S)

    if hd_cl_oid and rem_ig_uid:
        MetricStore().update_attribute_map(
            name="metadata.correlation.identities",
            map_name="interface_id_to_rem_interface_map",
            key=hd_cl_oid.group(1), value=rem_ig_uid.group(1))


def __ignored_function():
    pass
