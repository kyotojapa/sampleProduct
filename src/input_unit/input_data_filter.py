import re
from typing import Dict

from data_processing.models.unified_model import OBJECT_ID_PATTERN

ACCEPTED_OBJECT_IDS_PATTERN = re.compile(OBJECT_ID_PATTERN)


def has_valid_object_id(json_data: Dict) -> bool:
    """
    Checks if the object id inside the data is valid.

    :param json_data: metric
    :return: True if json_data includes a valid object id, False otherwise
    """
    if "snapshot" in json_data and "obj" in json_data["snapshot"]:
        object_id = json_data["snapshot"]["obj"]
        return ACCEPTED_OBJECT_IDS_PATTERN.match(object_id) is not None

    return False
