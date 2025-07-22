"""Module that holds the log data transformation functions.
This functions will be executed on each message when passing from input unit
to output unit.

Transforms are applied in the order they appear in this file - top down.
The output from transform1 forms the input to transform2 etc.

If a message is found to be unsuitable for processing an
ExitTransformLoopException can be thrown.

input_unit ----> apply transform functions ----> output_unit"""

from typing import Dict, Optional


def log_function_name1(message: Dict, config: Optional[Dict] = None) -> Dict:
    """Example transformation method."""
    return message


def __ignored_function():
    pass
