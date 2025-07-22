"""Module that implements the data processor of the log
and metrics forwarder that transforms the data and
stores it in the output unit """

import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
from typing import Callable, Dict, List, Tuple, Union

import product_common_logging as logging
from pysnapi.commands import OMT

from config_manager.models import CorrelationConfig, MetricTransformConfig
from data_processing import log_data_transforms, metric_data_transforms
from data_processing.exit_transform_loop_exception import (
    ExitTransformLoopException)
from data_processing.models.unified_model import UnifiedModel
from data_processing.sub_sampler import SubSampler
from input_unit.input import InputUnit
from input_unit.shared_data import MessageType
from output_unit.output_unit import OutputUnit
from output_unit.utils import (BufferFullError, BufferNames,
                               InvalidDataFormatError)


def get_transform_functions(module) -> List[Tuple[str, Callable]]:
    """Return a list of tuples. Each tuple has on 0 the name of func and on
    1 the function reference."""
    return [f for f in inspect.getmembers(module, inspect.isfunction)
            if not f[0].startswith("__")]


TRANSFORM_FUNCTIONS_MAP = {
    MessageType.LOG: get_transform_functions(log_data_transforms),
    MessageType.METRIC: get_transform_functions(metric_data_transforms),
}


class DataProcessor(Thread):
    """Class that implements the data processing unit"""

    def __init__(self, input_unit: InputUnit, output_unit: OutputUnit,
                 metric_transform_config: MetricTransformConfig,
                 correlation_config: CorrelationConfig):
        """Method that initialize the data processor"""
        self.sampler: Union[SubSampler, None]
        # to do hold output_unit
        self.input_unit = input_unit
        self.output_unit = output_unit
        self.metric_transform_config = metric_transform_config
        self.correlation_dict = {
            "omt_cmd": OMT(correlation_config.connection),
            "location": correlation_config.location,
            "connection": correlation_config.connection,
            "back_off_enabled": (
                correlation_config.ctx_release_exp_back_off_enabled),
            "hold_off": correlation_config.ctx_release_hold_off_period,
            "start_at": correlation_config.ctx_release_start_at,
            "max": correlation_config.ctx_release_max_setting,
            "exp": correlation_config.ctx_release_exp_factor,
            "first_ctx_released_at": "",
            "counter": 0,
            "last_ct_chg_event_at": "",
            "disabled_queries": correlation_config.disabled_queries,
        }
        if "release_context_over_snapi" in correlation_config.disabled_queries:
            logging.info("Release context over SNAPI has been disabled")
        if correlation_config.sub_sampler_enable:
            self.sampler = SubSampler(
                correlation_config.sub_sampling_period,
                correlation_config.sub_sampler_cleanup_period)
        else:
            self.sampler = None

        self.shutdown_event = Event()
        super().__init__()

    def process_data(
        self,
        msg_type: MessageType,
        output_buff_name: BufferNames,
        transforms: List[Tuple[str, Callable]],
    ):
        """Method that is used to process data from the input unit
        datastore for a data queue and is applying the transformation
        functions sequentially"""

        while not self.shutdown_event.is_set():
            # get the data from input, check for data and errors
            try:
                raw_msg: Dict = self.input_unit.data_store.get_message(
                    msg_type)
            except TypeError as err:
                logging.warning(err)
                continue

            if not raw_msg:
                # release CPU for a while
                time.sleep(0.001)
                continue

            msg = raw_msg
            for func_name, func in transforms:
                try:
                    logging.debug("Applying transform %s on %s",
                                  func_name, msg)
                    if msg_type == MessageType.METRIC:
                        msg = func(msg, self.sampler,
                                   self.metric_transform_config,
                                   self.correlation_dict)
                    else:
                        msg = func(msg, self.metric_transform_config)
                except ExitTransformLoopException:
                    msg = {}
                    break
                except Exception as err:
                    logging.warning(
                        "Transform function %s raised exception %s", func_name,
                        err
                    )
                    msg = {}
                    break

            # We only enqueue a message for output if one of the transforms
            # returned one
            if msg:
                try:
                    self.output_unit.enque_data_in_buffer(
                        output_buff_name, msg
                    )
                except (InvalidDataFormatError) as err:
                    logging.warning(err)
                except BufferFullError as err:
                    logging.debug(err)

    def run(self):
        """Method that implements the data processor threads and keeps adding
        the transformation tasks to these threads"""

        self.shutdown_event.clear()
        executor = ThreadPoolExecutor(max_workers=2)

        executor.submit(
            self.process_data,
            MessageType.LOG,
            BufferNames.LOGS_BUFFER,
            TRANSFORM_FUNCTIONS_MAP[MessageType.LOG],
        )

        executor.submit(
            self.process_data,
            MessageType.METRIC,
            BufferNames.METRICS_BUFFER,
            TRANSFORM_FUNCTIONS_MAP[MessageType.METRIC],
        )

        executor.shutdown(wait=True)

    def stop(self):
        """Method that stops the data processor unit."""

        self.shutdown_event.set()
