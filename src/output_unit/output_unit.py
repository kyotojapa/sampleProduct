"""This module is the Output Unit for the RazorLink Logs and Metrics
Forwarder. This component is responsible for pushing data out of the
Output Buffers (Logs and Metrics Queues) into Adapters so it will be
sent out to different locations.
The module does the following:
    - configure the buffers (e.g. size limit of Queue)
    - register and configure adapters
    - expose methods for enqueuing / dequeuing to the Data Processing Unit
    - route the stream from buffers to the appropriate adapter (use of
      policies that will choose the adapters based of met requirements)
"""
import inspect
import re
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from time import sleep
from typing import Any, Dict, List, Type, Union

import product_common_logging as logging
from config_manager.models import AnyConfig
from data_processing.models.unified_model import UnifiedModel

from output_unit import adapters, policies
from output_unit.utils import (
    BufferFullError,
    BufferNames,
    BufferNotFoundError,
    InvalidDataFormatError,
)

STOP_THREADS_EVENT = Event()
TIME_PROCESSING_METRICS = 0.01
TIME_PROCESSING_LOGS = 0.01


class OutputUnit:
    """OutputUnit main entrypoint. This component will receive as inputs
    the Adapters config dict (used for registering the Adapters that are
    present in it) and buffer sizes for both Logs and Metrics buffers.
    The unregistered adapters will be reported back to the Controller
    component. OutputUnit is responsible for processing the data from the
    buffers and send it to the appropriate Adapters.
    """

    def __init__(self, adapters_config: Dict[str, AnyConfig],
                 logs_buffer_size: int, metrics_buffer_size: int) -> None:
        """self._policies will store a list of Policy Instances (
        e.g. PolicyLogsDefault()) found in the output_unit.policies module.
        Each Policy is a Singleton instance. This will be used to decide
        which Adapter will be used to route the data.
        """
        if (isinstance(logs_buffer_size, int) and
                isinstance(metrics_buffer_size, int)):
            self._output_buffers: Dict[BufferNames, Queue] = {
                BufferNames.LOGS_BUFFER: Queue(maxsize=logs_buffer_size),
                BufferNames.METRICS_BUFFER: Queue(maxsize=metrics_buffer_size),
            }
        else:
            raise TypeError("Output buffer size must be an integer!")

        self._metrics_counter: int = 0
        self._logs_counter: int = 0
        self._adapters_config = adapters_config
        self._adapters: Dict = {}
        self._policies: List[policies.IPolicy] = [
            policy_class()
            for policy_name, policy_class in inspect.getmembers(policies)
            if policy_name.startswith("Policy")
            ]
        self.thread_logs: Union[None, Thread] = None
        self.thread_metrics: Union[None, Thread] = None

    def __check_buffer_name(self, buffer_name: BufferNames) -> bool:
        """Method used to check if the passed argument is found in the
        Output Buffers dict.
        Args:
            buffer_name (BufferNames): Enum representing either
                                       LOGS_BUFFER or METRICS_BUFFER
        Returns:
            bool: True if buffer_name in self._output_buffers
        Raises:
            BufferNotFoundError: If buffer_name not found in
                                 self._output_buffers
        """
        if buffer_name not in self._output_buffers:
            error_msg = (f"The provided {buffer_name} was not found in "
                         "output buffers!")
            raise BufferNotFoundError(error_msg)
        return True

    def enque_data_in_buffer(self, buffer_name: BufferNames,
                             data: Dict) -> None:
        """Method used to put data in queue.
        Args:
            buffer_name (BufferNames): Enum representing either
                                       LOGS_BUFFER or METRICS_BUFFER
            data (Dict): dictionary representing a log / metric line
                         from stream
        Raises:
            BufferFullError: If Output buffer is full and can't enque
                             any new data
            InvalidDataFormatError: If the data we want to put in Output
                                    buffer is not a dict
        """
        if self.__check_buffer_name(buffer_name):
            if isinstance(data, dict) or isinstance(data, UnifiedModel):
                if ((self._output_buffers[buffer_name].qsize() % 1000 - 1) == 0
                        and self._output_buffers[buffer_name].qsize() > 1000):
                    logging.warning(
                        f"Output Buffer size is: "
                        f"{self._output_buffers[buffer_name].qsize()}")
                if not self._output_buffers[buffer_name].full():
                    self._output_buffers[buffer_name].put(data)
                else:
                    raise BufferFullError(f"The buffer {buffer_name.name} "
                                          "is full!")
            else:
                raise InvalidDataFormatError(f"The provided {data} isn't in "
                                             "the right format!")

    def deque_data_from_buffer(self, buffer_name: BufferNames) -> Dict:
        """Method used to extract data from queue.
        Args:
            buffer_name (BufferNames): Enum representing either
                                       LOGS_BUFFER or METRICS_BUFFER
        Returns:
            Dict: data extracted from queue
        """
        if self.__check_buffer_name(buffer_name):
            if not self._output_buffers[buffer_name].empty():
                return self._output_buffers[buffer_name].get()
        return {}

    @staticmethod
    def get_available_adapters() -> List[Type]:
        """Method used to create a list with all available Adapter
        classes (output_unit.adapters).
        Returns:
            available_adapters (List[Type]): list with all the Adapters
        """
        available_adapters = [adapter_class
                              for adapter_name, adapter_class in
                              inspect.getmembers(adapters)
                              if re.match(r'^.+Adapter$', adapter_name)]
        return available_adapters

    @staticmethod
    def _read_and_amend_exporters_config(exporters_config: Union[Dict[str, Any], None]):
        enabled_exporters = []
        export_interval_millis = []
        export_timeout_millis = []

        if exporters_config:
            for exporter, exporter_values in exporters_config.items():
                if exporter_values["enabled"]:
                    export_interval_millis.append(exporter_values[
                        "interval"] * 1000)
                    if "timeout_interval" in exporter_values:
                        export_timeout_millis.append(exporter_values[
                            "timeout_interval"] * 1000)
                    else:
                        export_timeout_millis.append(10 * 1000)
                    enabled_exporters.append(exporter)

        return enabled_exporters, export_interval_millis, export_timeout_millis

    def register_adapters(
            self, system_id=None, instruments_config=None,
            resources=None, exporters_config=None, only_otel=False, path: Path = Path()) -> List[Type]:
        """Method used to register the Adapters for the Output Unit based
        on this logic:
            Iterating over the list of all Adapter classes and checking each
        Adapter class name if it's present as a key in self._adapters_config
        dict. If not present, a list of all unconfigured Adapters will append
        this item, otherwise in the self._adapters dict we will store Adapter
        class name as key and an instance of that class as value. Also call
        the `configure(adapter_config)` method.
            An additional functionality is that we need to re-register the OTEL
        adapter when we get the system_id, so it can be added as a Resource 
        attribute, hence the presence of "only_otel" flag.

        Returns:
            unconfigured_adapters (List[Type]): list of Adapter classes that
                weren't found in Adapters Config
        """
        available_adapters = self.get_available_adapters()
        unconfigured_adapters = []
        if only_otel:
            if ("OTELMetricsAdapter" in self._adapters and
                    self._adapters["OTELMetricsAdapter"].otel_metrics_obj):
                self._adapters["OTELMetricsAdapter"].otel_metrics_obj.meter_provider.shutdown()
                del self._adapters["OTELMetricsAdapter"]
            self._adapters["OTELMetricsAdapter"] = adapters.OTELMetricsAdapter()
            adapter_config = self._adapters_config.get("OTELMetricsAdapter")
            (adapter_config.enabled_exporters, adapter_config.export_interval_millis,  # type: ignore
                adapter_config.export_timeout_millis) = (  # type: ignore
                    self._read_and_amend_exporters_config(exporters_config))  # type: ignore
            self._adapters["OTELMetricsAdapter"].configure(adapter_config,
                                                           instruments_config=instruments_config,
                                                           resource_attributes=resources)
            return []
        for adapter_class in available_adapters:
            adapter_config = self._adapters_config.get(adapter_class.__name__)
            if not adapter_config:
                logging.error("%s was not found in Adapter's Config!",
                              adapter_class.__name__)
                unconfigured_adapters.append(adapter_class)
            else:
                if (adapter_class.__name__ in
                        self._adapters_config["General"].enabled_adapters):  # type: ignore  # noqa: E501
                    try:
                        self._adapters[adapter_class.__name__] = (
                            adapter_class()
                        )
                        if adapter_class.__name__ == "OTELMetricsAdapter":
                            self._adapters[adapter_class.__name__].configure(
                                adapter_config, system_id
                            )
                        else:
                            self._adapters[adapter_class.__name__].configure(
                                adapter_config
                            )
                    except Exception as err:
                        logging.warning("Caught %s when trying to configure "
                                        "the %s!", type(err).__name__,
                                        adapter_class.__name__)
                        unconfigured_adapters.append(adapter_class)

        return unconfigured_adapters

    def __count_messages(self, buffer_name: BufferNames) -> None:
        """Method used to count the processed messages.
        Args:
            buffer_name (BufferNames): Enum representing either
                                       LOGS_BUFFER or METRICS_BUFFER
        """
        if buffer_name == BufferNames.METRICS_BUFFER:
            self._metrics_counter += 1
        if buffer_name == BufferNames.LOGS_BUFFER:
            self._logs_counter += 1

    def __process_streaming(self, buffer_name: BufferNames) -> None:
        """Method used to process streaming out of buffers.
        Method extracts the data from the buffer and passes it to the list
        of policies (self._policies). Each Policy checks if it passes its
        conditions and will assign a specific Adapter that will take the
        data. Each processed data is counted by self.__count_messages().
        Args:
            buffer_name (BufferNames): Enum representing either
                                       LOGS_BUFFER or METRICS_BUFFER
        """
        data = self.deque_data_from_buffer(buffer_name)
        self.__count_messages(buffer_name)

        if data:
            for policy in self._policies:
                if (policy.should_process(buffer_name) and
                        policy.condition(data)):
                    adapter = self._adapters.get(policy.chosen_adapter())
                    if adapter:
                        adapter.write_data(data)

    def process_metrics(self, sleep_time: float = 0.1) -> None:
        """Method used to process streaming out of the metrics buffer.
        This method will be assigned to a Thread to run in loop until the
        stop event is set. Each message will be processed at a sleep_time
        rate.
        Args:
            sleep_time (float): time (sec) between two processes
        """
        while not STOP_THREADS_EVENT.is_set():
            try:
                self.__process_streaming(BufferNames.METRICS_BUFFER)
            except Exception as err:
                logging.error(err)
            sleep(sleep_time)

    def process_logs(self, sleep_time: float = 0.1) -> None:
        """Method used to process streaming out of the logs buffer.
        This method will be assigned to a Thread to run in loop until the
        stop event is set. Each message will be processed at a sleep_time
        rate.
        Args:
            sleep_time (float): time (sec) between two processes
        """
        while not STOP_THREADS_EVENT.is_set():
            try:
                self.__process_streaming(BufferNames.LOGS_BUFFER)
            except Exception as err:
                logging.error(err)
            sleep(sleep_time)

    def run_threads(self) -> None:
        """Method used to start the Threads for the Logs and
        Metrics Processing.
        """
        self.thread_metrics = Thread(name="OutputUnitMetrics",
                                     target=self.process_metrics,
                                     args=(TIME_PROCESSING_METRICS,))
        self.thread_logs = Thread(name="OutputUnitLogs",
                                  target=self.process_logs,
                                  args=(TIME_PROCESSING_LOGS,))
        self.thread_metrics.daemon = True
        self.thread_logs.daemon = True
        self.thread_metrics.start()
        self.thread_logs.start()

    def set_stop_event(self) -> None:
        """Method used to set the STOP_THREADS_EVENT in order to end
        the loops.
        """
        STOP_THREADS_EVENT.set()
