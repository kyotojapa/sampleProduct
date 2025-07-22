"""This module is the Policies for the Output Unit (RazorLink
Logs and Metrics forwarder). The Policies are responsible for
computing the rules on which Adapters will be used by the Output
Unit.
"""
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict

import product_common_logging as logging
import re

from data_processing.models.unified_model import UnifiedModel
from output_unit.adapters import (
    FluentdAdapter, LogsFileAdapter, MetricsFileAdapter,
    MQTTLogsAdapter, MQTTMetricsAdapter, OTELMetricsAdapter)
from output_unit.utils import BufferNames


class SingletonMeta(ABCMeta):
    """Implementation of a Singleton that will be used as Metaclass.
    """

    _instances: Dict[type, type] = {}

    def __call__(cls, *args, **kwargs) -> type:
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class IPolicy(ABC):
    """Abstract base class for Policy. This is the interface for the
    following Policies: PolicyLogsDebug, PolicyMetricsDebug,
    PolicyLogsDefault, PolicyMetricsDefault and PolicyShoreLogsDefault.
    """
    def __init__(self) -> None:
        pass

    @abstractmethod
    def should_process(self, buffer_name: BufferNames) -> bool:
        """Abstract base method for checking if the buffer's data
        should be processed or not. This will be overriden.
        `Returns`: bool
        """

    @abstractmethod
    def condition(self, data=None) -> bool:
        """Abstract base method for checking the conditions in which
        we will use one of the Adapters or not. This will be overriden.
        `Returns`: bool
        """

    @abstractmethod
    def chosen_adapter(self) -> str:
        """Abstract base method that returns the name of the chosen
        Adapter.
        `Returns`: str
        """


class PolicyLogsDebug(IPolicy, metaclass=SingletonMeta):
    """PolicyLogsDebug implements IPolicy and is used to check if the
    module is at Debug level and will return to the Output Unit
    the FileAdapter as the adapter to use.
    """
    def should_process(self, buffer_name: BufferNames) -> bool:
        """Take decision to process only logs."""
        return buffer_name is BufferNames.LOGS_BUFFER

    def condition(self, data=None) -> bool:
        """PolicyLogsDebug is satisfying this condition only if the logging
        level is set on DEBUG.
        """
        return logging.getLogger().getEffectiveLevel() == (logging.DEBUG)

    def chosen_adapter(self) -> str:
        """PolicyLogsDebug will use LogsFileAdapter if the condition is met.
        """
        return LogsFileAdapter.__name__


class PolicyMetricsDebug(IPolicy, metaclass=SingletonMeta):
    """PolicyMetricsDebug implements IPolicy and is used to check if the
    module is at Debug level and will return to the Output Unit
    the MetricsFileAdapter as the adapter to use.
    """
    def should_process(self, buffer_name: BufferNames) -> bool:
        """Take decision to process only metrics."""
        return buffer_name is BufferNames.METRICS_BUFFER

    def condition(self, data=None) -> bool:
        """PolicyMetricsDebug is satisfying this condition only if the
        logging level is set on DEBUG.
        """
        return logging.getLogger().getEffectiveLevel() == (logging.DEBUG)

    def chosen_adapter(self) -> str:
        """PolicyMetricsDebug will use MetricsFileAdapter if the condition
        is met.
        """
        return MetricsFileAdapter.__name__


class PolicyLogsDefault(IPolicy, metaclass=SingletonMeta):
    """PolicyLogsDefault implements IPolicy and is the default
    Policy / rule that will always route the Logs Buffer output to
    the MQTTLogsAdapter.
    """
    def should_process(self, buffer_name: BufferNames) -> bool:
        """Take decision to process only logs."""
        return buffer_name is BufferNames.LOGS_BUFFER

    def condition(self, data=None) -> bool:
        """PolicyLogsDefault is the default rule to use."""
        return logging.getLogger().getEffectiveLevel() != (logging.DEBUG)

    def chosen_adapter(self) -> str:
        """PolicyLogsDefault will use MQTTLogsAdapter if the condition
        is met.
        """
        return MQTTLogsAdapter.__name__


class PolicyMetricsDefault(IPolicy, metaclass=SingletonMeta):
    """PolicyMetricsDefault implements IPolicy and is the default
    Policy / rule that will always route the Metrics Buffer output to
    the MQTTMetricsAdapter.
    """
    def should_process(self, buffer_name: BufferNames) -> bool:
        """Take decision to process only metrics."""
        return buffer_name is BufferNames.METRICS_BUFFER

    def condition(self, data=None) -> bool:
        """PolicyMetricsDefault is the default rule to use."""
        return (logging.getLogger().getEffectiveLevel() != (logging.DEBUG) and
                self.__is_allowed_message(data))

    def chosen_adapter(self) -> str:
        """PolicyMetricsDefault will use MQTTMetricsAdapter if the
        condition is met.
        """
        return MQTTMetricsAdapter.__name__

    def __is_allowed_message(self, data=None) -> bool:
        """Method to determine if the metrics adapter is allowed
        to receive a message"""

        res = False
        if isinstance(data, UnifiedModel):
            obj_field = data.snapshot.obj
            mt = re.compile(r"^<(IrIf|MbcpDefCC)![0-9]+>$",
                            re.IGNORECASE).match(obj_field)
            if mt and mt.groups() and len(mt.groups()) >= 1:
                res = True
        return res


class PolicyShoreLogsDefault(IPolicy, metaclass=SingletonMeta):
    """PolicyShoreLogsDefault implements IPolicy and is used to check
    if the incoming data is from Logs output buffer and will route the
    stream to the FluentdAdapter.
    """
    def should_process(self, buffer_name: BufferNames) -> bool:
        """This Policy only triggers if the data is coming from
        Logs output buffer.
        """
        return buffer_name == BufferNames.LOGS_BUFFER

    def condition(self, data=None) -> bool:
        """This method is set default to return True.
        """
        return True

    def chosen_adapter(self) -> str:
        """PolicyShoreLogsDefault will use the FluentdAdaper if the
        conditions are met.
        """
        return FluentdAdapter.__name__


class PolicyShoreMetricsOTEL(IPolicy, metaclass=SingletonMeta):
    """Implements IPolicy and is used to check if the incoming data
    should be routed to the OTELMetricsAdapter.
    """

    def should_process(self, buffer_name: BufferNames) -> bool:
        """the routing of each message is decided in InputDataProcessor.
        get_message_type at the moment, those which have a snapshot kw,
        will end up in the metrics buffer"""
        return buffer_name == BufferNames.METRICS_BUFFER

    def condition(self, data=None) -> bool:
        """see OutputUnit.__process_streaming / __process_metrics
        to fine-tune what data is handled by this specific adapter
        data-specific checks can be added here something like re.match
        (OID_PATTERN, data.snapshot.hd.cl.oid) the data received here
        is already processed by the data_processing module"""

        return True

    def chosen_adapter(self) -> str:
        return OTELMetricsAdapter.__name__
