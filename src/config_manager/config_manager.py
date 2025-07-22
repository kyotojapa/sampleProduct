"""This module is the Config Manager for the prod1 Logs and
Metrics Forwarder.
The module is responsible of reading all the configuration files
(e.g. startup.conf, logs.conf), validating these files, exposing the content
of the files via objects (e.g. dict), watching over the changes that could
appear in metrics.conf and logs.conf files and alerting the controller in
case of changes in these files.
"""
from configparser import ConfigParser
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from queue import Queue
from typing import Dict, List

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from config_manager import models

EVENTS_QUEUE: Queue = Queue()
MANDATORY_STARTUP_CONF_SECTIONS = ["General", "Vault", "SNAPI"]
STARTUP_CFG_SECTION_MAP = {
    "General": models.GeneralConfig,
    "Vault": models.VaultConfig,
    "SNAPI": models.SNAPIConfig,
    "LogsFileAdapter": models.FileAdapterConfig,
    "MetricsFileAdapter": models.FileAdapterConfig,
    "MQTTLogsAdapter": models.MQTTAdapterConfig,
    "MQTTMetricsAdapter": models.MQTTAdapterConfig,
    "FluentdAdapter": models.FluentdAdapterConfig,
    "PrometheusAdapter": models.PrometheusAdapterConfig,
    "MetricTransform": models.MetricTransformConfig,
    "OTELMetricsAdapter": models.OTELMetricsAdapterConfig,
    "AMQPSubscriber": models.AMQPSubscriberConfig,
    "NCAT": models.NCATConfig,
}


class EventType(Enum):
    """Enum used for sending different event types based on the
    changes in metrics.conf and logs.conf.
    """

    METRICS_CONFIG_CHANGED = auto()
    LOGS_CONFIG_CHANGED = auto()


class AMQPEvents(Enum):
    """Enum used for sending events based on the changes in the
    AMQPSubscriber instance to trigger the reconfiguring and restart of the
    instance.
    """
    AMQP_CONFIG_CHANGED = auto()


@dataclass
class ConfigStore:
    """ConfigStore is responsible for storing the read config
    files (e.g. startup.conf) as Python objects.
    """

    startup_config: Dict = field(default_factory=dict)
    metrics_config: List[str] = field(default_factory=list)
    logs_config: List[str] = field(default_factory=list)
    query_config: Dict = field(default_factory=dict)


class FileChangesHandler(FileSystemEventHandler):
    """Class responsible for event handling the changes that are
    captured by the watchdog. It overrides the `on_modified` method
    in order to watch only for the paths of logs.conf and metrics.conf.
    """

    def __init__(self, config_manager) -> None:
        self.config_manager = config_manager
        super().__init__()

    def on_modified(self, event):
        if event.src_path == str(self.config_manager.metrics_conf_path):
            EVENTS_QUEUE.put(EventType.METRICS_CONFIG_CHANGED)
            self.config_manager.store_metrics_conf()
            return
        if event.src_path == str(self.config_manager.logs_conf_path):
            EVENTS_QUEUE.put(EventType.LOGS_CONFIG_CHANGED)
            self.config_manager.store_logs_conf()


class ConfigManager:
    """This is the main Config Manager class which handles the
    following operations:
        * reading the configuration files
        * validating the configuration files
        * exposing the content of the configuration files as Python Objects
        * watching over the changes of logs.conf and metrics.conf
        * alerting the changes of any configuration
    """

    def __init__(self, config_store: ConfigStore, config_root: Path) -> None:
        self.config_store = config_store
        self.config_parser = ConfigParser()
        self.observer = Observer()
        self.config_files_root = config_root
        self.startup_conf_path = self.config_files_root.joinpath(
            "startup.conf")
        self.metrics_conf_path = self.config_files_root.joinpath(
            "metrics.conf")
        self.logs_conf_path = self.config_files_root.joinpath("logs.conf")
        self.query_conf_path = self.config_files_root.joinpath("query.conf")
        super().__init__()

    def read_file_txt(self, file_path: Path) -> List[str]:
        """Reads the content of basic text files.
        The method is used for reading metrics.conf and logs.conf files.
        """
        with open(file_path, "r", encoding="utf-8") as file:
            content = file.readlines()
            content = list(map(lambda x: x.strip("\n\r "), content))
        return content

    def read_file_cfg(self, file_path: Path) -> Dict:
        """Reads the configuration files using configparser library."""
        self.config_parser.clear()
        with file_path.open() as _file:
            self.config_parser.read_file(_file)
        return dict(self.config_parser)

    def store_startup_conf(self) -> None:
        """Stores the read startup.conf file in config store object
        after validating the file.
        Important: in case of errors while reading the startup.conf file
        or while validating it, the config_store object will store a default
        empty dictionary (if it's the first time calling the
        store_startup_conf method) or the last valid dictionary that was
        stored successfully.

        Raises:
            * pydantic.ValidationError - in case of invalid values provided
        """
        read_startup_cfg = self.read_file_cfg(self.startup_conf_path)
        for section, model in STARTUP_CFG_SECTION_MAP.items():
            conf_section_items = read_startup_cfg.get(section, {})
            self.config_store.startup_config[section] = model(
                **conf_section_items)

    def store_metrics_conf(self) -> None:
        """Stores the read metrics.conf file in config store object
        after validating the file.
        Important: in case of errors while reading the metrics.conf file
        or while validating it, the config_store object will store a default
        empty list (if it's the first time calling the
        store_metrics_conf method) or the last valid list of strings that was
        stored successfully.
        """
        self.config_store.metrics_config = self.read_file_txt(
            self.metrics_conf_path
        )

    def store_logs_conf(self) -> None:
        """Stores the read logs.conf file in config store object
        after validating the file.
        Important: in case of errors while reading the logs.conf file
        or while validating it, the config_store object will store a default
        empty list (if it's the first time calling the
        store_logs_conf method) or the last valid list of strings that was
        stored successfully.
        """
        self.config_store.logs_config = self.read_file_txt(
            self.logs_conf_path
        )

    def store_query_conf(self) -> None:
        """Stores the read query.conf file in config store object
        after validating the file.
        Important: in case of errors while reading the query.conf file
        or while validating it, the config_store object will store a default
        empty dictionary (if it's the first time calling the
        store_query_conf method) or the last valid dictionary that was
        stored successfully.

        Raises:
            * pydantic.ValidationError - in case of invalid values provided
        """
        read_query_cfg = self.read_file_cfg(self.query_conf_path)
        for section in read_query_cfg:
            if section.startswith("service_index"):
                self.config_store.query_config[section] = (
                    models.ServiceIndexConfig(
                        **read_query_cfg.get(section, {})))

    def watch_files(self) -> None:
        """Implements a file changes watcher using watchdog library."""
        self.observer.schedule(FileChangesHandler(self),
                               self.config_files_root)
        self.observer.start()

    def stop(self):
        """Stops the Observer thread."""
        self.observer.stop()
