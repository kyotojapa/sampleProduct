"""Core module for rl_controller."""

import time
from enum import Enum, auto
from queue import LifoQueue
from threading import Lock, Thread
from time import sleep
from typing import Dict, List, Union

import product_common_logging as logging
import requests
from pysnapi.auth import SNAPIBasicAuth, SNAPITokenAuth
from pysnapi.commands import OMT, OTA, Generic, Logging
from pysnapi.core import Connection
from pysnapi.exceptions import PySNAPIException

from config_manager.models import InputUnitType
from output_unit.metric_storage import MetricStore

JSONType = Union[List, Dict, str, int, float, bool, None]


class RLControllerEventType(Enum):
    """Connection event types."""

    UNREACHABLE = auto()
    SNAPI_ERROR = auto()
    REACHABLE = auto()


class RLController(Thread):
    """RazorLink controller class that handles SNAPI interactions and sessions.
    This class extends Thread in order to provide refresh mechanism in a
    separate thread.
    """

    def __init__(
        self,
        address: str,
        port: int,
        auth: Union[SNAPIBasicAuth, SNAPITokenAuth],
        is_sdep: bool,
        input_unit_type: InputUnitType,
        system_id: Union[str, None] = None,
        **kwargs,
    ) -> None:
        super().__init__()
        self.__keep_refreshing = True
        self.__connection = Connection(address, port, auth, **kwargs)
        self.__logging_cmd = Logging(self.__connection)
        self.__generic_cmd = Generic(self.__connection)
        self.__ota_cmd = OTA(self.__connection)
        self.__omt_cmd = OMT(self.__connection)
        self.__conn_lock = Lock()
        self._system_id = system_id
        self._ota_config = None
        self._status_data = None
        self.event_queue: LifoQueue = LifoQueue()
        self.is_sdep = is_sdep
        self.input_unit_type = input_unit_type

    def run(self) -> None:
        """This method is trigger by .start() method. It ensures that the
        current session is refreshed at constant periods of time so the
        logging session is not destroyed by RL.
        """
        while self.__keep_refreshing:
            start = time.time()
            try:
                logging.debug("Calling refresh...")
                with self.__conn_lock:
                    if self.input_unit_type == InputUnitType.TCP_LISTENER:
                        self.__generic_cmd.refresh_session()
                    self.system_id = None  # set the system_id
                    self.ota_config = None
                    self.status_data = None
                    full_response = self.__connection.latest_full_response
                    self.__store_snapi_metric(
                        value=int(full_response.elapsed.total_seconds() * 1000),
                        status_code=full_response.status_code,
                    )
            except PySNAPIException as err:
                logging.warning(err)
                with self.__conn_lock:
                    self.__store_snapi_metric(
                        value=err.resp.elapsed.total_seconds() * 1000,
                        status_code=err.resp.status_code,
                    )
                    logging.debug("Resetting the session auth token.")
                    self.__connection.reset_auth()
                self.event_queue.put(RLControllerEventType.SNAPI_ERROR)
            except requests.exceptions.RequestException as err:
                self.__store_snapi_metric(
                    value=(time.time() - start) * 1000,
                    status_code=503,
                )
                logging.warning("RazorLink unreachable")
                logging.debug(err)
                self.event_queue.put(RLControllerEventType.UNREACHABLE)
            else:
                logging.info("Session refreshed")
                self.event_queue.put(RLControllerEventType.REACHABLE)
            finally:
                wait_period = self.__connection.session_idle_time / 4
                logging.debug(f"Waiting {wait_period}" "seconds between refreshes.")
                sleep(wait_period)

    def open_logging_session(
        self, categories: List[str], dst_addr: str, dst_port: int, cleanup_tcp_connection: bool = False
    ) -> JSONType:
        """To make sure that you opened a session call this method twice. First
        you will get 200 then 503 if the TCP stream is running. If you get 200
        each time you call it, then that means that the TCP destination for
        logs is not available.

        Arguments:
            * `categories`: a list of logging categories to enable
            * `dst_addr`: the URL / IP address to send logs to
            * `dst_port`: the TCP port of destination for logs

        Returns:
            * JSONType => Union[List, Dict, str, int, float, bool, None]
            The data returned by SNAPI.

        Raises:
            * `pysnapi.exceptions.PySNAPIException`: if the response code is
            not successful
        """
        with self.__conn_lock:
            logging.warning("Resetting the session auth token.")
            self.__connection.reset_auth()
            logging.warning("Opening logging session.")
            if cleanup_tcp_connection:
                time.sleep(30) # workaround until load balancer issue fixed SDWAN-6626
                logging.warning("Waiting to close any past established sessions.")
            response = self.__logging_cmd.open_session_logging(
                categories, dst_addr, dst_port
            )
        return response

    def enable_bus_categories(self, categories: List[str]):
        with self.__conn_lock:
            logging.warning("Resetting the session auth token.")
            self.__connection.reset_auth()
            response = self.__logging_cmd.enable_bus_logger_categories(
                categories
            )
        return response

    def disable_bus_categories(self, categories: List[str]):
        with self.__conn_lock:
            logging.warning("Resetting the session auth token.")
            self.__connection.reset_auth()
            response = self.__logging_cmd.disable_bus_logger_categories(
                categories
            )
        return response

    def close_logging_session(self):
        """Close a logging session.

        Raises:
            * `pysnapi.exceptions.PySNAPIException`: if the response code is
            not successful"""
        with self.__conn_lock:
            response = self.__logging_cmd.close_session_logging()
        return response

    def enable_logging_categories(self, categories: List[str]) -> JSONType:
        """Enable logging categories for an existing session.
        If you got 503 that means that logging session is not started.

        Arguments:
            * `categories`: a list of logging categories to enable

        Returns:
            * JSONType => Union[List, Dict, str, int, float, bool, None]
            The data returned by SNAPI.

        Raises:
            * `pysnapi.exceptions.PySNAPIException`: if the response code is
            not successful
        """
        with self.__conn_lock:
            response = self.__logging_cmd.enable_session_log_categories(categories)
        return response

    def disable_logging_categories(self, categories: List[str]) -> JSONType:
        """Disable logging categories for an existing session.
        If you got 503 that means that logging session is not started.

        Arguments:
            * `categories`: a list of logging categories to disable
        Returns:
            * JSONType => Union[List, Dict, str, int, float, bool, None]
            The data returned by SNAPI.

        Raises:
            * `pysnapi.exceptions.PySNAPIException`: if the response code is
            not successful"""
        with self.__conn_lock:
            response = self.__logging_cmd.disable_session_log_categories(categories)
        return response

    def gracefully_stop(self):
        """Stop the refreshing thread."""
        self.__keep_refreshing = False

    @property
    def connection(self):
        """Gets the connection from rl_controller"""
        return self.__connection

    @property
    def system_id(self):
        """Get the system id for the assurance agent from OTA"""
        return self._system_id

    @system_id.setter
    def system_id(self, _):
        self._system_id = self.__ota_cmd.retrieve_ota_system_id(self.is_sdep)

    @property
    def ota_config(self):
        """Get the system id for the assurance agent from OTA"""
        return self._ota_config

    @ota_config.setter
    def ota_config(self, _):
        self._ota_config = self.__ota_cmd.retrieve_agent_config(
            "assurance-agent", self.is_sdep
        )

    @property
    def status_data(self):
        """Get the status for each tenant and their respective sites"""
        return self._status_data

    @status_data.setter
    def status_data(self, _):
        self._status_data = self.__omt_cmd.retrieve_tenant_status_details(
            not self.is_sdep)

    def __store_snapi_metric(self, value, status_code) -> None:
        """
        Method that stores a snapi metric
        """
        MetricStore().set_metric(
            name="services.hosting.virtual_machine.sdwan_end_point.agent.assurance.snapi_requests",
            value=value,
            attrs={"status_code": status_code},
            attributes_set=False,
            use_default_attributes=False,
        )
