"""Module for the RLF controller class"""
import configparser
import hashlib
import json
import secrets
import sys
import time
import httpx
from functools import wraps
from pathlib import Path
from queue import Empty
from threading import Lock
from typing import Any, Callable, Dict, List, Union

from json_repair import repair_json
from httpx import Response

import product_common_logging as logging
import requests
from hvac.exceptions import InvalidPath, InvalidRequest
from productutils.vault_client_abc import ForbiddenException
from pydantic import ValidationError
from urllib3.exceptions import NewConnectionError, SSLError, HTTPError

from config_manager.config_manager import (EVENTS_QUEUE, AMQPEvents,
                                           ConfigManager, ConfigStore)
from config_manager.config_manager import EventType as ConfMgrEventType
from config_manager.models import (AgentConfig, AMQPSubscriberConfig,
                                   CorrelationConfig, InputUnitType, MetricTransformConfig,
                                   NCATConfig, SNAPIConfig)
from controller.common import (NotBooted, ReturnCodes, RLFCriticalException,
                               SecretNotReadException,
                               get_secret_based_on_vault_location)
from data_processing.data_processor import DataProcessor
from input_unit.input import InputUnit
from input_unit.shared_data import EventType as InputUnitEventType
from output_unit.metric_storage import MetricStore
from output_unit.output_unit import OutputUnit
from rl_controller import (PySNAPIException, RLController,
                           RLControllerEventType, SNAPITokenAuth)

VAULT_SNAPI_TOKEN_PATH: str = ("plain-text/prod/edge/fleet-edge/internal/"
                               "snapi/private/token")


class RLFController:
    """RazorLink Logs Forwarder Controller is the core that holds the business
    logic for handling configuration changes, re-connects, retries and also
    manages all other units.

    Attrs:
        * root_config (Path): directory that holds config files (startup.conf
        logs.conf, metrics.conf)
    """

    def __init__(self, root_config: Path) -> None:
        self.config_store = ConfigStore()
        self.config_manager = ConfigManager(
            self.config_store,
            root_config,
        )
        self.rl_controller: Union[RLController, None] = None
        self.input_unit: Union[InputUnit, None] = None
        self.output_unit: Union[OutputUnit, None] = None
        self.data_processor: Union[DataProcessor, None] = None

        self.prev_categories: List[str] = []
        self.should_reopen = True
        self.snapi_ownership = Lock()
        self.__boot_complete = False
        self.__rlc_handlers = {
            RLControllerEventType.UNREACHABLE: self.__handle_rl_lost,
            RLControllerEventType.SNAPI_ERROR: self.__handle_rl_fault,
            RLControllerEventType.REACHABLE: self.__handle_rl_reachable
        }
        self.metric_transform_config: MetricTransformConfig = (
            MetricTransformConfig())
        self.system_id = None
        self.last_system_id = None
        self.ota_config = None
        self.status_data: Dict = {}
        self.last_ota_config: Dict = {"exporters": {},
                                      "ncat": {},
                                      "instruments-config": {},
                                      "resources": {},
                                      "metrics-config": [],
                                      "logs-config": [],
                                      "amqp-subscriber-config": {}}
        self.irs_expected: set = {
            'ppp100', 'Ethernet_eth2_3917',
            'Ethernet_eth7_4033'}
        self.input_unit_type = None

    def shutdown(self):
        """Stop all threaded units."""
        if self.rl_controller is not None:
            self.rl_controller.gracefully_stop()

        if self.input_unit is not None:
            self.input_unit.stop()

        if self.output_unit is not None:
            self.output_unit.set_stop_event()

        if self.data_processor is not None:
            self.data_processor.stop()

    def __handle_rl_lost(self):
        """React when RL is unreachable."""
        logging.warning("Lost RL SNAPI connection.")
        self.input_unit.drop_all_clients()
        self.should_reopen = True

    def __handle_rl_fault(self):
        """React when SNAPI answers with error."""
        logging.warning("Received SNAPI Errors while refreshing")

    def __handle_rl_reachable(self):
        """Open a new logging session after RL is available again.
        This could cause infinite loop if no successful SNAPI calls for
        opening a new logging session."""
        if self.should_reopen:
            logging.info("RL SNAPI connection recovered.")
        self.safe_open_logging_session()

    def boot(self):
        """This method should configure all units. Beware that this method
        has the ability to exit the program."""
        logging.info("Booting RLF units.")
        self.__boot_config_manager()
        self.__warn_missing_categories()
        self.__boot_rl_controller(self.__retrieve_snapi_token_vault())
        self.__boot_output_unit()
        self.__boot_input_unit()
        self.__boot_data_processor()
        self.__boot_complete = True
        logging.info("RLF boot complete.")

    def set_otel_adapter_system_id(self):
        """Method for recreating the MeterPoint when system_id isn't available
        at boot time"""
        self.system_id = self.rl_controller.system_id
        if self.system_id and self.last_system_id != self.system_id:
            self.output_unit.register_adapters(
                system_id=self.system_id, only_otel=True)
            self.last_system_id = self.system_id

    @property
    def boot_complete(self) -> bool:
        """States if 'boot' method was called."""
        return self.__boot_complete

    def boot_guard(f):
        """Boot guard decorator is raising NotBooted exception if trying to
        call a function that is depended to boot and boot was not called."""
        @wraps(f)
        def inner(self, *args, **kwargs):
            if not self.boot_complete:
                raise NotBooted()
            return f(self, *args, **kwargs)
        return inner

    def __warn_missing_categories(self):
        if len(self.config_store.metrics_config) == 0:
            logging.warning("Empty list of metrics categories.")
        if len(self.config_store.logs_config) == 0:
            logging.warning("Empty list of logging categories.")

    def __boot_rl_controller(self, snapi_token: str) -> None:
        """Configure and start rl_controller."""
        logging.debug("Booting RL CONTROLLER...")
        snapi_cfg: SNAPIConfig = self.config_store.startup_config["SNAPI"]
        snapi_auth = SNAPITokenAuth(snapi_token)

        is_sdep = True if (self.config_store.startup_config["General"].
                           vault_location != "edge") else False

        input_unit_type = (self.config_store.startup_config["General"].
                           enabled_input_unit)

        self.rl_controller = RLController(
            snapi_cfg.address,
            snapi_cfg.port,
            snapi_auth,
            is_sdep,
            input_unit_type,
            system_id=self.system_id
        )
        self.rl_controller.name = self.rl_controller.__class__.__name__
        self.rl_controller.daemon = True
        self.rl_controller.start()
        self.system_id = self.rl_controller.system_id
        logging.debug("RL CONTROLLER boot complete.")

    def __boot_data_processor(self):
        logging.debug("Booting DATA PROCESSOR...")
        self.metric_transform_config: MetricTransformConfig = (
            self.config_store.startup_config['MetricTransform'])
        self.data_processor = DataProcessor(self.input_unit, self.output_unit,
                                            self.metric_transform_config,
                                            self.__get_correlation_config())
        self.data_processor.start()
        logging.debug("DATA PROCESSOR boot complete.")

    def __get_correlation_config(self):

        location = "seep" if self.config_store.startup_config[
                "General"].vault_location == "edge" else "sdep"
        hold_off = self.config_store.startup_config[
            "General"].ctx_release_hold_off_period
        start_at = self.config_store.startup_config[
            "General"].ctx_release_start_at
        max = self.config_store.startup_config[
            "General"].ctx_release_max_setting
        exp = self.config_store.startup_config[
            "General"].ctx_release_exp_factor
        back_off_enabled = self.config_store.startup_config[
            "General"].ctx_release_exp_back_off_enabled

        ss_enable = self.config_store.startup_config[
            "General"].sub_sampler_enable
        ss_period = self.config_store.startup_config[
            "General"].sub_sampling_period
        ss_cleanup_period = self.config_store.startup_config[
            "General"].sub_sampler_cleanup_period

        # Getting service_index (pod number) from startup.conf
        service_index = self.config_store.startup_config.get(
            "General").service_index
        # Getting disabled_queries field under a [service_index] section from
        # query.conf based on the pod number
        service_index_cfg = self.config_store.query_config.get(
            f"service_index-{service_index}")
        disabled_queries = service_index_cfg.disabled_queries

        return CorrelationConfig(
            location=location,
            connection=self.rl_controller.connection,
            ctx_release_exp_back_off_enabled=back_off_enabled,
            ctx_release_hold_off_period=hold_off,
            ctx_release_start_at=start_at,
            ctx_release_max_setting=max,
            ctx_release_exp_factor=exp,
            sub_sampler_enable=ss_enable,
            sub_sampling_period=ss_period,
            sub_sampler_cleanup_period=ss_cleanup_period,
            disabled_queries=disabled_queries)

    def __boot_input_unit(self) -> None:
        """Configure and start input_unit."""
        logging.debug("Booting INPUT UNIT...")
        startup_config = self.config_store.startup_config
        self.input_unit = InputUnit(startup_config)
        self.input_unit_type = startup_config["General"].enabled_input_unit
        logging.debug(
            "Input Unit created using following config: %s",
            self.config_store.startup_config)
        try:
            self.input_unit.start()
        except OSError as err:
            logging.error(err)
            sys.exit(ReturnCodes.SOCKET_ERRORS)
        else:
            logging.debug("INPUT UNIT boot complete.")

    def __boot_output_unit(self):
        """Configure output unit with its adapters and run its threads."""
        logging.debug("Booting OUTPUT UNIT....")
        try:
            self.output_unit = OutputUnit(
                self.config_store.startup_config,
                self.config_store.startup_config["General"].queue_max_items_logs,  # noqa: E501
                self.config_store.startup_config["General"].queue_max_items_metrics  # noqa: E501
            )
        except TypeError as err:
            logging.error(err)
            sys.exit(ReturnCodes.INVALID_CONFIG)

        unconfigured_adapters = self.output_unit.register_adapters(
            system_id=self.system_id)
        if len(unconfigured_adapters) > 0:
            logging.warning(f"Unconfigured adapters: {unconfigured_adapters}")

        logging.info(
            "Output unit will use following adapters: %s",
            self.config_store.startup_config["General"].enabled_adapters
        )

        self.output_unit.run_threads()
        logging.debug("OUTPUT UNIT boot complete.")

    def __boot_config_manager(self):
        """Start config_manager. This will populate config_store with initial
        configs."""
        try:
            logging.debug("Booting CONFIG MANAGER...")
            logging.debug("Loading configs...")
            self.config_manager.store_startup_conf()
            self.config_manager.store_logs_conf()
            self.config_manager.store_metrics_conf()
            self.config_manager.store_query_conf()
        except OSError as err:
            logging.error(
                "Config directory does not contain required files. %s",
                err
            )
            sys.exit(ReturnCodes.MISSING_FILES)
        except configparser.Error as err:
            logging.error("Startup config parsing error. %s", err)
            sys.exit(ReturnCodes.INVALID_CONFIG)
        except ValidationError as err:
            logging.error("Invalid values in startup config. %s", err)
            sys.exit(ReturnCodes.INVALID_CONFIG)
        else:
            logging.debug("Configs loaded.")
            logging.debug("Startup config: %s",
                          self.config_store.startup_config)
            logging.debug("Metrics config: %s",
                          self.config_store.metrics_config)
            logging.debug("Logs config: %s", self.config_store.logs_config)

            merged_categories = list(
                set(self.config_store.logs_config +
                    self.config_store.metrics_config)
            )
            self.prev_categories = merged_categories
            logging.debug("Starting file watcher....")
            self.config_manager.watch_files()
            logging.debug("File watcher started.")

    def __retrieve_snapi_token_vault(self) -> str:
        logging.info("Retrieving SNAPI token...")
        try:
            snapi_token = get_secret_based_on_vault_location(
                self.config_store.startup_config["Vault"],
                self.config_store.startup_config["General"])
        except (FileNotFoundError, ForbiddenException, InvalidPath,
                InvalidRequest, NewConnectionError, SecretNotReadException,
                SSLError) as acc_exc:
            logging.error("Failed to read SNAPI token from Edge Vault"
                          f" with Exception - {acc_exc} ")
            sys.exit(ReturnCodes.VAULT_ERRORS)
        else:
            logging.info("SNAPI Token retrieved from Edge Vault successfully")
            return snapi_token

    def __confirm_connection(self, retries=3, timeout=2):
        """Contact input_unit and check if it has accepted an
        input type connection."""
        client_connected = False
        retry_count = 0
        while not client_connected and retry_count < retries:
            try:
                event = self.input_unit.events_queue.get(timeout=timeout)
            except Empty:
                retry_count += 1
            else:
                retry_count += 1
                if event is InputUnitEventType.ACCEPTED_CLIENT_CONNECTION:
                    client_connected = True

        return client_connected

    @boot_guard  # type: ignore
    def handle_rl_connection_closed(self):
        """Checks if RL closed TCP active TCP connection, if yes it will
        try to open a new logging session."""
        try:
            event = self.input_unit.events_queue.get(block=False)
        except Empty:
            pass
        else:
            if event == InputUnitEventType.CLIENT_CLOSED_CONNECTION:
                logging.error("Received CLIENT_CLOSED_CONNECTION event to " 
                              "close connection.")
            elif event == InputUnitEventType.CLIENT_TIMED_OUT_CONNECTION:
                logging.error("Received CLIENT_TIMED_OUT_CONNECTION event "
                              "to close connection.")
            if event in [InputUnitEventType.CLIENT_CLOSED_CONNECTION,
                         InputUnitEventType.CLIENT_TIMED_OUT_CONNECTION]:
                try:
                    if self.rl_controller is not None:
                        self.rl_controller.close_logging_session()
                        logging.warning("Initiating close active TCP "
                                        "connection to RLF.")
                except (PySNAPIException, NewConnectionError, HTTPError) as err:
                    logging.error("Failed to close active TCP connection "
                                  "to RLF. %s", err)
                finally:
                    self.should_reopen = True
                    self.safe_open_logging_session()

    @boot_guard  # type: ignore
    def handle_config_change(self):
        """Check config managers's events queue and react by setting new
        categories using latest categories strings."""
        try:
            event = EVENTS_QUEUE.get(block=False)
        except Empty:
            pass
        else:
            if event in list(ConfMgrEventType):
                self.__set_new_categories()
            else:
                EVENTS_QUEUE.put(event)

    def compare_ota_config(self,
                           previous_config: Any,
                           current_config: Any) -> bool:
        """Method for checking if OTA configuration changed between updates
        Args:
            * previous_config (json) previous agent config received from OTA.
            * current_config (json) current agent config received from OTA.
        """
        previous_config_sha = hashlib.sha256(json.dumps(
            previous_config).encode('utf-8')).digest()
        current_config_sha = hashlib.sha256(json.dumps(
            current_config).encode('utf-8')).digest()

        return secrets.compare_digest(previous_config_sha, current_config_sha)

    def set_vnf_state_data(self, ncat_config: NCATConfig):
        """Method used to query bgp status, ping status, interface status"""
        try:
            if not self.rl_controller.is_sdep:  # type: ignore
                requests.get(
                    f"http://{ncat_config.address}:{ncat_config.port}/state.html",
                    timeout=ncat_config.timeout,
                    headers={
                        'Host': '{ncat_config.address}:{ncat_config.port}',
                        # 'User-Agent': urllib3.util.SKIP_HEADER,
                        # 'Accept-Encoding': urllib3.util.SKIP_HEADER,
                        'Accept': '*/*',
                        'Connection': 'close'
                    }, hooks={'response': self.__process_vnf_state_data}
                )
        except (AttributeError, requests.exceptions.RequestException) as exc:
            logging.error(exc)

    def __process_vnf_state_data(self, response, *args, **kwargs):
        try:
            if response.status_code == 200:
                nc_data_str = repair_json(response.text)
                if nc_data_str:
                    nc_data = json.loads(nc_data_str)
                    ex_ifs = self.config_store.startup_config[
                        "OTELMetricsAdapter"].excluded_dig_interfaces
                    ex_vrfs = self.config_store.startup_config[
                        "OTELMetricsAdapter"].excluded_vrfs_from_bgp
                    MetricStore().set_status_based_on_netcat_updates(
                            self.rl_controller.is_sdep, nc_data=nc_data,  # type: ignore
                            excluded_ifs=ex_ifs, excluded_vrfs=ex_vrfs)
        except Exception as exc:
            logging.error(exc)

    async def __process_vnf_state_data_async(
            self, response: httpx.Response, *args, **kwargs):
        try:
            if response.status_code == 200:
                nc_data_str = repair_json(response.text)
                if nc_data_str:
                    nc_data = json.loads(nc_data_str)  # type: ignore
                    ex_ifs = self.config_store.startup_config[
                        "OTELMetricsAdapter"].excluded_dig_interfaces
                    ex_vrfs = self.config_store.startup_config[
                        "OTELMetricsAdapter"].excluded_vrfs_from_bgp
                    MetricStore().set_status_based_on_netcat_updates(
                            self.rl_controller.is_sdep, nc_data=nc_data,  # type: ignore
                            excluded_ifs=ex_ifs, excluded_vrfs=ex_vrfs)
        except Exception as exc:
            logging.error(exc)

    async def set_vnf_state_data_async(self, ncat_config: NCATConfig):
        """Method used to query bgp status, ping status, interface status"""
        if not self.rl_controller.is_sdep:  # type: ignore
            url = f"http://{ncat_config.address}:{ncat_config.port}/state.html"
            headers = {
                    'Host': '{ncat_config.address}:{ncat_config.port}',
                    'Accept': '*/*',
                    'Connection': 'close'
            }
            async with httpx.AsyncClient(timeout=httpx.Timeout(
                    ncat_config.timeout)) as client:
                try:
                    response = await client.get(url, headers=headers)
                    await self.__process_vnf_state_data_async(response)
                except Exception as e:
                    logging.error(e)

    def set_status_data(self):
        """Method that updates the status data of the status metrics
        that have been retrieved by querying SNAPI
        """
        try:
            self.status_data: Dict = self.rl_controller.status_data
            if self.status_data:
                t_status = self.__get_tenant_status(self.status_data["tenancy"])
                ir_status = self.__get_ir_status(self.status_data["irs"])
                ctx_data = self.__get_ctx_map(self.status_data["ctx_maps"],
                                              self.rl_controller.is_sdep)
                # self.__print_status_from_snapi(
                #    self.status_data, t_status, ir_status, ctx_data)
                MetricStore().set_status_metric_data(
                    ctx_data, ir_status, self.rl_controller.is_sdep)
                MetricStore().set_can_calculate_status(t_status)
                sdp_id = (self.status_data["sdp_id"] if "sdp_id"
                          in self.status_data else "")
                MetricStore().set_status_if_not_receiving_status_updates(
                    t_status, ir_status, self.rl_controller.is_sdep, sdp_id)
        except Exception as exc:
            logging.error(f"Error in processing tenancy data: {exc}")

    def __print_status_from_snapi(
            self, status_data, t_status, ir_status, ctx_data):
        print("-------------SNAPI------------------\n"
              f"{status_data=}\n"
              f"{t_status=}\n"
              f"{ir_status=}\n"
              f"{ctx_data=}\n"
              "------------------------------------")

    def __print_nc_data(self, nc_data):
        print("-------------NCDATA------------------\n"
              f"{nc_data=}\n"
              "------------------------------------")

    def __get_tenant_status(self, tenancy_data):
        t_status = {}
        for tenant, status in tenancy_data.items():
            t_status[tenant] = 0
            if status["is_seep"]:
                if (status["ready"] and not status["holdoff_state"] and
                        status["started"] and status["current_field"]):
                    t_status[tenant] = self.__get_sites_status(status)
            else:
                if status["ready"] and status["started"]:
                    t_status[tenant] = 1
        return t_status

    def __get_ir_status(self, ir_data):
        ir_status = {ir: (1 if status["active"] else 0)
                     for ir, status in ir_data.items()}
        irs_seen = set(ir_status.keys())
        self.irs_expected = self.irs_expected.union(irs_seen)
        if self.irs_expected != irs_seen:
            diff = self.irs_expected.difference(irs_seen)
            for k in diff:
                ir_status[k] = 0
        return ir_status

    def __get_ctx_map(self, ctx_data, is_sdep):
        if not is_sdep:
            prim_ctx = ""
            max_t_count = 0
            for ctx, map_data in ctx_data.items():
                if map_data["tenants_count"] > max_t_count:
                    max_t_count = map_data["tenants_count"]
                    prim_ctx = ctx

            return {ctx: {"sdp": map_data["sdp_id"],
                          "tenants": map_data["tenants"],
                          "prim_ctx": (True if prim_ctx and ctx == prim_ctx
                                       else False)}
                    for ctx, map_data in ctx_data.items()}
        else:
            return {ctx: {'sdp': map_data["sdp_id"]}
                    for ctx, map_data in ctx_data.items()}

    def __get_sites_status(self, status):
        sites_started = 1
        if "sites" in status:
            for k in status["sites"].keys():
                if (not status["sites"][k]["started"] or
                        not status["sites"][k]["ready"]):
                    sites_started = 0
        return sites_started

    def handle_ota_config_change(self):
        """Method for handling config changes received from OTA"""
        logging.info("Get OTA config")
        if self.rl_controller.ota_config:
            self.ota_config = self.rl_controller.ota_config.get("otel-config")
            try:
                AgentConfig(**self.ota_config)
            except (ValidationError, TypeError) as exc:
                logging.info(f"Invalid or missing agent config in OTA: {exc}")
                return

        if self.ota_config and not self.compare_ota_config(
                self.last_ota_config, self.ota_config):
            logging.info("Applying new configuration from OTA")
            self.__handle_otel_config_change()
            self.__handle_ncat_config_change()

            if self.input_unit_type == InputUnitType.TCP_LISTENER:
                self.__handle_ota_metrics_config_change()
                self.__handle_ota_logs_config_change()
            if self.input_unit_type == InputUnitType.AMQP_SUBSCRIBER:
                self.__handle_ota_amqp_config_change()
            self.last_ota_config = self.ota_config.copy()

    def __handle_ncat_config_change(self):
        logging.info("Check for differences in ncat configuration from OTA")

        try:
            if "ncat" in self.ota_config:
                if (not self.compare_ota_config(
                        self.last_ota_config.get("ncat"),
                        self.ota_config.get("ncat"))):
                    logging.info("Applying new ncat configuration from OTA")
                    current_ncat_cfg = self.ota_config.get("ncat")
                    exc_interfaces = current_ncat_cfg.get(
                        "excluded_dig_interfaces")
                    self.config_store.startup_config[
                            "OTELMetricsAdapter"].excluded_dig_interfaces = (
                                exc_interfaces)
                    exc_vrfs = current_ncat_cfg.get(
                        "excluded_vrfs_from_bgp")
                    self.config_store.startup_config[
                            "OTELMetricsAdapter"].excluded_vrfs_from_bgp = (
                                exc_vrfs)
        except (ValueError, KeyError, AttributeError) as err:
            logging.error(f"Could not process ncat config: {err}")

    def __handle_otel_config_change(self):
        """Method for handling assurance agent changes received from OTA"""
        logging.info("Check for differences in agent configuration from OTA")
        if (
            not self.compare_ota_config(
                self.last_ota_config.get("exporters"),
                self.ota_config.get("exporters"),
            )
            or not self.compare_ota_config(
                self.last_ota_config.get("instruments-config"),
                self.ota_config.get("instruments-config"),
            )
            or not self.compare_ota_config(
                self.last_ota_config.get(
                    "resources"), self.ota_config.get("resources")
            )
        ):
            logging.info("Applying new agent configuration from OTA")
            self.output_unit.register_adapters(
                exporters_config=self.ota_config.get("exporters").copy(),
                instruments_config=self.ota_config.get(
                    "instruments-config").copy(),
                resources=self.ota_config.get("resources").copy(),
                only_otel=True)
            logging.info("Agent configuration from OTA applied")

    def __handle_ota_metrics_config_change(self):
        """Method for handling metrics config changes received from OTA"""
        logging.info("Check for differences in metrics configuration from OTA")
        if (set(self.last_ota_config.get("metrics-config")) !=
                set(self.ota_config.get("metrics-config"))):
            EVENTS_QUEUE.put(ConfMgrEventType.METRICS_CONFIG_CHANGED)
            logging.info("Applying new metrics configuration from OTA")
            self.config_manager.config_store.metrics_config = (
                self.ota_config.get("metrics-config"))
            logging.info("Metrics configuration from OTA applied")

    def __handle_ota_logs_config_change(self):
        """Method for handling logs config changes received from OTA"""
        logging.info("Check for differences in logs configuration from OTA")
        if (set(self.last_ota_config.get("logs-config")) !=
                set(self.ota_config.get("logs-config"))):
            EVENTS_QUEUE.put(ConfMgrEventType.LOGS_CONFIG_CHANGED)
            logging.info("Applying new logs configuration from OTA")
            self.config_manager.config_store.logs_config = (
                self.ota_config.get("logs-config"))
            logging.info("Logs configuration from OTA applied")

    def __handle_ota_amqp_config_change(self):
        """Method for handling AMQP Subscriber changes received from OTA"""
        logging.info("Check for differences in agent configuration "
                     "(amqp-subscriber-config section) from OTA")
        if (not self.compare_ota_config(
            self.last_ota_config.get("amqp-subscriber-config"),
                self.ota_config.get("amqp-subscriber-config"))):
            logging.info("Applying new AMQP Subscriber configuration from "
                         "OTA")
            self.input_unit.amqp_subscriber.amqp_config = self.input_unit.amqp_subscriber.amqp_config.copy(
                update=self.ota_config.get("amqp-subscriber-config")
            )
            EVENTS_QUEUE.put(AMQPEvents.AMQP_CONFIG_CHANGED)
            logging.info("Agent configuration from OTA applied")

    @boot_guard  # type: ignore
    def handle_availability_change(self):
        """Handle RL Controller events."""
        try:
            event = self.rl_controller.event_queue.get(block=False)
        except Empty:
            pass
        else:
            handler = self.__rlc_handlers[event]
            handler()

    @boot_guard  # type: ignore
    def safe_open_logging_session(self):
        """Thread safe open a logging session if necessary. The necessity is
        controlled by `self.should_reopen` attribute. After successfully opened
        it will wait to have the confirmation of a client being connected from
        input_unit perspective."""
        with self.snapi_ownership:
            if not self.should_reopen:
                return True
              
            if self.input_unit_type == InputUnitType.TCP_LISTENER:
                address = self.config_store.startup_config["General"].listening_address
                port = self.config_store.startup_config["General"].listening_port
                cleanup_tcp_connection = self.config_store.startup_config["General"].cleanup_existing_tcp_connection
                logging.info("Starting to reset/open logging session to %s:%s",
                         address, port)
                self.__retry_snapi_call(
                    lambda: self.rl_controller.open_logging_session(
                        list(self.prev_categories),
                        address,
                        port,
                        cleanup_tcp_connection
                    )
                )
                logging.info("Logging session open request accepted.")
            elif self.input_unit_type == InputUnitType.AMQP_SUBSCRIBER:
                logging.info("Enabling bus categories")
                self.__retry_snapi_call(
                    lambda: self.rl_controller.enable_bus_categories(
                        list(self.prev_categories)
                    )
                )

            logging.info("Waiting for session establishment...")
            client_connected = self.__confirm_connection()

            if client_connected:
                logging.info("Session established.")
                self.should_reopen = False
                with self.rl_controller.event_queue.mutex:
                    self.rl_controller.event_queue.queue.clear()
            else:
                logging.error("Failed to establish a logging session.")
                self.should_reopen = True

            return client_connected

    def __set_new_categories(self):
        new_categories = set(
            self.config_store.logs_config + self.config_store.metrics_config
        )
        to_remove = list(set(self.prev_categories).difference(new_categories))
        to_add = list(new_categories.difference(self.prev_categories))

        self.prev_categories = list(new_categories)

        if len(to_remove) > 0:
            logging.info("Removing logging categories %s", to_remove)
            if self.input_unit_type == InputUnitType.TCP_LISTENER:
                self.__retry_snapi_call(
                    lambda: self.rl_controller.disable_logging_categories(
                        to_remove),
                    retries=5, interval=1
                )
            elif self.input_unit_type == InputUnitType.AMQP_SUBSCRIBER:
                self.__retry_snapi_call(
                    lambda: self.rl_controller.disable_bus_categories(
                        to_remove
                    ), retries=5, interval=1
                )

        if len(to_add) > 0:
            logging.info("Adding logging categories: %s", to_add)
            if self.input_unit_type == InputUnitType.TCP_LISTENER:
                self.__retry_snapi_call(
                    lambda: self.rl_controller.enable_logging_categories(to_add),
                    retries=5, interval=1
                )
            elif self.input_unit_type == InputUnitType.AMQP_SUBSCRIBER:
                self.__retry_snapi_call(
                    lambda: self.rl_controller.enable_bus_categories(to_add),
                    retries=5, interval=1
                )

    def __retry_snapi_call(self, func: Callable, retries: int = -1,
                           interval: float = 1.0):
        """Retry SNAPI calls for an unlimited number of
        times at one second interval.
        Args:
            * func (Callable) HTTP call function to be retried.
            * retries (int) number of retries, by default this will run
            endlessly.
            * interval (float) number of seconds to wait between retries
        """
        while retries != 0:
            start = time.time()
            try:
                return func()
            except requests.exceptions.RequestException as err:
                MetricStore().set_metric(
                    name="services.hosting.virtual_machine.sdwan_end_point."
                         "agent.assurance.snapi_requests",
                    value=(time.time() - start) * 1000,
                    attrs={"status_code": 503},
                    attributes_set=False,
                    use_default_attributes=False,

                )
                logging.error("SNAPI unreachable. %s", err)
                logging.info("Retrying....")
                time.sleep(interval)
            except PySNAPIException as err:
                MetricStore().set_metric(
                    name="services.hosting.virtual_machine.sdwan_end_point."
                         "agent.assurance.snapi_requests",
                    value=int(err.resp.elapsed.total_seconds() * 1000),
                    attrs={"status_code": err.resp.status_code},
                    attributes_set=False,
                    use_default_attributes=False,
                )
                logging.error("Non-success SNAPI response received. %s",
                              err.resp)
                if err.resp.status_code >= 400 and err.resp.status_code < 500:
                    logging.info("SNAPI Error 4XX when contacting server. "
                                 "Quitting retries")
                    raise RLFCriticalException() from err
                elif err.resp.status_code >= 500:
                    retries = -1
                    logging.info("SNAPI Error 5XX. Retrying again since issue "
                                 "may resolve with server %s", err)
                    time.sleep(interval)
            finally:
                retries -= 1
