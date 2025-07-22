"""This is the entrypoint of RazorLink Logs and Metrics forwarder.
This will use an event loop to constantly check for config and socket changes
to apply the proper actions.
"""
import datetime
import os
import sys
import time
import asyncio
from pathlib import Path
from typing import Dict

import product_common_logging as logging

from config_manager.models import GeneralConfig, NCATConfig, ServiceIndexConfig
from controller.common import ReturnCodes, RLFCriticalException
from controller.controller import RLFController


DEBUG = os.getenv("RLF_DEBUG", False)
logging.setLevel(logging.INFO)

if DEBUG:
    logging.setLevel(logging.DEBUG)


async def handle_scheduled_tasks(
        rlf_controller: RLFController,
        service_index_config: ServiceIndexConfig,
        previous_times: Dict[str, datetime.datetime]) -> None:
    """Handles scheduled tasks based on configured intervals in query.conf."""
    current_time = datetime.datetime.now(datetime.timezone.utc)

    tasks = [
        ("handle_ota_config_change", service_index_config.handle_ota_config_change_interval, "previous_time_ota"),  # noqa: E501
        ("set_status_data", service_index_config.set_status_data_interval, "previous_time_tenancy"),  # noqa: E501
        ("set_vnf_state_data_async", service_index_config.set_vnf_state_data_interval, "previous_time_vnf_state"),  # noqa: E501
    ]

    for method_name, interval, time_key in tasks:
        if (interval and (current_time - previous_times[time_key]) >=
                datetime.timedelta(seconds=interval)):
            previous_times[time_key] = current_time
            if method_name == "set_vnf_state_data_async":
                set_vnf_func = getattr(rlf_controller, method_name)
                await set_vnf_func(
                    rlf_controller.config_store.startup_config.get(
                        "NCAT", NCATConfig()))
            else:
                getattr(rlf_controller, method_name)()


async def main():

    root_config = Path(os.getenv("RLF_CONFIG", "/opt/rlf"))
    connected = False

    rlf = RLFController(root_config)
    time.sleep(30)  # workaround until load balancer issue fixed SDWAN-6626
    rlf.boot()
    general_cfg: GeneralConfig = rlf.config_store.startup_config.get(
        "General", GeneralConfig())
    """service_index indicates pod number and it's part of a GeneralConfig as
    it's provided as an environment variable"""
    service_index = general_cfg.service_index
    """getting the scheduling periods under the corresponding section
    (given the service_index number) in query.conf"""
    service_index_cfg: ServiceIndexConfig = rlf.config_store.query_config.get(
        f"service_index-{service_index}", ServiceIndexConfig())

    while not connected:
        try:
            connected = rlf.safe_open_logging_session()
            time.sleep(10)
        except (RLFCriticalException, ValueError) as err:
            logging.error(err)
            rlf.shutdown()
            sys.exit(ReturnCodes.GENERIC_OTHER_ERRORS)
        finally:
            logging.info(f"Connection Status: {connected}")

    if connected:
        logging.info("Connection successful.")
        previous_times = {
            "previous_time_ota": datetime.datetime.min.replace(
                tzinfo=datetime.timezone.utc),
            "previous_time_tenancy": datetime.datetime.min.replace(
                tzinfo=datetime.timezone.utc),
            "previous_time_vnf_state": datetime.datetime.min.replace(
                tzinfo=datetime.timezone.utc),
        }

        while True:
            try:
                rlf.handle_config_change()
                await handle_scheduled_tasks(
                    rlf, service_index_cfg, previous_times)
                rlf.handle_rl_connection_closed()
                rlf.handle_availability_change()
            except Exception as err:
                logging.error(err)
            finally:
                time.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
