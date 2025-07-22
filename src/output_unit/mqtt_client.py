"""A module that handles all aspects of connecting to an mqtt broker"""


import random
import string
import time
from enum import Enum, auto
from threading import Event, Thread
from typing import Any, Dict, Tuple

import paho.mqtt.client as mqtt  # type: ignore
import product_common_logging as logging
from paho.mqtt.packettypes import PacketTypes as packettypes  # type: ignore
from paho.mqtt.properties import Properties as properties  # type: ignore

from config_manager.models import MQTTAdapterConfig

MAX_MQTT_CONN_ATTEMPTS = 20
CONN_ATTEMPT_TIMEOUT = 5.0


class ConnectionStatus(str, Enum):
    """An Enumeration class for the mqtt client connection status"""

    CONNECTED = auto()
    DISCONNECTED = auto()


class MQTTClientCouldNotConnectException(Exception):
    """Exception for announcing that the mqtt client
    could not connect."""

    def __init__(self, mqtt_client: mqtt.Client, *args: object) -> None:
        self.mqtt_client = mqtt_client
        super().__init__(*args)


class MQTTClient():
    "A class for client to connect to mqtt brokers"

    def __init__(self, srv_name: str) -> None:
        """Class init method"""

        self._opts: Dict = {}
        self._srv_name = '-'.join([srv_name, ''.join(
            random.SystemRandom().choice(
                string.ascii_uppercase + string.digits) for _ in range(10))])
        self.mqtt_version = 3
        self.broker_port = 1883
        self.broker_address = "127.0.0.1"
        self.mqtt_client = mqtt.Client()
        self._transport_type = "tcp"
        self.disconnected_evt = Event()
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.keep_trying_to_connect = Event()
        self.connect_thread: Thread = Thread(target=callable)

    def configure(
            self, adapter_config: MQTTAdapterConfig,
            start_connect_thread: bool = True):
        """Method that configures the mqtt client. This can raise
        ValueError exceptions that need to be handled. It also
        enables a background thread for trying to connect to
        broker for the first time"""

        self.mqtt_version = adapter_config.mqtt_version

        self.broker_port = adapter_config.broker_port
        self.broker_address = adapter_config.broker_address
        self.keep_trying_to_connect.clear()

        self.mqtt_client, self._opts = self.__get_cl_and_opts()

        self.mqtt_client.on_connect = self.__on_connect
        self.mqtt_client.on_disconnect = self.__on_disconnect
        self.connection_status = ConnectionStatus.DISCONNECTED

        # start the connect for the first time thread. After connecting
        # once the mqtt client background thread will handle the connection
        self.connect_thread = Thread(
            target=self.__try_to_connect_the_first_time)
        self.connect_thread.daemon = True
        if start_connect_thread:
            self.connect_thread.start()

    def __try_to_connect_the_first_time(self):
        """Try to connect the first time until you succeed.
        After that the loop_start method will handle
        the message processing and reconnection"""

        connect_attempts = 0
        while not self.keep_trying_to_connect.is_set():
            try:
                if self.connection_status == ConnectionStatus.DISCONNECTED:
                    logging.info("Attempting to Connect")
                    self.__connect()

            except OSError as err:
                logging.error(f"Error in attempting to connect. {err}")
                connect_attempts += 1
                # if we failed to connect max times raise exception
                # for controller to handle
                if connect_attempts > MAX_MQTT_CONN_ATTEMPTS:
                    raise MQTTClientCouldNotConnectException(
                        self.mqtt_client) from err
            else:
                self.keep_trying_to_connect.set()

            time.sleep(CONN_ATTEMPT_TIMEOUT)

    def __get_v3_options(self) -> (Dict[str, Dict[str, Any]]):
        """Method that returns v3 client options"""

        return {
            "options": {
                "client_id": self._srv_name,
                "transport": self._transport_type,
                "protocol": mqtt.MQTTv311,
                "clean_session": True
            },
            "connect_options": {
                "port": self.broker_port,
                "keepalive": 60
            }
        }

    def __get_v5_options(self, props: properties) -> (
            Dict[str, Dict[str, Any]]):
        """Method that returns v5 client options"""

        return {
            "options": {
                "client_id": self._srv_name,
                "transport": self._transport_type,
                "protocol": mqtt.MQTTv5
            },
            "connect_options": {
                "port": self.broker_port,
                "clean_start": mqtt.MQTT_CLEAN_START_FIRST_ONLY,
                "properties": props,
                "keepalive": 60
            }
        }

    def __get_options(self) -> (Dict[str, Dict[str, Any]]):
        """Method that returns client options"""

        if self.mqtt_version == 5:
            props = properties(packettypes.CONNECT)
            props.SessionExpiryInterval = 30*60
            return self.__get_v5_options(props)

        return self.__get_v3_options()

    def __get_cl_and_opts(self) -> (Tuple[mqtt.Client, Dict[str, Any]]):
        """Method that gives us an mqtt client of specific version
        and connect options"""

        opts = self.__get_options()
        return (mqtt.Client(**opts['options']), opts['connect_options'])

    def __connect(self):
        """Method that attempts to connect to broker
        and starts the internal background thread in mqtt library
        that handles message processing and reconnections"""

        self.mqtt_client.connect(self.broker_address,
                                 **self._opts)
        self.disconnected_evt.clear()
        self.mqtt_client.loop_start()

    def __del__(self):
        """Method that disconnects from mqtt broker and stops the background
        thread responsible for message processing and reconnections"""

        self.keep_trying_to_connect.set()
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def __on_connect(self, client, user_data, flags, rc_res, props=None):
        """Callback method for connection events"""

        if rc_res == 0:
            logging.info(f"mqtt exporter client connected: {client}")
            self.connection_status = ConnectionStatus.CONNECTED

        else:
            logging.debug("Exporter Client status: Not connected "
                          f"User data: {user_data} "
                          f"Flags: {flags} "
                          f"Result Code: {rc_res} "
                          f"Properties: {props} ")

    def __on_disconnect(self, client, user_data, rc_res, props=None):
        """Callback method for disconnection events"""

        if rc_res != 0:
            logging.warning("MQTT exporter client disconnected unexpectedly. "
                            f"Client: {client} "
                            f"Result Code: {rc_res} "
                            f"User Data: {user_data} "
                            f"Props: {props} ")
        else:
            logging.info("mqtt exporter client disconnected")

        self.disconnected_evt.set()
        self.connection_status = ConnectionStatus.DISCONNECTED
