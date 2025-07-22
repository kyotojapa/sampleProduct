import asyncio
import json
from queue import Empty
from threading import Thread
from typing import Dict

import aio_pika
import product_common_logging as logging
from aiormq import ProbableAuthenticationError
from tenacity import before_log, retry, retry_if_exception_type, wait_fixed

from config_manager.config_manager import EVENTS_QUEUE, AMQPEvents
from input_unit.input_data_store import InputDataStore
from input_unit.shared_data import EventType, MessageType, get_events_queue


class AMQPSubscriber(Thread):
    """
    Class that sets up a connection to a RabbitMQ server, listens for
    incoming messages and sends them further to the InputDataStore.
    """

    def __init__(self, input_unit, data_store: InputDataStore):
        super().__init__()
        self.input_unit = input_unit
        self.data_store: InputDataStore = data_store
        self.events_queue = get_events_queue()

        self.amqp_config = self.input_unit.amqp_subscriber_cfg
        self.amqp_url = self.__get_amqp_url()
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.running = None

    def __get_amqp_url(self):
        """
        Creates the RabbitMQ connection URL based on the fields stored in the
        [AMQPSubscriber] category from startup.conf.
        """
        amqp_host = self.amqp_config.amqp_host
        amqp_port = self.amqp_config.amqp_port
        amqp_user = self.amqp_config.amqp_user
        amqp_pass = self.amqp_config.amqp_pass
        amqp_vhost = self.amqp_config.amqp_vhost

        return (f"amqp://{amqp_user}:{amqp_pass}@{amqp_host}:{amqp_port}"
                f"{amqp_vhost}")

    async def connect(self):
        """
        Connects to RabbitMQ and sets up the channel, queue and bindings based
        on the routing keys.
        """
        logging.info("Connecting to RabbitMQ.")
        self.amqp_url = self.__get_amqp_url()

        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()

            # service_index placeholder is not mandatory
            # if missing it will return the value of self.amqp_config.amqp_exchange
            exchange_name = self.amqp_config.amqp_exchange.format(
                service_index=self.input_unit.general_cfg.service_index)

            self.exchange = await self.channel.declare_exchange(
                exchange_name, aio_pika.ExchangeType.TOPIC,
                internal=True, passive=True)

            if self.input_unit.general_cfg.vault_location != "edge":
                queue_name = self.amqp_config.amqp_queue_name.format(
                    service_index=self.input_unit.general_cfg.service_index)
                self.queue = await self.channel.declare_queue(
                    queue_name, arguments={"x-max-length": self.amqp_config.amqp_queue_max_size}
                )
                routing_keys = [""]
            else:
                self.queue = await self.channel.declare_queue(
                    self.amqp_config.amqp_queue_name, passive=True
                )
                routing_keys = (self.amqp_config.amqp_logs_routing_keys +
                                self.amqp_config.amqp_metrics_routing_keys)

            for routing_key in routing_keys:
                await self.queue.bind(self.exchange, routing_key)
        except ProbableAuthenticationError:
            logging.error("Authentication error occurred", exc_info=True)
            self.events_queue.put(EventType.ACCEPTED_CLIENT_CONNECTION)
            raise

    def get_message_type(self, message: Dict) -> MessageType:
        """
        Checks if incoming message from RabbitMQ (metrics in particular) has
        the `snapshot` key.
        """
        if hasattr(message, "get"):
            if "snapshot" in message:
                return MessageType.METRIC

            return MessageType.LOG

        return MessageType.UNKNOWN

    async def start_consuming(self):
        """
        Starts consuming messages from the RabbitMQ queue using an iterator.
        Processes the messages and sends them further to the InputDataStore.
        """
        try:
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.running:
                        logging.info("Stopped consuming messages from "
                                     f"{self.queue.name}!")
                        break

                    async with message.process():
                        try:
                            raw_message = message.body.decode()
                            json_message = json.loads(raw_message)
                            detail_section = json_message["Detail"]
                        except (json.JSONDecodeError, KeyError) as exc:
                            logging.warning(f"Invalid message {raw_message} "
                                            "when trying to convert to json."
                                            f"{exc}")
                        except ValueError as va:
                            logging.error(f"Value Error has occured: {va} "
                                          f"for message body: {message.body}")
                        else:
                            message_type = self.get_message_type(
                                detail_section)
                            logging.debug(f"Received {message_type.name} "
                                          f"message: {raw_message}")
                            self.data_store.put_message(message_type,
                                                        detail_section)

        except asyncio.CancelledError:
            logging.warning("The message consumption on AMQPSubscriber "
                            "stopped!")

    async def run_async(self):
        """
        Main method to run the subscriber: connects and starts consuming.
        This method is designed to be run in an asyncio event loop.
        """
        logging.info("Starting async AMQPSubscriber...")
        await self.connect()
        await self.confirm_connection()
        self.running = True

        logging.info(f"Waiting for messages in {self.queue.name}.")
        await self.start_consuming()

    async def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection:
            await self.connection.close()

    def stop(self):
        """
        Calls close method to stop the connection to RabbitMQ.
        """
        logging.info("Stopping AMQPSubscriber...")
        self.running = False
        asyncio.create_task(self.close())

    async def restart(self):
        """
        Restarts the AMQPSubscriber after OTA configuration update.
        """
        logging.info("Restarting AMQPSubscriber...")
        self.stop()
        self.amqp_url = self.__get_amqp_url()
        await asyncio.sleep(5)
        self.running = True
        await asyncio.create_task(self.run_async())

    async def listen_for_events(self):
        """
        Listens for events from EVENTS_QUEUE and handles AMQP config changes.
        """
        while True:
            try:
                event = EVENTS_QUEUE.get(block=False)
                logging.debug(f"Received event in AMQPSubscriber: {event}")
                if event == AMQPEvents.AMQP_CONFIG_CHANGED:
                    if self.running:
                        await self.restart()
                else:
                    EVENTS_QUEUE.put(event)
            except Empty:
                pass
            finally:
                await asyncio.sleep(1)

    async def main_coroutine(self):
        """
        `run_async` and `listen_for_events` tasks are gathered as a main
        coroutine.
        """
        tasks = [
            self.run_async(),
            self.listen_for_events()
        ]
        await asyncio.gather(*tasks)

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_fixed(5),
        before=before_log(logging.getLogger(), logging.INFO)
    )
    def run(self):
        """
        Overrides the run method of Thread. Runs with asyncio the main two
        tasks: `run_async` and `listen_for_events` when the AMQPSubscriber
        thread starts.
        """
        try:
            asyncio.run(self.main_coroutine())
        except KeyboardInterrupt:
            logging.info("Shutting down AMQPSubscriber...")
            self.stop()
        except asyncio.CancelledError:
            logging.error("Encountered CancelledError in AMQPSubscriber!")
            raise
        except Exception as exc:
            logging.error(f"Encountered {exc} in AMQPSubscriber!")
            raise

    async def confirm_connection(self):
        """
        Confirms if the AMQP subscriber is connected to the RabbitMQ queue
        and exchange.
        """
        try:
            if not self.connection or self.connection.is_closed:
                logging.error("No active connection to RabbitMQ.")
                return False

            if not self.channel or self.channel.is_closed:
                logging.error("Channel is not open.")
                return False

            if not self.queue:
                logging.error(f"Queue {self.amqp_config.amqp_queue_name} is "
                              "not declared.")
                return False

            if not self.exchange:
                logging.error(f"Exchange {self.amqp_config.amqp_exchange} is "
                              "not declared.")
                return False

            self.events_queue.put(EventType.ACCEPTED_CLIENT_CONNECTION)
            logging.info("Successfully connected to RabbitMQ queue and "
                         "exchange.")
            return True

        except Exception as e:
            logging.error(f"Error confirming connection to RabbitMQ: {e}")
            return False
