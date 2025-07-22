from queue import Queue
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch, call

import aio_pika
import pytest
from aio_pika import RobustQueue
from aiormq import ProbableAuthenticationError

from config_manager.models import AMQPSubscriberConfig, GeneralConfig
from input_unit.amqp_subscriber import AMQPSubscriber
from input_unit.input import InputUnit
from input_unit.input_data_store import InputDataStore
from input_unit.shared_data import PurgeScheme, DecoderErrorScheme, EventType, MessageType


class TestAMQPSubscriber(IsolatedAsyncioTestCase):
    def setUp(self):
        self.startup_config = {
            "General": GeneralConfig(
                listening_port=5679,
                listening_address="0.0.0.0",
                queue_max_items_metrics=1000,
                queue_max_items_logs=1000,
                cycle_queue_metrics=True,
                cycle_queue_logs=True,
                input_buffer_chunk_size=4096,
                input_buffer_size=32768,
                input_buffer_purge_scheme=PurgeScheme.DISABLED,
                input_buffer_error_scheme=DecoderErrorScheme.REPLACE,
            ),
            "AMQPSubscriber": AMQPSubscriberConfig(
                amqp_host="127.0.0.1",
                amqp_port=5672,
                amqp_user="guest",
                amqp_pass="guest",
                amqp_vhost="/",
                amqp_logs_routing_keys="RazorLink.Internal.Razor.#",
                amqp_metrics_routing_keys=("lw.comp.ISC.context.snapshot,"
                                           "Components.RazorLink.snapshot"),
                amqp_exchange="logging",
                amqp_exchange_type="topic",
                amqp_queue_name="StandardAuditTransient",
            )
        }

        input_unit = InputUnit(self.startup_config)
        self.input_data_store = InputDataStore(self.startup_config["General"])
        self.amqp_subscriber = AMQPSubscriber(
            input_unit, self.input_data_store
        )
        self.amqp_subscriber.events_queue = Queue()

    def test_get_amqp_url(self):
        amqp_config = self.startup_config.get("AMQPSubscriber")
        amqp_url = (f"amqp://{amqp_config.amqp_user}:{amqp_config.amqp_pass}@"
                    f"{amqp_config.amqp_host}:{amqp_config.amqp_port}"
                    f"{amqp_config.amqp_vhost}")
        assert amqp_url == self.amqp_subscriber._AMQPSubscriber__get_amqp_url()

    async def test_connect_successful(self):
        with patch("input_unit.amqp_subscriber.aio_pika.connect_robust") as mock_connect_robust:
            mock_connection = AsyncMock()
            mock_channel = AsyncMock()
            mock_exchange = AsyncMock()
            mock_queue = AsyncMock()

            mock_connect_robust.return_value = mock_connection
            mock_connection.channel.return_value = mock_channel
            mock_channel.declare_exchange.return_value = mock_exchange
            mock_channel.declare_queue.return_value = mock_queue

            await self.amqp_subscriber.connect()

            mock_connect_robust.assert_called_once_with(
                self.amqp_subscriber.amqp_url)
            mock_connection.channel.assert_called_once()
            mock_channel.declare_exchange.assert_called_once_with(
                self.amqp_subscriber.amqp_config.amqp_exchange,
                aio_pika.ExchangeType.TOPIC,
                internal=True, passive=True
            )
            mock_channel.declare_queue.assert_called_once_with(
                self.amqp_subscriber.amqp_config.amqp_queue_name, passive=True
            )
            routing_keys = (
                    self.amqp_subscriber.amqp_config.amqp_logs_routing_keys +
                    self.amqp_subscriber.amqp_config.amqp_metrics_routing_keys
            )
            for routing_key in routing_keys:
                mock_queue.bind.assert_any_call(mock_exchange, routing_key)

    async def test_connect_failure(self):
        with patch("input_unit.amqp_subscriber.aio_pika.connect_robust",
                   new=AsyncMock(side_effect=ProbableAuthenticationError)):
            with patch.object(self.amqp_subscriber.events_queue, "put", new=Mock()) as mock_put:
                with pytest.raises(ProbableAuthenticationError):
                    await self.amqp_subscriber.connect()

                mock_put.assert_called_once_with(
                    EventType.ACCEPTED_CLIENT_CONNECTION)

    def test_get_message_type_metric(self):
        message = {
            '0': '',
            'Hd': {
                'Ct': 'lw.comp.ISC.context.snapshot',
                'Fu': 'DoSnapshotLog',
                'P': {
                    'Fi': 'lw/shared/util/src/public/log2/LoggingObject.cpp',
                    'Li': 862
                }
            },
            'snapshot': {
                'ctx': {
                    'ctxId': '89ae29e8-a061-46fe-9f9e-d99dcf515045',
                    'oid': '<IscCtx!5181160>'
                }
            }
        }
        message_type = self.amqp_subscriber.get_message_type(message)
        assert message_type == MessageType.METRIC

    def test_get_message_type_log(self):
        message = {
            'Hd': {
                'Ct': 'RazorLink.Internal.Razor.SNE.API.AccessError',
                'Fu': 'LogAuthTokenSessionRetrievalError',
                'P': {
                    'Fi': 'lw/shared/gen-api/src/engine/GenAPIEngine.cpp',
                    'Li': 1150
                }
            },
            'NoSessionFound': {
                'AuthToken': 'veDotR3DqOItk',
                'Event': 'No session found for this Auth Token'
            }
        }
        message_type = self.amqp_subscriber.get_message_type(message)
        assert message_type == MessageType.LOG

    def test_get_message_type_unknown(self):
        message = "Wrong message type"
        message_type = self.amqp_subscriber.get_message_type(message)
        assert message_type == MessageType.UNKNOWN

    async def test_connect_successful_shore(self):
        with patch("input_unit.amqp_subscriber.aio_pika.connect_robust") as mock_connect_robust:
            mock_connection = AsyncMock()
            mock_channel = AsyncMock()
            mock_exchange = AsyncMock()
            mock_queue = AsyncMock()

            mock_connect_robust.return_value = mock_connection
            mock_connection.channel.return_value = mock_channel
            mock_channel.declare_exchange.return_value = mock_exchange
            mock_channel.declare_queue.return_value = mock_queue

            self.amqp_subscriber.input_unit.general_cfg.vault_location = "shore"
            self.amqp_subscriber.amqp_config.amqp_queue_name = "queue-{service_index}"

            await self.amqp_subscriber.connect()

            mock_connect_robust.assert_called_once_with(
                self.amqp_subscriber.amqp_url)
            mock_connection.channel.assert_called_once()
            mock_channel.declare_exchange.assert_called_once_with(
                self.amqp_subscriber.amqp_config.amqp_exchange,
                aio_pika.ExchangeType.TOPIC,
                internal=True, passive=True
            )

            mock_channel.declare_queue.assert_called_once_with(
                "queue-1", arguments={"x-max-length": 100000}
            )
            routing_keys = [""]
            for routing_key in routing_keys:
                mock_queue.bind.assert_any_call(mock_exchange, routing_key)

    async def test_start_consuming(self):
        queue_mock = AsyncMock(RobustQueue)
        queue_iterator = MockAsyncContextManager(
            [
                "{\"Detail\": {\"snapshot\":{}}}",
                "invalid_json"
            ]
        )
        queue_mock.iterator.return_value = queue_iterator
        self.amqp_subscriber.running = True

        self.amqp_subscriber.queue = queue_mock

        await self.amqp_subscriber.start_consuming()

        self.assertEqual(self.input_data_store.queues[MessageType.LOG].get_size(), 0)
        self.assertEqual(self.input_data_store.queues[MessageType.METRIC].get_size(), 1)

    async def test_confirm_connection(self):
        self.amqp_subscriber.channel = AsyncMock()
        self.amqp_subscriber.queue = AsyncMock()
        self.amqp_subscriber.exchange = AsyncMock()
        self.amqp_subscriber.connection = AsyncMock()

        self.amqp_subscriber.channel.is_closed = False
        self.amqp_subscriber.connection.is_closed = False

        result = await self.amqp_subscriber.confirm_connection()

        self.assertTrue(result)
        self.assertFalse(self.amqp_subscriber.channel.is_closed)
        self.assertIsNotNone(self.amqp_subscriber.queue)
        self.assertIsNotNone(self.amqp_subscriber.exchange)


class MockAsyncContextManager:
    def __init__(self, messages: list):
        self.messages = [MockMessage(message) for message in messages]

    async def __aenter__(self):
        return MockIterator(self.messages)

    async def __aexit__(self, exc_type, exc, tb):
        self.messages = []


class MockIterator:
    def __init__(self, messages: list):
        self.messages = messages
        self.index = -1

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.index += 1
        if self.index >= len(self.messages):
            raise StopAsyncIteration()

        return self.messages[self.index]


class MockMessage:
    def __init__(self, message: str):
        self.body = message.encode()

    def process(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass
