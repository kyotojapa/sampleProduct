""" Module to run tests on input unit data store"""

import unittest
from unittest.mock import patch, call
import time
from json import loads
import product_common_logging as logs
from input_unit.input_data_store import InputDataStore, MessageType
from input_unit.input_queues import InputQueue
from input_unit.shared_data import (DecoderErrorScheme,
                                    PurgeScheme)
from config_manager.models import GeneralConfig


logs.setLevel(logs.ERROR)
# pylint: disable=no-member


class TestInputDataStore(unittest.TestCase):
    """Test if the input unit is operating correctly"""

    @classmethod
    def setUpClass(cls):
        "Test set up method"

        gen_cfg_dict = {
                "listening_port": 5679,
                "listening_address": "0.0.0.0",
                "input_buffer_chunk_size": 4096,
                "input_buffer_size": 32768,
                "input_buffer_purge_scheme":  PurgeScheme.DISABLED,
                "input_buffer_error_scheme": DecoderErrorScheme.REPLACE,
                "queue_max_items_metrics": 1000,
                "queue_max_items_logs": 1000,
                "cycle_queue_metrics": True,
                "cycle_queue_logs": True
        }
        gen_config = GeneralConfig.parse_obj(gen_cfg_dict)
        cls.gen_config = gen_config
        cls.metric_message = loads(
            '{"Hd": {"Ct": "cw.snapshot", "Ti": "16/02/2023 10:30:00"}}')
        cls.log_message = loads(
            '{"Hd": {"Ct": "cw.verbose", "Ti": "17/02/2023 11:20:00"}}')

    @classmethod
    def tearDownClass(cls):
        time.sleep(2.0)

    def setUp(self):
        self.data_store = InputDataStore(TestInputDataStore.gen_config)

    def test_data_store_init(self):
        """Method that tests the initialise function of the
        input data store"""

        self.assertNotEqual(self.data_store.queues, None)
        self.assertEqual(len(self.data_store.queues), 2)

        self.assertEqual(str(type(self.data_store.queues[MessageType.LOG])),
                         str(InputQueue))
        self.assertEqual(str(type(self.data_store.queues[MessageType.METRIC])),
                         str(InputQueue))

    def test_data_store_get_queue_size(self):
        """Method that tests the get_queue_size function of the
        input data store"""

        self.data_store.put_message(MessageType.LOG,
                                    TestInputDataStore.log_message)
        self.data_store.put_message(MessageType.METRIC,
                                    TestInputDataStore.metric_message)

        self.assertEqual(self.data_store.get_queue_size(MessageType.LOG), 1)
        self.assertEqual(self.data_store.get_queue_size(MessageType.METRIC), 1)

        with self.assertRaises(TypeError):
            self.data_store.get_queue_size(MessageType.UNKNOWN)

    def test_data_store_get_message(self):
        """Method that tests the get_message function of the
        input data store"""

        self.data_store.put_message(MessageType.LOG,
                                    TestInputDataStore.log_message)
        self.data_store.put_message(MessageType.METRIC,
                                    TestInputDataStore.metric_message)

        self.assertEqual(self.data_store.get_message(MessageType.LOG),
                         TestInputDataStore.log_message)
        self.assertEqual(self.data_store.get_message(MessageType.METRIC),
                         TestInputDataStore.metric_message)

        with self.assertRaises(TypeError):
            self.data_store.get_message(MessageType.UNKNOWN)

    @patch("input_unit.input_data_store.logging")
    def test_data_store_log_que_add_del_op(self, mock_logs):
        """Method that tests the log_que_add_del_op function of the
        input data store"""

        self.data_store.put_message(MessageType.LOG,
                                    TestInputDataStore.log_message)

        self.data_store.log_que_add_del_op(
            self.data_store.queues[MessageType.LOG],
            TestInputDataStore.log_message,
            TestInputDataStore.log_message)

        call1 = (f"Max {self.data_store.queues[MessageType.LOG]} "
                 f"queue size reached. "
                 "Popped msg time tag: "
                 f"{TestInputDataStore.log_message['Hd']['Ti']}. "
                 "New msg time tag: "
                 f"{TestInputDataStore.log_message['Hd']['Ti']}. "
                 f"Log type: {self.data_store.queues[MessageType.LOG]}. "
                 "Current size: "
                 f"{self.data_store.queues[MessageType.LOG].get_size()}.")

        call2 = ("----Input Message Counter for queue "
                 f"{self.data_store.queues[MessageType.LOG]}: "
                 f"{self.data_store.queues[MessageType.LOG].messages_in}----")

        calls = [call(call1), call(call2)]

        mock_logs.debug.assert_has_calls(calls, False)

    @patch("input_unit.input_data_store.logging")
    def test_data_store_log_que_add_op(self, mock_logs):
        """Method that tests the log_que_add_op function of the
        input data store"""

        self.data_store.put_message(MessageType.LOG,
                                    TestInputDataStore.log_message)

        self.data_store.log_que_add_op(
            self.data_store.queues[MessageType.LOG],
            TestInputDataStore.log_message)

        call1 = (f"Found log type: {self.data_store.queues[MessageType.LOG]}. "
                 "Current size: "
                 f"{self.data_store.queues[MessageType.LOG].get_size()}. "
                 f"Message time tag: "
                 f"{TestInputDataStore.log_message['Hd']['Ti']}")

        call2 = ("-----Input Message Counter: "
                 f"{self.data_store.queues[MessageType.LOG].messages_in}-----")

        calls = [call(call1), call(call2)]

        mock_logs.debug.assert_has_calls(calls, False)

    @patch("input_unit.input_data_store.logging")
    def test_data_store_put_message_queue_not_found(self, mock_logs):
        """Method that tests the put_message function of the
        input data store"""

        self.data_store.put_message(MessageType.UNKNOWN,
                                    TestInputDataStore.log_message)
        mock_logs.warning.assert_called_with(
            "Could not find queue type to store. "
            "Dropping message"
        )

    def test_data_store_put_message_queue_found_under_max_size(self):
        """Method that tests the put_message function of the
        input data store"""

        with patch.object(
                self.data_store.queues[MessageType.LOG],
                "enque_message") as enque_call:
            self.data_store.put_message(MessageType.LOG,
                                        TestInputDataStore.log_message)

            enque_call.assert_called_with(
                TestInputDataStore.log_message)

    def test_data_store_put_message_queue_max_size_reached(self):
        """Method that tests the put_message function of the
        input data store"""

        self.data_store.queues[MessageType.LOG].max_size = 0
        self.data_store.queues[MessageType.LOG].cycle_queue = True

        with patch.object(
                self.data_store.queues[MessageType.LOG],
                "enque_message") as enque_call:
            with patch.object(
                self.data_store.queues[MessageType.LOG],
                    "deque_message") as deque_call:

                self.data_store.put_message(MessageType.LOG,
                                            TestInputDataStore.log_message)

                deque_call.assert_called_with()
                enque_call.assert_called_with(
                    TestInputDataStore.log_message)

    @patch("input_unit.input_data_store.logging")
    def test_data_store_put_message_queue_max_size_reached_drop_message(
            self,
            mock_logs):
        """Method that tests the put_message function of the
        input data store"""

        self.data_store.queues[MessageType.LOG].max_size = 0
        self.data_store.queues[MessageType.LOG].cycle_queue = False

        self.data_store.put_message(MessageType.LOG,
                                    TestInputDataStore.log_message)

        mock_logs.warning.assert_called_with(
            "Max size reached cannot store any more messages. "
            "Dropping message"
        )

    @patch("input_unit.input_data_store.logging")
    @patch("input_unit.input_queues.InputQueue.clear")
    def test_clear(self, mock_clear, mock_logging):
        self.data_store.clear(MessageType.LOG)

        mock_clear.assert_called_once()
        mock_logging.debug.assert_called_with(
            "cleared queue of type MessageType.LOG")

    @patch("input_unit.input_data_store.logging")
    def test_clear_raises_typeerror(self, mock_logging):
        invalid_message_type = MessageType.UNKNOWN
        with self.assertRaises(TypeError) as context:
            self.data_store.clear(invalid_message_type)

        self.assertEqual(
            str(context.exception),
            f"No queue found for {invalid_message_type} type."
        )
        mock_logging.debug.assert_not_called()

    @patch("input_unit.input_data_store.logging")
    @patch("input_unit.input_queues.InputQueue.clear")
    def test_clear_all(self, mock_clear, mock_logging):
        self.data_store.clear_all()
        self.assertEqual(mock_clear.call_count, 2)

        mock_logging.debug.assert_any_call(
            "cleared queue of type MessageType.LOG")
        mock_logging.debug.assert_any_call(
            "cleared queue of type MessageType.METRIC")
