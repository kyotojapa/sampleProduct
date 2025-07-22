from time import sleep
from unittest import TestCase, mock
from unittest.mock import MagicMock

import requests

from config_manager.models import InputUnitType
from rl_controller.core import (
    PySNAPIException, RLController, RLControllerEventType)


class TestRun(TestCase):

    def setUp(self):
        self.token_auth = MagicMock()

    def tearDown(self) -> None:
        self.rlc.gracefully_stop()

    @mock.patch("rl_controller.core.Generic")
    @mock.patch("rl_controller.core.Connection")
    def test_calls_refresh(self, connection, generic):
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        connection.return_value.session_idle_time.return_value = 0
        self.rlc.start()
        # prevent race conditions
        sleep(1)
        generic.return_value.refresh_session.assert_called()
        self.assertEqual(self.rlc.event_queue.get(),
                         RLControllerEventType.REACHABLE)

    @mock.patch("rl_controller.core.OTA")
    @mock.patch("rl_controller.core.Generic")
    @mock.patch("rl_controller.core.Connection")
    def test_calls_reset(self, connection, generic, ota_mock):
        ota_mock.retrieve_ota_system_id.return_value = None
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        connection.return_value.reset_mock()
        generic.return_value.refresh_session.side_effect = PySNAPIException(
            MagicMock())
        self.rlc.start()
        sleep(1)
        connection.return_value.reset_auth.assert_called()
        self.assertEqual(self.rlc.event_queue.get(),
                         RLControllerEventType.SNAPI_ERROR)

    @mock.patch("rl_controller.core.Generic")
    @mock.patch("rl_controller.core.OTA")
    def test_timeout(self, ota_mock, generic):
        ota_mock.retrieve_ota_system_id.return_value = None
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        generic.return_value.refresh_session.side_effect = requests.exceptions.RequestException()
        self.rlc.start()
        sleep(1)
        self.assertEqual(self.rlc.event_queue.get(),
                         RLControllerEventType.UNREACHABLE)

    @mock.patch("output_unit.metric_storage.MetricStore.set_metric")
    @mock.patch("rl_controller.core.OTA")
    def test_store_snapi_metric(self, ota_mock ,set_metric_mock):
        ota_mock.retrieve_ota_system_id.return_value = None
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None,
            is_sdep=True, input_unit_type=InputUnitType.TCP_LISTENER)
        self.rlc._RLController__store_snapi_metric(value=200, status_code=200)
        set_metric_mock.assert_called_with(
            name="services.hosting.virtual_machine.sdwan_end_point.agent.assurance.snapi_requests",
            value=200,
            attrs={"status_code": 200},
            attributes_set=False,
            use_default_attributes=False,)


class TestOpenLoggingSession(TestCase):

    def setUp(self):
        self.token_auth = MagicMock()

    @mock.patch("rl_controller.core.Logging")
    @mock.patch("rl_controller.core.OTA")
    def test_snapi_call(self, ota_mock, logging):
        ota_mock.retrieve_ota_system_id.return_value = None
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        logging.return_value.open_session_logging.return_value = {
            "Filename": ""}
        cat = ["test", "test"]
        addr = "localhost"
        port = 9999
        rr = self.rlc.open_logging_session(cat, addr, port)
        logging.return_value.open_session_logging.assert_called_with(
            cat, addr, port)
        self.assertEqual(rr, {"Filename": ""})


class TestCloseLoggingSession(TestCase):

    def setUp(self):
        self.token_auth = MagicMock()

    @mock.patch("rl_controller.core.Logging")
    @mock.patch("rl_controller.core.OTA")
    def test_snapi_call(self, ota_mock, logging):
        ota_mock.retrieve_ota_system_id.return_value = None
        rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        logging.return_value.reset_mock()
        logging.return_value.close_session_logging.return_value = "Test"
        rr = rlc.close_logging_session()
        logging.return_value.close_session_logging.assert_called_once()
        self.assertEqual(rr, "Test")


class TestEnableLoggingCategories(TestCase):

    def setUp(self):
        self.token_auth = MagicMock()

    @mock.patch("rl_controller.core.Logging")
    @mock.patch("rl_controller.core.OTA")
    def test_snapi_call(self, ota_mock, logging):
        ota_mock.retrieve_ota_system_id.return_value = None
        rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        logging.return_value.reset_mock()
        logging.return_value.enable_session_log_categories.return_value = "Test"
        cat = ["test", "test"]
        rr = rlc.enable_logging_categories(cat)
        logging.return_value.enable_session_log_categories.assert_called_with(
            cat)
        self.assertEqual(rr, "Test")


class TestDisableLoggingCategories(TestCase):

    def setUp(self):
        self.token_auth = MagicMock()

    @mock.patch("rl_controller.core.Logging")
    @mock.patch("rl_controller.core.OTA")
    def test_snapi_call(self, ota_mock, logging):
        ota_mock.retrieve_ota_system_id.return_value = None
        self.rlc = RLController(
            "localhost", 4444, self.token_auth, system_id=None, is_sdep=True,
            input_unit_type=InputUnitType.TCP_LISTENER)
        logging.return_value.reset_mock()
        logging.return_value.disable_session_log_categories.return_value = "Test"
        cat = ["test", "test"]
        rr = self.rlc.disable_logging_categories(cat)
        logging.return_value.disable_session_log_categories.assert_called_with(
            cat)
        self.assertEqual(rr, "Test")
