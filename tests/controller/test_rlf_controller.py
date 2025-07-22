# pylint: disable-all
import configparser
import sys
from collections import namedtuple
from pathlib import Path
from queue import Empty
from unittest import TestCase, mock
from unittest.mock import MagicMock, PropertyMock

from config_manager.models import GeneralConfig, InputUnitType
from controller.common import NotBooted, ReturnCodes, RLFCriticalException
from controller.controller import RLFController
from config_manager.config_manager import ConfigStore
from config_manager.models import OTELMetricsAdapterConfig
from tests.controller import exception_mocks

class TestHandleRLLost(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    def test_unintialized(self):
        with self.assertRaises(AttributeError):
            self.rlf_c._RLFController__handle_rl_lost()

    def test_initialized(self):
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c._RLFController__handle_rl_lost()
        self.rlf_c.input_unit.drop_all_clients.assert_called_once()
        self.assertTrue(self.rlf_c.should_reopen)


class TestHandleRLFault(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.logging")
    def test_logging(self, logging):
        self.rlf_c._RLFController__handle_rl_fault()
        logging.warning.assert_called()


class TestHandleRLReachable(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.logging")
    def test_log_info(self, logging):
        with mock.patch.object(self.rlf_c, "safe_open_logging_session") as so:
            self.rlf_c.should_reopen = True
            self.rlf_c._RLFController__handle_rl_reachable()
            logging.info.assert_called()

    @mock.patch("controller.controller.logging")
    def test_just_open(self, logging):
        with mock.patch.object(self.rlf_c, "safe_open_logging_session") as so:
            self.rlf_c.should_reopen = False
            self.rlf_c._RLFController__handle_rl_reachable()
            logging.info.assert_not_called()
            so.assert_called_once()


class TestWarnMissingCategories(TestCase):
    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.logging")
    def test_warn_both(self, logging):
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_store.metrics_config = []
        self.rlf_c.config_store.logs_config = []
        self.rlf_c._RLFController__warn_missing_categories()
        logging.warning.assert_called()
        self.assertEqual(logging.warning.call_count, 2)


class TestBootRLController(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.SNAPITokenAuth")
    @mock.patch("controller.controller.RLController")
    def test_normal_boot(self, rl_controller_mock, snapi_auth_mock):
        snapi_cfg_mock = mock.MagicMock()
        snapi_cfg_mock.address = "test"
        snapi_cfg_mock.port = "8000"
        cfg_store = mock.MagicMock()
        general = mock.MagicMock()
        general.vault_location.return_value = "edge"
        general.enabled_input_unit = InputUnitType.TCP_LISTENER
        cfg_store.startup_config = {
            "SNAPI": snapi_cfg_mock, "Vault": mock.MagicMock(),
            "MetricTransform": mock.MagicMock(),
            "General": general}
        self.rlf_c.config_store = cfg_store

        with mock.patch.object(self.rlf_c, "_RLFController__retrieve_snapi_token_vault") as gst:
            gst.return_value = "test"
            self.rlf_c._RLFController__boot_rl_controller("test")
            snapi_auth_mock.assert_called_with("test")
            rl_controller_mock.assert_called_with(
                snapi_cfg_mock.address, snapi_cfg_mock.port,
                snapi_auth_mock.return_value, True,
                general.enabled_input_unit,
                system_id=None
            )
            self.assertTrue(rl_controller_mock.return_value.daemon)
            rl_controller_mock.return_value.start.assert_called_once()



class TestBootDataProcessor(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.RLFController._RLFController__get_correlation_config")
    @mock.patch("controller.controller.DataProcessor")
    def test_normal_boot(self, data_processor_mock, get_config_mock):
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.output_unit = mock.MagicMock()
        self.rlf_c.config_store.startup_config = mock.MagicMock()
        get_config_mock.return_value = return_value_mock = mock.MagicMock()
        self.rlf_c._RLFController__boot_data_processor()

        data_processor_mock.assert_called_with(
            self.rlf_c.input_unit, self.rlf_c.output_unit,
            self.rlf_c.metric_transform_config,
            return_value_mock)
        data_processor_mock.return_value.start.assert_called_once()


class TestBootInputUnit(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.InputUnit")
    def test_normal_boot(self, input_unit_mock):
        general_cfg_mock = mock.MagicMock()
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store

        self.rlf_c._RLFController__boot_input_unit()

        input_unit_mock.assert_called_with(cfg_store.startup_config)
        input_unit_mock.return_value.start.assert_called_once()

    @mock.patch("controller.controller.logging")
    @mock.patch("controller.controller.InputUnit")
    def test_socket_errors(self, input_unit_mock, logging_mock):
        general_cfg_mock = mock.MagicMock()
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store
        input_unit = input_unit_mock.return_value
        input_unit.start.side_effect = OSError()

        with mock.patch.object(sys, "exit") as mock_exit:
            self.rlf_c._RLFController__boot_input_unit()
            mock_exit.assert_called_with(ReturnCodes.SOCKET_ERRORS)
            logging_mock.error.assert_called_once()

        input_unit_mock.assert_called_with(cfg_store.startup_config)
        input_unit_mock.return_value.start.assert_called_once()


class TestBootOutputUnit(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.OutputUnit")
    def test_normal_boot(self, output_unit_mock):
        general_cfg_mock = mock.MagicMock()
        general_cfg_mock.queue_max_items_logs = 10
        general_cfg_mock.queue_max_items_metrics = 10
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store
        self.rlf_c._RLFController__boot_output_unit()
        output_unit_mock.assert_called_with(
            cfg_store.startup_config,
            general_cfg_mock.queue_max_items_logs,
            general_cfg_mock.queue_max_items_metrics
        )
        output_unit = output_unit_mock.return_value
        output_unit.register_adapters.assert_called_once()
        output_unit.run_threads.assert_called_once()

    @mock.patch("controller.controller.logging")
    @mock.patch("controller.controller.OutputUnit")
    def test_unconfigured_adapters(self, output_unit_mock, logging_mock):
        general_cfg_mock = mock.MagicMock()
        general_cfg_mock.queue_max_items_logs = 10
        general_cfg_mock.queue_max_items_metrics = 10
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store
        output_unit = output_unit_mock.return_value
        output_unit.register_adapters.return_value = ["test"]
        self.rlf_c._RLFController__boot_output_unit()
        logging_mock.warning.assert_called_once()

    @mock.patch("controller.controller.OutputUnit")
    def test_config_errors(self, output_unit_mock):
        general_cfg_mock = mock.MagicMock()
        o_unit1 = mock.MagicMock()
        o_unit1.register_adapters.return_value = []
        general_cfg_mock.queue_max_items_logs = 10
        general_cfg_mock.queue_max_items_metrics = 10
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store
        self.rlf_c.output_unit = o_unit1

        output_unit_mock.side_effect = TypeError()
        with mock.patch.object(sys, "exit") as mock_exit:
            self.rlf_c._RLFController__boot_output_unit()
            mock_exit.assert_called_once_with(ReturnCodes.INVALID_CONFIG)


class TestBootConfigManager(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    def test_normal_boot(self):
        self.rlf_c.config_manager = mock.MagicMock()
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_store.logs_config = [1]
        self.rlf_c.config_store.metrics_config = [1]
        self.rlf_c._RLFController__boot_config_manager()
        self.rlf_c.config_manager.store_startup_conf.assert_called_once()
        self.rlf_c.config_manager.store_logs_conf.assert_called_once()
        self.rlf_c.config_manager.store_metrics_conf.assert_called_once()
        self.rlf_c.config_manager.watch_files.assert_called_once()
        self.assertEqual(self.rlf_c.prev_categories, [1])

    def test_missing_files(self):
        self.rlf_c.config_manager = mock.MagicMock()
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_manager.store_startup_conf.side_effect = OSError()

        with mock.patch.object(sys, "exit") as mock_exit:
            self.rlf_c._RLFController__boot_config_manager()
            mock_exit.assert_called_with(ReturnCodes.MISSING_FILES)

    def test_invalid_config(self):
        self.rlf_c.config_manager = mock.MagicMock()
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_manager.store_startup_conf.side_effect = configparser.Error()

        with mock.patch.object(sys, "exit") as mock_exit:
            self.rlf_c._RLFController__boot_config_manager()
            mock_exit.assert_called_with(ReturnCodes.INVALID_CONFIG)


class TestConfirmConnection(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.InputUnitEventType")
    def test_connected(self, input_event_type):
        accepted = input_event_type.ACCEPTED_CLIENT_CONNECTION
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.input_unit.events_queue.get.return_value = accepted
        r = self.rlf_c._RLFController__confirm_connection(3, 2)
        self.assertTrue(r)
        self.rlf_c.input_unit.events_queue.get.assert_called_once()

    @mock.patch("controller.controller.InputUnitEventType")
    def test_retries(self, input_event_type):
        accepted = input_event_type.ACCEPTED_CLIENT_CONNECTION
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.input_unit.events_queue.get.side_effect = [
            None, None, accepted]
        r = self.rlf_c._RLFController__confirm_connection(3, 2)
        self.assertTrue(r)
        self.rlf_c.input_unit.events_queue.get.assert_called()
        self.assertEqual(self.rlf_c.input_unit.events_queue.get.call_count, 3)

    @mock.patch("controller.controller.InputUnitEventType")
    def test_empty(self, input_event_type):
        accepted = input_event_type.ACCEPTED_CLIENT_CONNECTION
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.input_unit.events_queue.get.side_effect = [
            Empty(), Empty(), accepted]
        r = self.rlf_c._RLFController__confirm_connection(3, 2)
        self.assertTrue(r)
        self.rlf_c.input_unit.events_queue.get.assert_called()
        self.assertEqual(self.rlf_c.input_unit.events_queue.get.call_count, 3)


class TestHandleRLConnectionClosed(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    def test_empty(self):
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.input_unit.events_queue.get.side_effect = Empty()
        self.rlf_c.handle_rl_connection_closed()
        self.rlf_c.input_unit.events_queue.get.assert_called_once()

    @mock.patch("controller.controller.InputUnitEventType")
    def test_closed_connection(self, input_event_type):
        closed = input_event_type.CLIENT_CLOSED_CONNECTION
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.input_unit = mock.MagicMock()
        self.rlf_c.input_unit.events_queue.get.return_value = closed
        with mock.patch.object(self.rlf_c, "safe_open_logging_session") as mock_open:
            self.rlf_c.handle_rl_connection_closed()
            mock_open.assert_called_once()
        self.rlf_c.input_unit.events_queue.get.assert_called_once()
        self.assertTrue(self.rlf_c.should_reopen)


class TestHandleConfigChange(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    @mock.patch("controller.controller.EVENTS_QUEUE")
    def test_empty(self, events_queue):
        self.rlf_c._RLFController__boot_complete = True
        events_queue.get.side_effects = Empty()
        self.rlf_c.handle_config_change()
        with mock.patch.object(self.rlf_c, "_RLFController__set_new_categories") as snc:
            snc.assert_not_called()

    @mock.patch("controller.controller.ConfMgrEventType", ["test"])
    @mock.patch("controller.controller.EVENTS_QUEUE")
    def test_config_change(self, events_queue):
        self.rlf_c._RLFController__boot_complete = True
        events_queue.get.return_value = "test"
        with mock.patch.object(self.rlf_c, "_RLFController__set_new_categories") as snc:
            self.rlf_c.handle_config_change()
            snc.assert_called()


class TestHandleOTAConfigChange(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())
        self.rlf_c.last_ota_config = {"exporters": {},
                                      "instruments-config": {},
                                      "resources": {},
                                      "metrics-config": [],
                                      "logs-config": []}

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.AgentConfig")
    def test_ota_agent_instruments_config_change(self, rl_controller_mock, ac_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {"otel-config": {"exporters": {},
                                                            "instruments-config": {"k1":"v1","k2":"v2"},
                                                            "resources": {},
                                                            "metrics-config": [],
                                                            "logs-config": []}}
            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.config_store = mock.Mock()
            self.rlf_c.config_store.startup_config = {
                "General": GeneralConfig()
            }
            with mock.patch.object(self.rlf_c, "_RLFController__handle_otel_config_change") as snc:
                self.rlf_c.handle_ota_config_change()
                snc.assert_called()

            assert self.rlf_c.last_ota_config == self.rlf_c.ota_config

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.OutputUnit")
    def test_handle_ota_config_change(self, output_unit_mock, rl_controller_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {
                "otel-config": {"exporters": {},
                                "instruments-config": {"k1": "v1", "k2": "v2"},
                                "resources": {},
                                "metrics-config": [],
                                "logs-config": []}}

            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.output_unit = output_unit_mock
            self.rlf_c.output_unit.register_adapters = MagicMock()
            self.rlf_c.handle_ota_config_change()
            self.rlf_c.last_ota_config = {"exporters": {},
                                          "instruments-config": {},
                                          "resources": {},
                                          "metrics-config": [],
                                          "logs-config": []}
            with mock.patch.object(self.rlf_c.output_unit, "register_adapters") as snc:
                self.rlf_c._RLFController__handle_otel_config_change()
                snc.assert_called()


    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.AgentConfig")
    def test_ota_resources_config_change(self, rl_controller_mock, ac_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {
                "otel-config": {"exporters": {},
                                "instruments-config": {},
                                "resources": {"system-id": "s1"},
                                "metrics-config": [],
                                "logs-config": []}}
            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.config_store = mock.Mock()
            self.rlf_c.config_store.startup_config = {
                "General": GeneralConfig(
                    enabled_input_unit=InputUnitType.TCP_LISTENER)
            }
            with mock.patch.object(self.rlf_c, "_RLFController__handle_otel_config_change") as snc:
                self.rlf_c.handle_ota_config_change()
                snc.assert_called()

            assert self.rlf_c.last_ota_config == self.rlf_c.ota_config

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.AgentConfig")
    def test_ota_metrics_change(self, rl_controller_mock, ac_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {
                "otel-config": {"exporters": {},
                                "instruments-config": {},
                                "resources": {},
                                "metrics-config": ["m1"],
                                "logs-config": []}}
            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.config_store = mock.Mock()
            self.rlf_c.config_store.startup_config = {
                "General": GeneralConfig(
                    enabled_input_unit=InputUnitType.TCP_LISTENER)
            }
            self.rlf_c.input_unit_type = InputUnitType.TCP_LISTENER
            with mock.patch.object(self.rlf_c, "_RLFController__handle_ota_metrics_config_change") as snc:
                self.rlf_c.handle_ota_config_change()
                snc.assert_called()

            assert self.rlf_c.last_ota_config == self.rlf_c.ota_config

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    def test_handle_ota_metrics_config_change(self):
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.last_ota_config = {"exporters": {},
                                      "instruments-config": {},
                                      "resources": {},
                                      "metrics-config": [],
                                      "logs-config": []}
        self.rlf_c.ota_config = {"exporters": {},
                                 "instruments-config": {},
                                 "resources": {},
                                 "metrics-config": ["m1"],
                                 "logs-config": []}

        self.rlf_c.config_manager = mock.MagicMock()
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_manager.config_store.metrics_config = []
        self.rlf_c._RLFController__handle_ota_metrics_config_change()

        self.assertEqual(self.rlf_c.config_manager.config_store.metrics_config,
                         self.rlf_c.ota_config.get("metrics-config"))

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.AgentConfig")
    def test_ota_logs_change(self, rl_controller_mock,ac_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {"otel-config": {"exporters": {},
                                                            "instruments-config": {},
                                                            "resources": {},
                                                            "metrics-config": [],
                                                            "logs-config": ["l1", "l2"]}}
            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.config_store = mock.Mock()
            self.rlf_c.config_store.startup_config = {
                "General": GeneralConfig(
                    enabled_input_unit=InputUnitType.TCP_LISTENER)
            }
            self.rlf_c.input_unit_type = InputUnitType.TCP_LISTENER
            with mock.patch.object(self.rlf_c,
                                   "_RLFController__handle_ota_logs_config_change") as snc:
                self.rlf_c.handle_ota_config_change()
                snc.assert_called()

            assert self.rlf_c.last_ota_config == self.rlf_c.ota_config

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    def test_handle_ota_logs_config_change(self):
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.last_ota_config = {"exporters": {},
                                      "instruments-config": {},
                                      "resources": {},
                                      "metrics-config": [],
                                      "logs-config": []}
        self.rlf_c.ota_config = {"exporters": {},
                                 "instruments-config": {},
                                 "resources": {},
                                 "metrics-config": [],
                                 "logs-config": ["l1"]}

        self.rlf_c.config_manager = mock.MagicMock()
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.config_manager.config_store.logs_config = []
        self.rlf_c._RLFController__handle_ota_logs_config_change()

        self.assertEqual(self.rlf_c.config_manager.config_store.logs_config,
                         self.rlf_c.ota_config.get("logs-config"))

    @mock.patch("controller.controller.ConfMgrEventType",
                namedtuple("EventType",
                           ["LOGS_CONFIG_CHANGED", "METRICS_CONFIG_CHANGED"]))
    @mock.patch("controller.controller.RLController")
    @mock.patch("controller.controller.AgentConfig")
    def test_handle_ncat_config_change(self, rl_controller_mock, ac_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.ota_config',
                        new_callable=PropertyMock) as mock_ota_config:
            mock_ota_config.return_value = {"otel-config": {"exporters": {},
                                                            "ncat": {"excluded_dig_interfaces": ["l1", "l2"]},
                                                            "instruments-config": {},
                                                            "resources": {},
                                                            "metrics-config": [],
                                                            "logs-config": []}}
            rl_controller_mock.ota_config = mock_ota_config()
            self.rlf_c.rl_controller = rl_controller_mock
            self.rlf_c.config_store = mock.Mock()
            self.rlf_c.config_store.startup_config = {
                "General": GeneralConfig(
                    enabled_input_unit=InputUnitType.TCP_LISTENER),
                "OTELMetricsAdapter": OTELMetricsAdapterConfig(
                    excluded_dig_interfaces="")
            }
            self.rlf_c.input_unit_type = InputUnitType.TCP_LISTENER
            self.rlf_c.handle_ota_config_change()

            assert self.rlf_c.last_ota_config == self.rlf_c.ota_config
            assert self.rlf_c.config_store.startup_config["OTELMetricsAdapter"].excluded_dig_interfaces == ["l1", "l2"]

    def test_compare_ota_config(self):
        self.rlf_c._RLFController__boot_complete = True

        self.assertTrue(self.rlf_c.compare_ota_config({1: 2}, {1: 2}))
        self.assertTrue(self.rlf_c.compare_ota_config([1, 2], [1, 2]))
        self.assertTrue(self.rlf_c.compare_ota_config("1: 2", "1: 2"))
        self.assertFalse(self.rlf_c.compare_ota_config({1: 2}, {"1": "2"}))

    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_status_if_not_receiving_status_updates")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_can_calculate_status")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_status_metric_data")
    @mock.patch("controller.controller.RLController")
    def test_set_status_data(
            self, rl_controller_mock, set_data_mock, set_cc_stat_mock, set_not_upd_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.status_data',
                        new_callable=PropertyMock) as mock_status_data:
            mock_status_data.return_value = {'tenancy': {'fvkau3': {'ready': True, 'started': True, 'sites': {'e77to3ru': {'started': True, 'ready': True}}, 'current_field': 'SDEP_G2', 'holdoff_state': False, 'is_seep': True}}, 'ctx_maps': {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'pub_ctx_id': '34a9b166-9aa3-4d7b-bd32-85a28576c8eb', 'state': 3.0, 'is_seep': True, 'sdp_id': '230000000103600', 'tenants': ['fvkau3'], 'tenants_count': 1}}, 'irs': {'Ethernet_eth2_3917': {'active': True, 'name': 'Ethernet_eth2_3917'}, 'ppp100': {'active': True, 'name': 'ppp100'}}}
            t_status = {'fvkau3': True}
            ir_status = {'Ethernet_eth2_3917': 1, 'ppp100': 1, 'Ethernet_eth7_4033': 0}
            ctx_data = {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'sdp': '230000000103600', 'tenants': ['fvkau3'], 'prim_ctx': True}}
            rl_controller_mock.status_data = mock_status_data()
            rl_controller_mock.is_sdep = False
            self.rlf_c.rl_controller = rl_controller_mock

            self.rlf_c.set_status_data()
            self.rlf_c.status_data = {}
            set_data_mock.assert_called_with(
                ctx_data, ir_status, False)
            set_cc_stat_mock.assert_called_with(
                t_status)
            set_not_upd_mock.assert_called_with(
                t_status, ir_status, False, "")

    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_status_if_not_receiving_status_updates")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_can_calculate_status")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_status_metric_data")
    @mock.patch("controller.controller.RLController")
    def test_set_status_data_part2(
            self, rl_controller_mock, set_data_mock, set_cc_stat_mock, set_not_upd_mock):
        self.rlf_c._RLFController__boot_complete = True

        with mock.patch('controller.controller.RLController.status_data',
                        new_callable=PropertyMock) as mock_status_data:
            mock_status_data.return_value = {'tenancy': {'fvkau3': {'ready': True, 'started': True, 'sites': {'e77to3ru': {'started': True, 'ready': True}}, 'current_field': 'SDEP_G2', 'holdoff_state': False, 'is_seep': True}}, 'ctx_maps': {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'pub_ctx_id': '34a9b166-9aa3-4d7b-bd32-85a28576c8eb', 'state': 3.0, 'is_seep': True, 'sdp_id': '230000000103600'}}, 'irs': {'Ethernet_eth2_3917': {'active': True, 'name': 'Ethernet_eth2_3917'}, 'ppp100': {'active': True, 'name': 'ppp100'}}}
            t_status = {'fvkau3': True}
            ir_status = {'Ethernet_eth2_3917': 1, 'ppp100': 1, 'Ethernet_eth7_4033': 0}
            ctx_data = {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'sdp':'230000000103600'}}
            rl_controller_mock.status_data = mock_status_data()
            rl_controller_mock.is_sdep = True
            self.rlf_c.rl_controller = rl_controller_mock

            self.rlf_c.set_status_data()
            self.rlf_c.status_data = {}
            set_data_mock.assert_called_with(
                ctx_data, ir_status, True)
            set_cc_stat_mock.assert_called_with(
                t_status)
            set_not_upd_mock.assert_called_with(
                t_status, ir_status, True, "")

    def test_get_tenant_status(self):
        self.rlf_c._RLFController__boot_complete = True
        data1 = {'fvkau3': {'ready': True, 'started': True, 'sites': {'e77to3ru': {'started': True, 'ready': True}}, 'current_field': 'SDEP_G2', 'holdoff_state': False, 'is_seep': True}}
        res1 = self.rlf_c._RLFController__get_tenant_status(data1)
        self.assertEqual(res1, {'fvkau3': True})
        data2 = {'fvkau3': {'ready': True, 'started': True, 'sites': {'e77to3ru': {'started': True, 'ready': True}}, 'is_seep': False}}
        res2 = self.rlf_c._RLFController__get_tenant_status(data2)
        self.assertEqual(res2, {'fvkau3': True})
        data3 = {'fvkau3': {'ready': True, 'started': False, 'sites': {'e77to3ru': {'started': True, 'ready': True}}, 'is_seep': False}}
        res3 = self.rlf_c._RLFController__get_tenant_status(data3)
        self.assertEqual(res3, {'fvkau3': False})
        data4 = {}
        res4 = self.rlf_c._RLFController__get_tenant_status(data4)
        self.assertEqual(res4, {})

    def test_get_ctx_map(self):
        self.rlf_c._RLFController__boot_complete = True
        data1 = {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'pub_ctx_id': '34a9b166-9aa3-4d7b-bd32-85a28576c8eb', 'state': 3.0, 'is_seep': True, 'sdp_id': '230000000103600'}}
        res1 = self.rlf_c._RLFController__get_ctx_map(data1, True)
        self.assertEqual(res1, {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'sdp': '230000000103600'}})
        data2 = {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'pub_ctx_id': '34a9b166-9aa3-4d7b-bd32-85a28576c8eb', 'state': 3.0, 'is_seep': True, 'sdp_id': '230000000103600', 'tenants': ['a_tenant'], 'tenants_count': 1}}
        res2 = self.rlf_c._RLFController__get_ctx_map(data2, False)
        self.assertEqual(res2, {'f9a84e00-4775-4899-8caa-8ef145d6d7b1': {'sdp': '230000000103600', 'tenants': ['a_tenant'], 'prim_ctx': True }})

    def test_get_ir_status(self):
        self.rlf_c._RLFController__boot_complete = True
        data = {'Ethernet_eth2_3917': {'active': True, 'name': 'Ethernet_eth2_3917'}, 'ppp100': {'active': True, 'name': 'ppp100'}}
        res = self.rlf_c._RLFController__get_ir_status(data)
        print(res)
        self.assertEqual(res, {'Ethernet_eth2_3917': 1, 'ppp100': 1, 'Ethernet_eth7_4033': 0})

    def test_get_sites_status(self):
        self.rlf_c._RLFController__boot_complete = True
        data1 = {'sites':{'e77to3ru': {'started': True, 'ready': True}}}
        data2 =  {'sites':{'e77to3ru': {'started': True, 'ready': True}, 'e77to3rx': {'started': True, 'ready': False}}}
        res1 = self.rlf_c._RLFController__get_sites_status(data1)
        res2 = self.rlf_c._RLFController__get_sites_status(data2)
        self.assertEqual(res1, True)
        self.assertEqual(res2, False)

    @mock.patch('builtins.print')
    def test_print_status_from_snapi(self, mock_print):
        self.rlf_c._RLFController__print_status_from_snapi(
            "status_data", "ts_status", "ir_status", "ctx_data")
        mock_print.assert_called_with(
            "-------------SNAPI------------------\n"
            "status_data='status_data'\n"
            "t_status='ts_status'\n"
            "ir_status='ir_status'\n"
            "ctx_data='ctx_data'\n"
            "------------------------------------")

    @mock.patch('builtins.print')
    def test_print_nc_data(self, mock_print):
        self.rlf_c._RLFController__print_nc_data(
            "nc_data")
        mock_print.assert_called_with(
            "-------------NCDATA------------------\n"
            "nc_data='nc_data'\n"
            "------------------------------------")

    @mock.patch("controller.controller.RLController")
    @mock.patch('controller.controller.requests.get')
    def test_set_vnf_state_data(
            self, get_req_mock, rl_controller_mock):
        ncat_config = mock.MagicMock(address="0.0.0.0", port=8000, timeout=1)
        rl_controller_mock.is_sdep = False
        self.rlf_c.rl_controller = rl_controller_mock
        self.rlf_c.set_vnf_state_data(ncat_config)
        get_req_mock.assert_called()

    @mock.patch("controller.controller.repair_json")
    @mock.patch("controller.controller.requests.Response")
    @mock.patch("controller.controller.RLController")
    @mock.patch("output_unit.metric_storage.MetricStore."
                "set_status_based_on_netcat_updates")
    def test_process_vnf_state_data(
            self, set_nc_mock, rl_controller_mock, resp_mock, repair_json_mock):

        resp_mock = mock.MagicMock(status_code=200, text='{"data": "some_data",}')
        repair_json_mock.return_value = '{"data": "some_data"}'
        response = resp_mock
        rl_controller_mock.is_sdep = False
        startup_cfg = {"OTELMetricsAdapter": OTELMetricsAdapterConfig()}
        config_store = ConfigStore(startup_config=startup_cfg)
        self.rlf_c.config_store = config_store
        self.rlf_c.rl_controller = rl_controller_mock
        self.rlf_c._RLFController__process_vnf_state_data(response)
        repair_json_mock.assert_called()
        set_nc_mock.assert_called()


class TestHandleAvailabilityChange(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    def test_empty(self):
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.rl_controller = mock.MagicMock()
        self.rlf_c.rl_controller.event_queue.get.side_effect = Empty()
        self.rlf_c.handle_availability_change()

    def test_change(self):
        change_mock = mock.MagicMock()
        self.rlf_c._RLFController__rlc_handlers = {
            "test": change_mock
        }
        self.rlf_c._RLFController__boot_complete = True
        self.rlf_c.rl_controller = mock.MagicMock()
        self.rlf_c.rl_controller.event_queue.get.return_value = "test"
        self.rlf_c.handle_availability_change()
        change_mock.assert_called_once()

    def test_boot_guard(self):
        self.rlf_c._RLFController__boot_complete = False
        with self.assertRaises(NotBooted):
            self.rlf_c.handle_availability_change()


class TestSafeOpenLoggingSession(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())
        general_cfg_mock = mock.MagicMock()
        cfg_store = mock.MagicMock()
        cfg_store.startup_config = {
            "General": general_cfg_mock
        }
        self.rlf_c.config_store = cfg_store
        self.rlf_c._RLFController__boot_complete = True

    def test_not_ready(self):
        self.rlf_c.should_reopen = False
        r = self.rlf_c.safe_open_logging_session()
        self.assertTrue(r)

    def test_client_connected(self):
        self.rlf_c.rl_controller = mock.MagicMock()
        with mock.patch.object(self.rlf_c,
                               "_RLFController__retry_snapi_call") as rsc:
            with mock.patch.object(self.rlf_c,
                                   "_RLFController__confirm_connection") as cc:
                cc.return_value = True
                r = self.rlf_c.safe_open_logging_session()
                self.assertTrue(r)

    def test_client_not_connected(self):
        with mock.patch.object(self.rlf_c,
                               "_RLFController__retry_snapi_call") as rsc:
            with mock.patch.object(self.rlf_c,
                                   "_RLFController__confirm_connection") as cc:
                cc.return_value = False
                r = self.rlf_c.safe_open_logging_session()
                self.assertFalse(r)


class TestSetNewCategories(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())
        self.rlf_c.prev_categories = ["1", "2", "3"]
        self.rlf_c.config_store = mock.MagicMock()
        self.rlf_c.rl_controller = mock.MagicMock()

    def test_to_remove(self):
        self.rlf_c.config_store.metrics_config = ["1"]
        self.rlf_c.config_store.logs_config = ["2"]
        self.rlf_c.input_unit_type = InputUnitType.TCP_LISTENER
        with mock.patch.object(self.rlf_c,
                               "_RLFController__retry_snapi_call") as rsc:
            self.rlf_c._RLFController__set_new_categories()
            rsc.assert_called()

    def test_to_add(self):
        self.rlf_c.config_store.metrics_config = ["1"]
        self.rlf_c.config_store.logs_config = ["2", "5"]
        self.rlf_c.input_unit_type = InputUnitType.TCP_LISTENER
        with mock.patch.object(self.rlf_c,
                               "_RLFController__retry_snapi_call") as rsc:
            self.rlf_c._RLFController__set_new_categories()
            rsc.assert_called()


class TestRetrySnapiCall(TestCase):

    def setUp(self) -> None:
        self.rlf_c = RLFController(Path())

    def test_first_shot(self):
        func = mock.MagicMock()
        self.rlf_c._RLFController__retry_snapi_call(func, 2)
        func.assert_called_once()

    @mock.patch("controller.controller.requests.exceptions.RequestException", exception_mocks.RequestException)
    def test_retry_twice(self):
        func = mock.MagicMock()
        func.side_effect = [exception_mocks.RequestException, None]
        self.rlf_c._RLFController__retry_snapi_call(func, 2)
        func.assert_called()
        self.assertEqual(func.call_count, 2)

    @mock.patch("controller.controller.PySNAPIException", exception_mocks.PySNAPIException)
    def test_snapi_err(self):
        func = mock.MagicMock()
        func.side_effect = exception_mocks.PySNAPIException
        with self.assertRaises(RLFCriticalException):
            self.rlf_c._RLFController__retry_snapi_call(func, 2)

    @mock.patch("controller.controller.PySNAPIException", exception_mocks.PySNAPIException)
    @mock.patch("controller.controller.logging")
    def test_snapi_err2(self, log_mock):
        func = mock.MagicMock()
        func.side_effect = [exception_mocks.PySNAPIException(status_code=500), None]
        self.rlf_c._RLFController__retry_snapi_call(func, 2)
        log_mock.info.assert_called()
