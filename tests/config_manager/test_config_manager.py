import tempfile
import unittest
from enum import EnumMeta
from pathlib import Path
from unittest.mock import patch

from watchdog.events import FileModifiedEvent

from config_manager.config_manager import (
    EVENTS_QUEUE, ConfigManager, ConfigStore, EventType,
    FileChangesHandler, models)


class TestConfigManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.mkdtemp()
        return super().setUpClass()

    def setUp(self) -> None:
        self.startup_conf = {
            "General": {
                "listening_port": 5000,
                "listening_address": "127.0.0.1",
                "input_buffer_chunk_size": 8192
            },
            "Vault": {
                "address": "192.168.1.2",
            },
            "SNAPI": {
                "address": "172.192.1.2",
                "port": 3000
            },
            "MetricTransform": {
                "loss_use_bytes": False,
                "include_timedout_in_loss": True
            },
        }
        self.metrics_conf = ['RazorLink.Internal.*.snapshot',
                             'RazorLink.Internal.*.qcow2',
                             'RazorLink.Internal.*.xml']
        self.logs_conf = ['RazorLink.Internal.Razor.Tenants.Info\n',
                          'RazorLink.Internal.Razor.Tenants.Warning\n',
                          'RazorLink.Internal.Razor.Tenants.Error\n']
        self.config_store = ConfigStore(self.startup_conf, self.metrics_conf,
                                        self.logs_conf)
        self.config_manager = ConfigManager(self.config_store,
                                            Path(TestConfigManager.temp_dir))
        self.file_changes_handler = FileChangesHandler(self.config_manager)

    def tearDown(self) -> None:
        del self.config_manager
        del self.config_store
        del self.startup_conf
        del self.metrics_conf
        del self.logs_conf

    def test_event_type_enum(self):
        self.assertIsInstance(EventType, EnumMeta)
        self.assertIsInstance(EventType.METRICS_CONFIG_CHANGED, EventType)
        self.assertIsInstance(EventType.LOGS_CONFIG_CHANGED, EventType)

    def test_config_store(self):
        self.assertIsInstance(self.config_store, ConfigStore)
        self.assertIsInstance(self.config_store.startup_config, dict)
        self.assertEqual(self.config_store.startup_config, self.startup_conf)
        self.assertIsInstance(self.config_store.metrics_config, list)
        self.assertEqual(self.config_store.metrics_config, self.metrics_conf)
        self.assertIsInstance(self.config_store.logs_config, list)
        self.assertEqual(self.config_store.logs_config, self.logs_conf)

    @patch("config_manager.config_manager.ConfigManager.store_metrics_conf")
    def test_file_changes_handler_metrics(self, mock_store_metrics):
        event_type = FileModifiedEvent(TestConfigManager.temp_dir +
                                       "/metrics.conf")
        mock_store_metrics.return_value = None
        self.file_changes_handler.on_modified(event_type)
        self.assertEqual(EVENTS_QUEUE.get(), EventType.METRICS_CONFIG_CHANGED)

    @patch("config_manager.config_manager.ConfigManager.store_logs_conf")
    def test_file_changes_handler_logs(self, mock_store_logs):
        event_type = FileModifiedEvent(TestConfigManager.temp_dir +
                                       "/logs.conf")
        mock_store_logs.return_value = None
        self.file_changes_handler.on_modified(event_type)
        self.assertEqual(EVENTS_QUEUE.get(), EventType.LOGS_CONFIG_CHANGED)

    def test_config_manager(self):
        self.assertIsInstance(self.config_manager, ConfigManager)
        self.assertDictEqual(self.config_manager.config_store.startup_config,
                             self.startup_conf)
        self.assertListEqual(self.config_manager.config_store.metrics_config,
                             self.metrics_conf)
        self.assertListEqual(self.config_manager.config_store.logs_config,
                             self.logs_conf)
        self.assertEqual(self.config_manager.config_files_root,
                         Path(TestConfigManager.temp_dir))
        self.assertEqual(self.config_manager.startup_conf_path,
                         Path(TestConfigManager.temp_dir) / "startup.conf")
        self.assertEqual(self.config_manager.metrics_conf_path,
                         Path(TestConfigManager.temp_dir) / "metrics.conf")
        self.assertEqual(self.config_manager.logs_conf_path,
                         Path(TestConfigManager.temp_dir) / "logs.conf")

    def test_read_file_txt(self):
        file_content = ["RazorLink.Internal.Razor.Tenants.Warning"]
        temp_file_path = Path(TestConfigManager.temp_dir).joinpath("file.txt")
        with open(temp_file_path, "w", encoding="utf-8") as file:
            file.writelines(file_content)
        self.assertEqual(self.config_manager.read_file_txt(temp_file_path),
                         file_content)

    def test_read_file_cfg(self):
        file_content = ["[General]\n", "listening_port = 8081\n",
                        "listening_address = 192.168.1.3\n"]
        tmp_file_path = Path(TestConfigManager.temp_dir).joinpath(
                             "startup.conf")
        with open(tmp_file_path, "w", encoding="utf-8") as file:
            file.writelines(file_content)
        startup_content = self.config_manager.read_file_cfg(tmp_file_path)
        self.assertIn('General', startup_content)
        self.assertIn('listening_port', startup_content['General'])
        self.assertIn('listening_address', startup_content['General'])

    @patch("config_manager.config_manager.ConfigManager.read_file_cfg")
    def test_store_startup_conf_success(self, mock_read_cfg):
        mock_read_cfg.return_value = {
            "General": {
                "listening_port": 8888,
                "listening_address": "0.0.0.0",
                "input_buffer_chunk_size": 2048
            },
            "Vault": {
                "address": "10.0.3.1",
            },
            "SNAPI": {
                "address": "10.1.124.236",
                "port": 3001
            },
            "MetricTransform": {
                "loss_use_bytes": False,
                "include_timedout_in_loss": True
            },
        }
        self.config_manager.store_startup_conf()
        self.assertIn("General", self.config_manager.config_store.
                      startup_config)
        self.assertIn("Vault", self.config_manager.config_store.
                      startup_config)
        self.assertIn("SNAPI", self.config_manager.config_store.
                      startup_config)

        startup_dict = self.config_manager.config_store.startup_config
        general_config = startup_dict.get("General")
        vault_config = startup_dict.get("Vault")
        snapi_config = startup_dict.get("SNAPI")
        metric_transform_config = startup_dict.get("MetricTransform")
        self.assertIsInstance(general_config, models.GeneralConfig)
        self.assertIsInstance(vault_config, models.VaultConfig)
        self.assertIsInstance(snapi_config, models.SNAPIConfig)
        self.assertIsInstance(metric_transform_config,
                              models.MetricTransformConfig)
        self.assertEqual(general_config.listening_port, 8888)
        self.assertEqual(general_config.listening_address, "0.0.0.0")
        self.assertEqual(vault_config.address, "10.0.3.1")
        self.assertEqual(snapi_config.address, "10.1.124.236")
        self.assertEqual(metric_transform_config.loss_use_bytes, False)

    @patch("config_manager.config_manager.ConfigManager.read_file_cfg")
    def test_store_startup_conf_empty(self, mock_read_cfg):
        mock_read_cfg.return_value = {}
        self.config_manager.store_startup_conf()
        self.assertIn("General", self.config_manager.config_store.
                      startup_config)
        self.assertIn("Vault", self.config_manager.config_store.
                      startup_config)
        self.assertIn("SNAPI", self.config_manager.config_store.
                      startup_config)

        startup_dict = self.config_manager.config_store.startup_config
        general_config = startup_dict.get("General")
        vault_config = startup_dict.get("Vault")
        snapi_config = startup_dict.get("SNAPI")
        metric_transform_config = startup_dict.get("MetricTransform")
        self.assertEqual(general_config.listening_port, 4444)
        self.assertEqual(general_config.listening_address, "0.0.0.0")
        self.assertEqual(general_config.input_buffer_chunk_size, 4096)
        self.assertEqual(general_config.vault_location, "edge")
        self.assertEqual(general_config.vault_env, "prod")
        self.assertEqual(vault_config.address, "https://edge.vault:8200")
        self.assertEqual(
            vault_config.path,
            "plain-text/prod/edge/fleet-edge/internal/snapi/private/token")
        self.assertEqual(vault_config.key, "/var/vault.key")
        self.assertEqual(vault_config.token, "/var/vault.token")
        self.assertEqual(vault_config.cert_path, "/var/vault.crt")
        self.assertEqual(vault_config.auth_method, "token")
        self.assertEqual(vault_config.vault_role, "deploy")
        self.assertEqual(snapi_config.address, "0.0.0.0")
        self.assertEqual(snapi_config.port, 8081)
        self.assertEqual(metric_transform_config.loss_use_bytes, False)
        self.assertEqual(metric_transform_config.include_timedout_in_loss,
                         True)

    @patch("config_manager.config_manager.ConfigManager.read_file_txt")
    def test_store_metrics_conf_success(self, mock_read_txt):
        mock_read_txt.return_value = ["RazorLink.Internal.*.snapshot",
                                      "RazorLink.Internal.*.qcow2"]
        self.config_manager.store_metrics_conf()
        self.assertEqual(self.config_manager.config_store.metrics_config,
                         mock_read_txt.return_value)

    @patch("config_manager.config_manager.ConfigManager.read_file_txt")
    def test_store_logs_conf_success(self, mock_read_txt):
        mock_read_txt.return_value = [("RazorLink.Internal.Razor.Tenants."
                                       "Warning No variable was found"),
                                      ("RazorLink.Internal.Razor.Tenants.Error"
                                       " Encountered error in component")]
        self.config_manager.store_logs_conf()
        self.assertEqual(self.config_manager.config_store.logs_config,
                         mock_read_txt.return_value)

    def test_watch_files(self):
        self.config_manager.watch_files()
        self.assertTrue(self.config_manager.observer.is_alive())

    def test_stop_watcher(self):
        self.config_manager.stop()
        self.assertTrue(not self.config_manager.observer.is_alive())
