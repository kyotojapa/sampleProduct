"""Unit tests from the new vault method"""
import shutil
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from src.config_manager.models import GeneralConfig, VaultConfig
from src.controller.common import (
    SecretNotReadException, extract_token_from_dict,
    get_edge_vault_creds, get_secret_based_on_vault_location)

# pylint: disable=no-member


class TestVault(TestCase):
    """Method that tests the vault functions"""

    @classmethod
    def setUpClass(cls) -> None:
        "Test set up method"

        vault_cfg_dict = {
            "address": "https://edge.vault:8888",
            "path": (
                "plain-text/prod/edge/fleet-edge/internal/snapi/private/token"),
            "key": f"{Path.home()}" + "/vaultfiles/vault.key",
            "token": f"{Path.home()}" + "/vaultfiles/vault.token",
            "cert_path": f"{Path.home()}" + "/vaultfiles/vault.crt",
            "auth_method": "token",
            "vault_role": "deploy"
        }
        gen_cfg_dict = {
            "vault_location": "edge",
            "vault_env": "prod"
        }
        cls.gen_config = GeneralConfig.parse_obj(gen_cfg_dict)
        cls.vault_config = VaultConfig.parse_obj(vault_cfg_dict)
        cls.vault_key_path = f"{Path.home()}" + "/vaultfiles/vault.key"
        cls.vault_token_path = f"{Path.home()}" + "/vaultfiles/vault.token"
        Path(cls.vault_key_path).parent.mkdir(exist_ok=True, parents=True)
        Path(cls.vault_token_path).parent.mkdir(exist_ok=True, parents=True)
        with Path(cls.vault_key_path).open(
                encoding='utf-8', mode='w+') as key_file:
            key_file.write("akey")
        with Path(cls.vault_token_path).open(
                encoding='utf-8', mode='w+') as token_file:
            token_file.write("atoken")

    @classmethod
    def tearDownClass(cls):
        """Tear Down method"""

        shutil.rmtree(Path.home() / "vaultfiles")

    def test_get_edge_vault_creds_success(self):
        """Test successfull retrieval of key and token"""

        key, tok = get_edge_vault_creds(TestVault.vault_config)
        self.assertEqual(tok, "atoken")
        self.assertEqual(key, "akey")

    def test_extract_token_from_dict(self):
        """Test successfull retrieval of key and token"""

        token = extract_token_from_dict({"data": {"value": "AValue"}})
        self.assertEqual(token, "AValue")

    @patch("src.controller.common.get_secret")
    def test_get_secret_based_on_vault_location_edge(self, mock_get_secret):
        """Test get_secret_from_vault_on_location method"""

        TestVault.gen_config.vault_location = "edge"
        mock_get_secret.retu = {"data": {"value": "AValue"}}
        result = get_secret_based_on_vault_location(TestVault.vault_config,
                                                    TestVault.gen_config)
        mock_get_secret.assert_called_with(
            TestVault.vault_config.path,
            vault_env=TestVault.gen_config.vault_env,
            vault_url=TestVault.vault_config.address,
            cert_path=TestVault.vault_config.cert_path,
            vault_auth_method=TestVault.vault_config.auth_method,
            key="akey", token="atoken")

    @patch("src.controller.common.get_secret")
    def test_get_secret_based_on_vault_location_aws(self, mock_get_secret):
        """Test get_secret_from_vault_on_location method"""

        TestVault.gen_config.vault_location = "shore"

        secret = get_secret_based_on_vault_location(TestVault.vault_config,
                                                    TestVault.gen_config)
        self.assertEqual(secret, "SNAPIl1vew1re")

    def test_get_secret_based_on_vault_location_raise_exc(self):
        """Test get_secret_from_vault_on_location method"""

        TestVault.gen_config.vault_location = "unexepect_location"

        with self.assertRaises(SecretNotReadException):
            get_secret_based_on_vault_location(TestVault.vault_config,
                                               TestVault.gen_config)
