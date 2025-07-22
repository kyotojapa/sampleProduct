"""Utility custom exceptions, Enum and function used by Controller module."""
from enum import Enum, auto
from pathlib import Path
from typing import Tuple

from productutils.secretsmanager import get_secret

from config_manager import models


def get_secret_based_on_vault_location(vault_cfg: models.VaultConfig,
                                       gen_cfg: models.GeneralConfig) -> str:
    """For SEEP RLF having 'vault_location = edge', the function calls
    get_secret from productutils library to retrieve SNAPI secret from Vault.
    A dictionary is returned from which the secret is extracted.
    For SDEP RLF having 'vault_location = shore', the function returns the
    SNAPI secret directly.

    Args:
        vault_cfg (models.VaultConfig): Uses Vault configuration details
        gen_cfg (models.GeneralConfig): Uses vault_location, vault_env values
        from [General] config

    Returns:
        str: Retrieved SNAPI secret as a string value

    Raises:
        SecretNotReadException
    """
    secret_data_dict = ''
    if gen_cfg.vault_location == "edge":
        key, token = get_edge_vault_creds(vault_cfg)
        if None not in (key, token):
            secret_data_dict = get_secret(
                vault_cfg.path, vault_env=gen_cfg.vault_env,
                vault_url=vault_cfg.address, cert_path=vault_cfg.cert_path,
                vault_auth_method=vault_cfg.auth_method, key=key, token=token)
    elif gen_cfg.vault_location == "shore":
        return "SNAPIl1vew1re"

    token = extract_token_from_dict(secret_data_dict)

    if token:
        return token

    raise SecretNotReadException


def extract_token_from_dict(secret_data_dict):
    "Method to extract secret from dictionary"

    if (hasattr(secret_data_dict, "get")
            and hasattr(secret_data_dict.get('data'), "get")
            and secret_data_dict.get('data').get('value') is not None):
        return secret_data_dict.get('data').get('value')

    return None


def get_edge_vault_creds(vault_cfg: models.VaultConfig) -> Tuple[str, str]:
    """Method to get edge vault token and key tuple"""

    with Path(vault_cfg.key).open(encoding='utf-8', mode='r') as key_file:
        with Path(vault_cfg.token).open(encoding='utf-8', mode='r') as tk_file:
            return (key_file.read().strip("\n\r "),
                    tk_file.read().strip("\n\r "))

    return None, None


class RLFException(Exception):
    "RLF Exception"


class RLFCriticalException(RLFException):
    "RLF Critical Exception"


class SecretNotReadException(RLFException):
    "Secret Not Read Exception"


class ComponentNotInitialized(RLFException):
    "Component Not Initialized Exception"
    def __init__(self, component: str, *args: object) -> None:
        super().__init__(f"Component {component} not initialized.", *args)


class NotBooted(RLFException):
    "Not Booted Exception"
    def __init__(self) -> None:
        super().__init__("RLF not booted. Call 'boot' method first.")


class OTELTransformationFunctionsNotLoaded(RLFException):
    "Raised when the OTEL Transformation functions were not loaded"
    def __init__(self) -> None:
        super().__init__("OTEL Transformation functions were not loaded."
                         " Check transforms directory.")


class OTELIngestionModelsNotLoaded(Exception):
    "Raised when the OTEL models for ingestion are not loaded"
    def __init__(self) -> None:
        super().__init__("OTEL Models were not loaded."
                         " Check model directory.")


class OTELConfigNotReadException(Exception):
    "Raised when the OTEL config was not read"
    def __init__(self) -> None:
        super().__init__("OTEL Config for metrics was not read."
                         " Check otel.json location.")


class OTELConfigNotValidException(Exception):
    "Raised when the OTEL config is not valid"
    def __init__(self) -> None:
        super().__init__("OTEL Config for metrics was not loaded because it"
                         " is invalid. Check otel.json.")


class ReturnCodes(int, Enum):
    "Return Codes"
    OK = auto()
    MISSING_FILES = auto()
    INCOMPLETE_CONFIG = auto()
    INVALID_CONFIG = auto()
    SOCKET_ERRORS = auto()
    VAULT_ERRORS = auto()
    GENERIC_OTHER_ERRORS = auto()
