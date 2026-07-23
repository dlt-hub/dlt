"""Tests for AzureKeyVaultProvider using a mocked SecretClient."""

from typing import Any, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.configuration.providers.azure_key_vault import AzureKeyVaultProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.config_providers_context import (
    AzureKeyVaultProviderConfiguration,
    ConfigProvidersConfiguration,
    _azure_key_vault_provider,
)
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


VAULT_URL = "https://test-vault.vault.azure.net/"


def _make_provider(**settings: Any) -> Tuple[AzureKeyVaultProvider, MagicMock]:
    mock_credential = MagicMock()
    provider = AzureKeyVaultProvider(
        vault_url=VAULT_URL, credential=mock_credential, **settings
    )
    mock_client = MagicMock()
    provider._client = mock_client
    return provider, mock_client


def _resource_not_found_error() -> Exception:
    from azure.core.exceptions import ResourceNotFoundError

    return ResourceNotFoundError("Secret not found")


def _http_response_error(status_code: int, message: str = "error") -> Exception:
    from azure.core.exceptions import HttpResponseError

    error = HttpResponseError(message=message)
    error.status_code = status_code
    return error


def _make_secret_mock(value: str) -> MagicMock:
    secret = MagicMock()
    secret.value = value
    return secret


def _make_secret_properties(name: str, enabled: bool = True) -> MagicMock:
    props = MagicMock()
    props.name = name
    props.enabled = enabled
    return props


def test_look_vault_returns_secret_value() -> None:
    provider, client = _make_provider()
    client.get_secret.return_value = _make_secret_mock("my-secret-value")
    assert provider._look_vault("sources-my_source", TSecretValue) == "my-secret-value"
    client.get_secret.assert_called_once_with("sources-my_source")


def test_look_vault_returns_none_on_not_found() -> None:
    provider, client = _make_provider()
    client.get_secret.side_effect = _resource_not_found_error()
    assert provider._look_vault("sources-my_source", TSecretValue) is None


def test_look_vault_returns_none_on_403() -> None:
    provider, client = _make_provider()
    client.get_secret.side_effect = _http_response_error(403, "Forbidden")
    assert provider._look_vault("sources-my_source", TSecretValue) is None


def test_look_vault_raises_on_other_http_error() -> None:
    from azure.core.exceptions import HttpResponseError

    provider, client = _make_provider()
    client.get_secret.side_effect = _http_response_error(500, "Internal Server Error")
    with pytest.raises(HttpResponseError):
        provider._look_vault("sources-my_source", TSecretValue)


def test_list_vault_lists_enabled_secrets() -> None:
    provider, client = _make_provider(list_secrets=True)
    client.list_properties_of_secrets.return_value = [
        _make_secret_properties("sources-a"),
        _make_secret_properties("sources-b"),
        _make_secret_properties("disabled-secret", enabled=False),
    ]
    assert provider._list_vault() == {"sources-a", "sources-b"}


def test_list_vault_raises_config_provider_exception_on_403() -> None:
    provider, client = _make_provider(list_secrets=True)
    client.list_properties_of_secrets.side_effect = _http_response_error(403, "Forbidden")
    with pytest.raises(ConfigProviderException) as exc_info:
        provider._list_vault()
    assert "access denied" in str(exc_info.value).lower()


def test_list_vault_raises_config_provider_exception_on_other_error() -> None:
    provider, client = _make_provider(list_secrets=True)
    client.list_properties_of_secrets.side_effect = _http_response_error(500, "Server Error")
    with pytest.raises(ConfigProviderException):
        provider._list_vault()


@pytest.mark.parametrize(
    "key,sections,expected",
    (
        ("credentials", ("sources", "my_source"), "sources-my-source-credentials"),
        ("dlt_secrets_toml", ("pipeline x !!",), "pipelinex-dlt-secrets-toml"),
        ("api-key", (), "api-key"),
        ("secret", ("destination", None, "bigquery"), "destination-bigquery-secret"),
    ),
    ids=["sections_joined", "punctuation_stripped", "no_sections", "empty_sections_filtered"],
)
def test_get_key_name(key: str, sections: Tuple[str, ...], expected: str) -> None:
    assert AzureKeyVaultProvider.get_key_name(key, *sections) == expected


def test_provider_name() -> None:
    provider, _ = _make_provider()
    assert provider.name == "Azure Key Vault"


def test_provider_locations() -> None:
    provider, _ = _make_provider()
    assert provider.locations == [VAULT_URL]


def test_provider_supports_secrets() -> None:
    provider, _ = _make_provider()
    assert provider.supports_secrets is True


def test_provider_client_created_once() -> None:
    mock_credential = MagicMock()
    provider = AzureKeyVaultProvider(vault_url=VAULT_URL, credential=mock_credential)
    mock_secret_client_cls = MagicMock()
    mock_module = MagicMock()
    mock_module.SecretClient = mock_secret_client_cls
    with patch.dict("sys.modules", {"azure.keyvault.secrets": mock_module}):
        client1 = provider._get_client()
        client2 = provider._get_client()
        assert client1 is client2
        mock_secret_client_cls.assert_called_once()


def test_factory_creates_provider(environment: Any) -> None:
    environment["PROVIDERS__ENABLE_AZURE_KEY_VAULT"] = "true"
    environment["PROVIDERS__AZURE_KEY_VAULT__VAULT_URL"] = VAULT_URL
    environment["PROVIDERS__AZURE_KEY_VAULT__LIST_SECRETS"] = "true"

    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    assert providers_config.enable_azure_key_vault is True

    with patch(
        "dlt.common.configuration.providers.azure_key_vault.AzureKeyVaultProvider._get_client"
    ):
        provider = _azure_key_vault_provider(providers_config.azure_key_vault)
        assert isinstance(provider, AzureKeyVaultProvider)
        assert provider.vault_url == VAULT_URL
        assert provider.list_secrets is True
        assert provider.only_secrets is True
        assert provider.only_toml_fragments is True
        assert provider.locations == [VAULT_URL]


def test_factory_raises_without_vault_url(environment: Any) -> None:
    environment["PROVIDERS__ENABLE_AZURE_KEY_VAULT"] = "true"

    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    with pytest.raises(ConfigProviderException) as exc_info:
        _azure_key_vault_provider(providers_config.azure_key_vault)
    assert "vault_url" in str(exc_info.value)


def test_default_azure_credential_fallback() -> None:
    with patch("azure.identity.DefaultAzureCredential") as mock_dac:
        mock_dac.return_value = MagicMock()
        provider = AzureKeyVaultProvider(vault_url=VAULT_URL)
        cred = provider._get_credential()
        mock_dac.assert_called_once()
        assert cred is mock_dac.return_value
