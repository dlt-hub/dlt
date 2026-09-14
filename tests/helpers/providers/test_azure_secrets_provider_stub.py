"""Tests for AzureKeyVaultProvider using a stubbed Key Vault secret client."""

from typing import Any, List, Optional, Tuple

import pytest

from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.configuration.providers.azure_secrets import AzureKeyVaultProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import AzureKeyVaultCredentials
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersConfiguration,
    _azure_secrets_provider,
)
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment

VAULT_URL = "https://test-vault.vault.azure.net"


class _FakeSecret:
    def __init__(self, value: Optional[str]) -> None:
        self.value = value


class _FakeSecretProperties:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeSecretClient:
    """Minimal stand-in for azure.keyvault.secrets.SecretClient"""

    def __init__(self, secrets: Any = None, get_error: Any = None, list_error: Any = None) -> None:
        self._secrets = secrets or {}
        self._get_error = get_error
        self._list_error = list_error

    def get_secret(self, name: str) -> _FakeSecret:
        if self._get_error is not None:
            raise self._get_error
        from azure.core.exceptions import ResourceNotFoundError

        if name not in self._secrets:
            raise ResourceNotFoundError(message=f"Secret {name} not found")
        return _FakeSecret(self._secrets[name])

    def list_properties_of_secrets(self) -> List[_FakeSecretProperties]:
        if self._list_error is not None:
            raise self._list_error
        return [_FakeSecretProperties(name) for name in self._secrets]


def _make_provider(client: _FakeSecretClient, **settings: Any) -> AzureKeyVaultProvider:
    credentials = AzureKeyVaultCredentials(azure_key_vault_url=VAULT_URL)
    provider = AzureKeyVaultProvider(credentials, **settings)
    # inject the fake client so no live Key Vault is contacted
    provider._client = client
    return provider


@pytest.mark.parametrize(
    "key,sections,expected",
    (
        ("credentials", ("sources", "my_source"), "sources-my-source-credentials"),
        ("dlt_secrets_toml", ("pipeline x !!",), "pipelinex-dlt-secrets-toml"),
        ("api-key", (), "api-key"),
        ("secret", ("destination", None, "bigquery"), "destination-bigquery-secret"),
    ),
    ids=["underscores_to_dashes", "punctuation_stripped", "no_sections", "empty_sections_filtered"],
)
def test_get_key_name(key: str, sections: Tuple[str, ...], expected: str) -> None:
    # Azure Key Vault secret names allow only [A-Za-z0-9-], so all underscores and other
    # punctuation are converted to dashes or stripped
    name = AzureKeyVaultProvider.get_key_name(key, *sections)
    assert name == expected
    assert "_" not in name


def test_look_vault_returns_secret_value() -> None:
    pytest.importorskip("azure.core")
    provider = _make_provider(_FakeSecretClient({"sources-my-source": "SRC_KEY"}))
    assert provider._look_vault("sources-my-source", TSecretValue) == "SRC_KEY"


def test_look_vault_missing_secret_is_none() -> None:
    pytest.importorskip("azure.core")
    provider = _make_provider(_FakeSecretClient({}))
    assert provider._look_vault("sources-my-source", TSecretValue) is None


def test_look_vault_forbidden_is_none() -> None:
    pytest.importorskip("azure.core")
    from azure.core.exceptions import HttpResponseError

    error = HttpResponseError(message="Forbidden")
    error.status_code = 403
    provider = _make_provider(_FakeSecretClient(get_error=error))
    assert provider._look_vault("sources-my-source", TSecretValue) is None


def test_look_vault_auth_error_is_none() -> None:
    pytest.importorskip("azure.core")
    from azure.core.exceptions import ClientAuthenticationError

    provider = _make_provider(_FakeSecretClient(get_error=ClientAuthenticationError(message="nope")))
    assert provider._look_vault("sources-my-source", TSecretValue) is None


def test_look_vault_reraises_unexpected_http_error() -> None:
    pytest.importorskip("azure.core")
    from azure.core.exceptions import HttpResponseError

    error = HttpResponseError(message="Boom")
    error.status_code = 500
    provider = _make_provider(_FakeSecretClient(get_error=error))
    with pytest.raises(HttpResponseError):
        provider._look_vault("sources-my-source", TSecretValue)


def test_list_vault_returns_names() -> None:
    pytest.importorskip("azure.core")
    provider = _make_provider(
        _FakeSecretClient({"sources-a": "1", "sources-b": "2"}), list_secrets=True
    )
    assert provider._list_vault() == {"sources-a", "sources-b"}


def test_list_vault_raises_config_provider_exception() -> None:
    pytest.importorskip("azure.core")
    from azure.core.exceptions import ClientAuthenticationError

    provider = _make_provider(
        _FakeSecretClient(list_error=ClientAuthenticationError(message="denied")),
        list_secrets=True,
    )
    with pytest.raises(ConfigProviderException):
        provider._list_vault()


def test_locations_returns_vault_url() -> None:
    provider = _make_provider(_FakeSecretClient({}))
    assert provider.locations == [VAULT_URL]


def test_azure_secrets_provider_factory(environment: Any) -> None:
    environment["PROVIDERS__ENABLE_AZURE_SECRETS"] = "true"
    environment["PROVIDERS__AZURE_SECRETS__LIST_SECRETS"] = "true"
    environment["PROVIDERS__AZURE_SECRETS__CREDENTIALS__AZURE_KEY_VAULT_URL"] = VAULT_URL
    environment["PROVIDERS__AZURE_SECRETS__CREDENTIALS__AZURE_TENANT_ID"] = "tenant"
    environment["PROVIDERS__AZURE_SECRETS__CREDENTIALS__AZURE_CLIENT_ID"] = "client"
    environment["PROVIDERS__AZURE_SECRETS__CREDENTIALS__AZURE_CLIENT_SECRET"] = "secret"

    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    assert providers_config.enable_azure_secrets is True

    provider = _azure_secrets_provider(providers_config.azure_secrets)
    assert isinstance(provider, AzureKeyVaultProvider)
    assert provider.list_secrets is True
    assert provider.only_secrets is True
    assert provider.only_toml_fragments is True
    assert provider.credentials.azure_key_vault_url == VAULT_URL
    assert provider.locations == [VAULT_URL]
