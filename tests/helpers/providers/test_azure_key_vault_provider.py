"""Integration tests for AzureKeyVaultProvider against a real Azure Key Vault.

Requires a vault reachable with the ambient Azure credentials (`az login` is enough) and the
secrets listed in `REQUIRED_SECRETS` below. Set `DLT_TEST_AZURE_KEY_VAULT_URL` to point at your
own vault. Tests skip when the vault is unset or unreachable, so they are safe to run anywhere.

Create the fixtures with:

    az keyvault secret set --vault-name <vault> --name dlt-pr-test--api-key --value sekret-value-42
    az keyvault secret set --vault-name <vault> --name dlt-pr-test--nested--token --value nested-token-99
"""

import os
from typing import Any, Iterator

import pytest

from dlt.common.configuration.providers.azure_key_vault import AzureKeyVaultProvider
from dlt.common.typing import TSecretValue

VAULT_URL = os.environ.get("DLT_TEST_AZURE_KEY_VAULT_URL")

REQUIRED_SECRETS = {
    "dlt-pr-test--api-key": "sekret-value-42",
    "dlt-pr-test--nested--token": "nested-token-99",
}

pytestmark = pytest.mark.skipif(not VAULT_URL, reason="DLT_TEST_AZURE_KEY_VAULT_URL is not set")


def _provider_or_skip(**settings: Any) -> AzureKeyVaultProvider:
    """Provider for a vault that holds the fixture secrets, skipping when it does not."""
    p = AzureKeyVaultProvider(vault_url=VAULT_URL, **settings)
    try:
        p._get_client().get_secret("dlt-pr-test--api-key")
    except Exception as ex:  # noqa: BLE001
        pytest.skip(f"Key Vault {VAULT_URL} has no test fixtures or is not reachable: {ex}")
    return p


@pytest.fixture(scope="module")
def provider() -> Iterator[AzureKeyVaultProvider]:
    yield _provider_or_skip()


@pytest.fixture(scope="module")
def listing_provider() -> Iterator[AzureKeyVaultProvider]:
    yield _provider_or_skip(list_secrets=True)


def test_look_vault_reads_real_secret(provider: AzureKeyVaultProvider) -> None:
    assert provider._look_vault("dlt-pr-test--api-key", TSecretValue) == "sekret-value-42"


def test_look_vault_reads_sectioned_secret(provider: AzureKeyVaultProvider) -> None:
    assert provider._look_vault("dlt-pr-test--nested--token", TSecretValue) == "nested-token-99"


def test_look_vault_returns_none_for_missing_secret(provider: AzureKeyVaultProvider) -> None:
    assert provider._look_vault("dlt-pr-test--does-not-exist", TSecretValue) is None


def test_list_vault_includes_test_secrets(listing_provider: AzureKeyVaultProvider) -> None:
    try:
        names = listing_provider._list_vault()
    except Exception as ex:  # noqa: BLE001
        pytest.skip(f"listing not permitted on {VAULT_URL}: {ex}")
    assert REQUIRED_SECRETS.keys() <= names


@pytest.fixture(scope="module")
def exact_key_provider() -> Iterator[AzureKeyVaultProvider]:
    """Provider that also looks up exact keys, not just the known toml fragments."""
    yield _provider_or_skip(only_toml_fragments=False)


def test_get_value_normalizes_underscores(exact_key_provider: AzureKeyVaultProvider) -> None:
    """`api_key` under section `dlt_pr_test` resolves to the `dlt-pr-test--api-key` secret."""
    value, key = exact_key_provider.get_value("api_key", TSecretValue, None, "dlt_pr_test")
    assert key == "dlt-pr-test--api-key"
    assert value == "sekret-value-42"


def test_get_value_walks_sections(exact_key_provider: AzureKeyVaultProvider) -> None:
    value, key = exact_key_provider.get_value("token", TSecretValue, None, "dlt_pr_test", "nested")
    assert key == "dlt-pr-test--nested--token"
    assert value == "nested-token-99"


def test_only_toml_fragments_skips_exact_key(provider: AzureKeyVaultProvider) -> None:
    """With the default `only_toml_fragments`, an exact key is not fetched from the vault."""
    value, key = provider.get_value("api_key", TSecretValue, None, "dlt_pr_test")
    assert key == "dlt-pr-test--api-key"
    assert value is None


def test_only_secrets_skips_non_secret_hint() -> None:
    """With the default `only_secrets`, a non-secret hint is not fetched from the vault.

    Uses a fresh provider: a shared one would serve the value from its cached toml document,
    which is populated by any earlier lookup of the same key.
    """
    fresh = _provider_or_skip(only_toml_fragments=False)
    value, _ = fresh.get_value("api_key", str, None, "dlt_pr_test")
    assert value is None


def test_client_is_cached(provider: AzureKeyVaultProvider) -> None:
    assert provider._get_client() is provider._get_client()


def test_default_azure_credential_authenticates() -> None:
    """Outside the Fabric runtime the provider signs in with DefaultAzureCredential."""
    p = AzureKeyVaultProvider(vault_url=VAULT_URL)
    credential = p._get_credential()
    assert type(credential).__name__ == "DefaultAzureCredential"
    token = credential.get_token("https://vault.azure.net/.default")
    assert token.token.count(".") == 2
