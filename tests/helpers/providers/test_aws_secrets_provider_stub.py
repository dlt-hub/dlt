"""Tests for AwsSecretsManagerProvider using a stubbed botocore client."""

from pathlib import Path
from typing import Any, Optional, Tuple

import pytest

pytest.importorskip("botocore")

from botocore.exceptions import ClientError
from botocore.stub import Stubber

from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.configuration.providers.aws_secrets import AwsSecretsManagerProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.configuration.specs.config_providers_context import (
    AwsSecretsProviderConfiguration,
    ConfigProvidersConfiguration,
    _aws_secrets_provider,
)
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


def _make_provider(**settings: Any) -> Tuple[AwsSecretsManagerProvider, Stubber]:
    # adapter tests use unprefixed secret names unless a prefix is requested explicitly
    settings.setdefault("secret_name_prefix", "")
    credentials = AwsCredentials(
        aws_access_key_id="fake_key",
        aws_secret_access_key=TSecretValue("fake_secret"),
        region_name="eu-central-1",
    )
    provider = AwsSecretsManagerProvider(credentials, **settings)
    stubber = Stubber(provider._get_client())
    stubber.activate()
    return provider, stubber


def test_default_secret_name_prefix() -> None:
    credentials = AwsCredentials(
        aws_access_key_id="fake_key", aws_secret_access_key=TSecretValue("fake_secret")
    )
    assert AwsSecretsManagerProvider(credentials).secret_name_prefix == "dlt/"
    assert AwsSecretsProviderConfiguration().secret_name_prefix == "dlt/"


@pytest.mark.parametrize(
    "response,expected",
    (
        ({"Name": "sources/my_source", "SecretString": "SRC_KEY"}, "SRC_KEY"),
        ({"Name": "sources/my_source", "SecretBinary": b"SRC_KEY"}, "SRC_KEY"),
        ({"Name": "sources/my_source", "SecretBinary": b"\xff\xfe\xfa"}, None),
    ),
    ids=["secret_string", "secret_binary_utf8", "secret_binary_not_utf8"],
)
def test_look_vault_secret_payload(response: Any, expected: Optional[str]) -> None:
    provider, stubber = _make_provider()
    stubber.add_response("get_secret_value", response, {"SecretId": "sources/my_source"})
    assert provider._look_vault("sources/my_source", TSecretValue) == expected
    stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    "error_code",
    (
        "ResourceNotFoundException",
        "AccessDeniedException",
        "UnrecognizedClientException",
        "InvalidRequestException",
        "ValidationException",
        "InvalidParameterException",
        "DecryptionFailure",
    ),
)
def test_look_vault_misses_on_client_error(error_code: str) -> None:
    """Errors that mean an unreadable or non-existent secret degrade to a miss."""
    provider, stubber = _make_provider()
    stubber.add_client_error("get_secret_value", service_error_code=error_code)
    assert provider._look_vault("sources/my_source", TSecretValue) is None


@pytest.mark.parametrize(
    "error_code", ("ThrottlingException", "InternalServiceError", "ExpiredTokenException")
)
def test_look_vault_raises_on_transient_error(error_code: str) -> None:
    provider, stubber = _make_provider()
    # queue enough errors to survive botocore internal retries
    for _ in range(10):
        stubber.add_client_error("get_secret_value", service_error_code=error_code)
    with pytest.raises(ClientError):
        provider._look_vault("sources/my_source", TSecretValue)


def test_look_vault_with_prefix() -> None:
    """Prefix is prepended to the secret name but full key stays logical."""
    provider, stubber = _make_provider(secret_name_prefix="dlt/")
    stubber.add_response(
        "get_secret_value",
        {"Name": "dlt/sources/my_source", "SecretString": "SRC_KEY"},
        {"SecretId": "dlt/sources/my_source"},
    )
    assert provider._look_vault("sources/my_source", TSecretValue) == "SRC_KEY"
    stubber.assert_no_pending_responses()


def test_list_vault_paginates() -> None:
    provider, stubber = _make_provider(list_secrets=True)
    stubber.add_response(
        "list_secrets", {"SecretList": [{"Name": "sources/a"}], "NextToken": "next-1"}, {}
    )
    stubber.add_response(
        "list_secrets", {"SecretList": [{"Name": "sources/b"}]}, {"NextToken": "next-1"}
    )
    assert provider._list_vault() == {"sources/a", "sources/b"}
    stubber.assert_no_pending_responses()


def test_list_vault_with_prefix_filters_and_strips() -> None:
    """Listing filters server-side by name prefix and returns keys without the prefix."""
    provider, stubber = _make_provider(secret_name_prefix="dlt/")
    stubber.add_response(
        "list_secrets",
        {"SecretList": [{"Name": "dlt/sources/a"}, {"Name": "dlt/"}, {"Name": "other/x"}]},
        {"Filters": [{"Key": "name", "Values": ["dlt/"]}]},
    )
    assert provider._list_vault() == {"sources/a"}
    stubber.assert_no_pending_responses()


def test_list_vault_raises_config_provider_exception() -> None:
    provider, stubber = _make_provider(list_secrets=True)
    stubber.add_client_error("list_secrets", service_error_code="AccessDeniedException")
    with pytest.raises(ConfigProviderException) as exc_info:
        provider._list_vault()
    assert "secretsmanager:ListSecrets" in str(exc_info.value)

    provider, stubber = _make_provider(list_secrets=True)
    stubber.add_client_error("list_secrets", service_error_code="InternalServiceError")
    with pytest.raises(ConfigProviderException):
        provider._list_vault()


@pytest.mark.parametrize(
    "key,sections,expected",
    (
        ("credentials", ("sources", "my_source"), "sources/my_source/credentials"),
        ("dlt_secrets_toml", ("pipeline x !!",), "pipelinex/dlt_secrets_toml"),
        ("api-key", (), "api-key"),
        ("secret", ("destination", None, "bigquery"), "destination/bigquery/secret"),
    ),
    ids=["sections_joined", "punctuation_stripped", "no_sections", "empty_sections_filtered"],
)
def test_get_key_name(key: str, sections: Tuple[str, ...], expected: str) -> None:
    assert AwsSecretsManagerProvider.get_key_name(key, *sections) == expected


def test_client_created_once() -> None:
    provider, _ = _make_provider()
    assert provider._get_client() is provider._get_client()


def test_missing_region_raises_config_provider_exception(environment: Any, tmp_path: Path) -> None:
    # point config file to non existing path so profile region is not used
    environment["AWS_CONFIG_FILE"] = str(tmp_path / "config")
    credentials = AwsCredentials(
        aws_access_key_id="fake_key", aws_secret_access_key=TSecretValue("fake_secret")
    )
    provider = AwsSecretsManagerProvider(credentials)
    with pytest.raises(ConfigProviderException) as exc_info:
        provider._get_client()
    assert "region" in str(exc_info.value)


def test_aws_secrets_provider_factory(environment: Any) -> None:
    environment["PROVIDERS__ENABLE_AWS_SECRETS"] = "true"
    environment["PROVIDERS__AWS_SECRETS__LIST_SECRETS"] = "true"
    environment["PROVIDERS__AWS_SECRETS__SECRET_NAME_PREFIX"] = "team/dlt/"
    environment["PROVIDERS__AWS_SECRETS__CREDENTIALS__AWS_ACCESS_KEY_ID"] = "fake_key"
    environment["PROVIDERS__AWS_SECRETS__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "fake_secret"
    environment["PROVIDERS__AWS_SECRETS__CREDENTIALS__REGION_NAME"] = "eu-central-1"

    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    assert providers_config.enable_aws_secrets is True

    provider = _aws_secrets_provider(providers_config.aws_secrets)
    assert isinstance(provider, AwsSecretsManagerProvider)
    assert provider.secret_name_prefix == "team/dlt/"
    assert provider.list_secrets is True
    assert provider.only_secrets is True
    assert provider.only_toml_fragments is True
    assert provider.credentials.region_name == "eu-central-1"
    assert provider.locations == ["fake_key@eu-central-1:team/dlt/"]
