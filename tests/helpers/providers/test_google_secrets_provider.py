from dlt import TSecretValue

import pytest

from dlt.common.configuration.specs import GcpServiceAccountCredentials, CredentialsConfiguration
from dlt.common.configuration.providers.google_secrets import GoogleSecretsProvider
from dlt.common.configuration.accessors import secrets
from dlt.common.configuration.specs.config_providers_context import (
    _google_secrets_provider,
    VaultProviderConfiguration,
)
from dlt.common.configuration.specs import GcpServiceAccountCredentials, known_sections
from dlt.common.typing import AnyType
from dlt.common.configuration.resolve import resolve_configuration

from tests.utils import init_test_logging
from tests.conftest import CACHED_GOOGLE_SECRETS


DLT_SECRETS_TOML_CONTENT = """
secret_value = 2137

[api]
secret_key = "ABCD"

[credentials]
secret_value = "2138"
project_id = "mock-credentials"
"""


@pytest.mark.parametrize(
    "settings",
    (VaultProviderConfiguration(), VaultProviderConfiguration(False, False, True)),
    ids=["default_settings", "list_vault"],
)
def test_regular_keys(settings: VaultProviderConfiguration) -> None:
    init_test_logging()
    CACHED_GOOGLE_SECRETS.clear()

    # copy bigquery credentials into providers credentials
    c = resolve_configuration(
        GcpServiceAccountCredentials(), sections=(known_sections.DESTINATION, "bigquery")
    )
    secrets[f"{known_sections.PROVIDERS}.google_secrets.credentials"] = dict(c)
    # c = secrets.get("destination.credentials", GcpServiceAccountCredentials)
    # print(c)
    provider: GoogleSecretsProvider = _google_secrets_provider(settings)  # type: ignore[assignment]
    # get non existing value, that will load DLT_SECRETS_TOML_CONTENT
    assert provider.get_value("secret_value", AnyType, "pipeline x !!") == (
        None,
        "pipelinex-secret_value",
    )
    assert provider.to_toml().strip() == DLT_SECRETS_TOML_CONTENT.strip()
    # check location
    assert "chat-analytics" in provider.locations[0]

    assert provider.get_value("secret_value", AnyType, None) == (2137, "secret_value")
    assert provider.get_value("secret_key", AnyType, None, "api") == ("ABCD", "api-secret_key")

    # skip when we look for all types okf keys
    if settings.only_toml_fragments:
        # load secrets toml for secret_key to be visible
        provider.get_value("secret_key", AnyType, "pipeline", "api")

    # only_secrets won't see AnyType as secret
    assert provider.get_value("secret_key", AnyType, "pipeline", "api") == (
        None if settings.only_secrets else "ABCDE",
        "pipeline-api-secret_key",
    )
    assert provider.get_value("credentials", CredentialsConfiguration, "pipeline") == (
        {"project_id": "mock-credentials-pipeline"},
        "pipeline-credentials",
    )

    # load source test_source which should also load "sources", "pipeline-sources", "sources-test_source" and "pipeline-sources-test_source"
    assert provider.get_value(
        "only_pipeline", TSecretValue, "pipeline", "sources", "test_source"
    ) == (
        "ONLY",
        "pipeline-sources-test_source-only_pipeline",
    )
    # we set sources.test_source.secret_prop_1="OVR_A" in pipeline-sources to override value in sources
    assert provider.get_value("secret_prop_1", AnyType, None, "sources", "test_source") == (
        "OVR_A",
        "sources-test_source-secret_prop_1",
    )
    # get element unique to pipeline-sources
    assert provider.get_value("only_pipeline_top", AnyType, "pipeline", "sources") == (
        "TOP",
        "pipeline-sources-only_pipeline_top",
    )
    # get element unique to sources
    assert provider.get_value("all_sources_present", AnyType, None, "sources") == (
        True,
        "sources-all_sources_present",
    )
    # get element unique to sources-test_source
    assert provider.get_value("secret_prop_2", AnyType, None, "sources", "test_source") == (
        "B",
        "sources-test_source-secret_prop_2",
    )

    # this destination will not be found
    assert provider.get_value("url", AnyType, "pipeline", "destination", "filesystem") == (
        None,
        "pipeline-destination-filesystem-url",
    )

    # try a single secret value - not found until single values enabled
    if provider.only_toml_fragments:
        assert provider.get_value("secret", TSecretValue, "pipeline") == (None, "pipeline-secret")

    # enable the single secrets
    provider.only_toml_fragments = False
    assert provider.get_value("secret", TSecretValue, "pipeline") == (
        "THIS IS SECRET VALUE",
        "pipeline-secret",
    )
    del provider._config_doc["pipeline"]["secret"]
    provider.clear_lookup_cache()

    # but request as not secret value -> still not found
    if provider.only_secrets:
        assert provider.get_value("secret", str, "pipeline") == (None, "pipeline-secret")
    provider.only_secrets = False
    # non secrets allowed
    assert provider.get_value("secret", str, "pipeline") == (
        "THIS IS SECRET VALUE",
        "pipeline-secret",
    )

    # request json
    # print(provider._toml.as_string())
    assert provider.get_value("halo", str, "halo") == ({"halo": True}, "halo-halo")
    assert provider.get_value("halo", bool, "halo", "halo") == (True, "halo-halo-halo")
