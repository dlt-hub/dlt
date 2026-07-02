from typing import Any, Dict, Iterator, Set

import pytest

from dlt.common.configuration.accessors import secrets
from dlt.common.configuration.providers import aws_secrets
from dlt.common.configuration.providers.aws_secrets import AwsSecretsManagerProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import AwsCredentials, known_sections
from dlt.common.configuration.specs.config_providers_context import (
    AwsSecretsProviderConfiguration,
    _aws_secrets_provider,
)
from dlt.common.typing import AnyType, TSecretValue

from tests.utils import init_test_logging


SECRET_NAME_PREFIX = "dlt-ci/"
FIXTURES_REGION = "eu-central-1"
"""Region where the fixture secrets are stored"""

DLT_SECRETS_TOML_CONTENT = """\
secret_value = 2137

[api]
secret_key = "ABCD"
"""

FIXTURE_SECRETS: Dict[str, str] = {
    "dlt_secrets_toml": DLT_SECRETS_TOML_CONTENT,
    "pipeline/dlt_secrets_toml": """\
[pipeline.api]
secret_key = "ABCDE"
""",
    "sources": """\
[sources]
all_sources_present = true

[sources.test_source]
secret_prop_1 = "A"
""",
    "sources/test_source": """\
[sources.test_source]
secret_prop_2 = "B"
""",
    "pipeline/sources": """\
[pipeline.sources]
only_pipeline_top = "TOP"

[sources.test_source]
secret_prop_1 = "OVR_A"
""",
    "pipeline/sources/test_source": """\
[pipeline.sources.test_source]
only_pipeline = "ONLY"
""",
    "pipeline/secret": "THIS IS SECRET VALUE",
    "sources/json_source": '{"sources": {"json_source": {"api_key": "JSON KEY"}}}',
}

CACHED_AWS_SECRETS: Dict[str, Any] = {}


class CachedAwsSecretsManagerProvider(AwsSecretsManagerProvider):
    """Caches vault lookups across parametrized runs to limit api calls"""

    def _look_vault(self, full_key: str, hint: type) -> str:
        if full_key not in CACHED_AWS_SECRETS:
            CACHED_AWS_SECRETS[full_key] = super()._look_vault(full_key, hint)
        return CACHED_AWS_SECRETS[full_key]

    def _list_vault(self) -> Set[str]:
        key_ = "__list_vault"
        if key_ not in CACHED_AWS_SECRETS:
            CACHED_AWS_SECRETS[key_] = super()._list_vault()
        return CACHED_AWS_SECRETS[key_]


@pytest.fixture(scope="module", autouse=True)
def cached_aws_secrets_provider() -> Iterator[None]:
    aws_secrets.AwsSecretsManagerProvider = CachedAwsSecretsManagerProvider  # type: ignore[misc]
    try:
        yield
    finally:
        aws_secrets.AwsSecretsManagerProvider = AwsSecretsManagerProvider  # type: ignore[misc]


@pytest.mark.parametrize(
    "settings",
    (
        AwsSecretsProviderConfiguration(secret_name_prefix=SECRET_NAME_PREFIX),
        AwsSecretsProviderConfiguration(
            only_secrets=False,
            only_toml_fragments=False,
            list_secrets=True,
            secret_name_prefix=SECRET_NAME_PREFIX,
        ),
    ),
    ids=["default_settings", "list_vault"],
)
def test_regular_keys(settings: AwsSecretsProviderConfiguration) -> None:
    init_test_logging()
    CACHED_AWS_SECRETS.clear()

    # copy aws credentials into providers credentials, ci secrets keep them for the
    # filesystem destination
    c = resolve_configuration(AwsCredentials(), sections=(known_sections.DESTINATION, "filesystem"))
    c.region_name = FIXTURES_REGION
    secrets[f"{known_sections.PROVIDERS}.aws_secrets.credentials"] = dict(c)
    provider: AwsSecretsManagerProvider = _aws_secrets_provider(settings)  # type: ignore[assignment]
    if settings.list_secrets:
        # all fixture keys are listed, with the name prefix stripped
        assert provider._list_vault() == set(FIXTURE_SECRETS)
    # get non existing value, that will load dlt_secrets_toml
    assert provider.get_value("secret_value", AnyType, "pipeline x !!") == (
        None,
        "pipelinex/secret_value",
    )
    assert provider.to_toml().strip() == DLT_SECRETS_TOML_CONTENT.strip()
    # secret name prefix is a location
    assert SECRET_NAME_PREFIX in provider.locations[0]

    assert provider.get_value("secret_value", AnyType, None) == (2137, "secret_value")
    assert provider.get_value("secret_key", AnyType, None, "api") == ("ABCD", "api/secret_key")

    # skip when we look for all types of keys
    if settings.only_toml_fragments:
        # load pipeline scoped secrets toml for secret_key to be visible
        provider.get_value("secret_key", AnyType, "pipeline", "api")

    # only_secrets won't see AnyType as secret
    assert provider.get_value("secret_key", AnyType, "pipeline", "api") == (
        None if settings.only_secrets else "ABCDE",
        "pipeline/api/secret_key",
    )

    # load source test_source which also loads "sources", "pipeline/sources",
    # "sources/test_source" and "pipeline/sources/test_source"
    assert provider.get_value(
        "only_pipeline", TSecretValue, "pipeline", "sources", "test_source"
    ) == (
        "ONLY",
        "pipeline/sources/test_source/only_pipeline",
    )
    # sources.test_source.secret_prop_1="OVR_A" in pipeline/sources overrides value in sources
    assert provider.get_value("secret_prop_1", AnyType, None, "sources", "test_source") == (
        "OVR_A",
        "sources/test_source/secret_prop_1",
    )
    # get element unique to pipeline/sources
    assert provider.get_value("only_pipeline_top", AnyType, "pipeline", "sources") == (
        "TOP",
        "pipeline/sources/only_pipeline_top",
    )
    # get element unique to sources
    assert provider.get_value("all_sources_present", AnyType, None, "sources") == (
        True,
        "sources/all_sources_present",
    )
    # get element unique to sources/test_source
    assert provider.get_value("secret_prop_2", AnyType, None, "sources", "test_source") == (
        "B",
        "sources/test_source/secret_prop_2",
    )
    # json secret works as fragment
    assert provider.get_value("api_key", TSecretValue, None, "sources", "json_source") == (
        "JSON KEY",
        "sources/json_source/api_key",
    )

    # this destination will not be found
    assert provider.get_value("url", AnyType, "pipeline", "destination", "filesystem") == (
        None,
        "pipeline/destination/filesystem/url",
    )

    # try a single secret value - not found until single values enabled
    if provider.only_toml_fragments:
        assert provider.get_value("secret", TSecretValue, "pipeline") == (None, "pipeline/secret")

    # enable the single secrets
    provider.only_toml_fragments = False
    assert provider.get_value("secret", TSecretValue, "pipeline") == (
        "THIS IS SECRET VALUE",
        "pipeline/secret",
    )
    del provider._config_doc["pipeline"]["secret"]
    provider.clear_lookup_cache()

    # but request as not secret value -> still not found
    if provider.only_secrets:
        assert provider.get_value("secret", str, "pipeline") == (None, "pipeline/secret")
    provider.only_secrets = False
    # non secrets allowed
    assert provider.get_value("secret", str, "pipeline") == (
        "THIS IS SECRET VALUE",
        "pipeline/secret",
    )


def provision_fixture_secrets() -> None:
    """Creates or updates FIXTURE_SECRETS in the AWS account resolved from test credentials"""
    from botocore.exceptions import ClientError

    c = resolve_configuration(AwsCredentials(), sections=(known_sections.DESTINATION, "filesystem"))
    c.region_name = FIXTURES_REGION
    client = c._to_botocore_session().create_client("secretsmanager")
    for name, value in FIXTURE_SECRETS.items():
        secret_id = SECRET_NAME_PREFIX + name
        try:
            client.create_secret(Name=secret_id, SecretString=value)
            print(f"created {secret_id}")
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceExistsException":
                raise
            client.put_secret_value(SecretId=secret_id, SecretString=value)
            print(f"updated {secret_id}")


if __name__ == "__main__":
    provision_fixture_secrets()
