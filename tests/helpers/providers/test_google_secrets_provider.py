import dlt
from dlt import TSecretValue
from dlt.common import logger
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.configuration.providers import GoogleSecretsProvider
from dlt.common.configuration.accessors import secrets
from dlt.common.configuration.specs.config_providers_context import _google_secrets_provider
from dlt.common.configuration.specs.run_configuration import RunConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials, known_sections
from dlt.common.typing import AnyType
from dlt.common.utils import custom_environ
from dlt.common.configuration.resolve import resolve_configuration


DLT_SECRETS_TOML_CONTENT = """
secret_value=2137
api.secret_key="ABCD"


[credentials]
secret_value="2138"
project_id="mock-credentials"
"""


def test_regular_keys() -> None:
    logger.init_logging(RunConfiguration())
    # copy bigquery credentials into providers credentials
    c = resolve_configuration(
        GcpServiceAccountCredentials(), sections=(known_sections.DESTINATION, "bigquery")
    )
    secrets[f"{known_sections.PROVIDERS}.google_secrets.credentials"] = dict(c)
    # c = secrets.get("destination.credentials", GcpServiceAccountCredentials)
    # print(c)
    provider: GoogleSecretsProvider = _google_secrets_provider()  # type: ignore[assignment]
    assert provider._toml.as_string().strip() == DLT_SECRETS_TOML_CONTENT.strip()
    assert provider.get_value("secret_value", AnyType, "pipeline x !!") == (
        None,
        "pipeline_x-secret_value",
    )
    assert provider.get_value("secret_value", AnyType, None) == (2137, "secret_value")
    assert provider.get_value("secret_key", AnyType, None, "api") == ("ABCD", "api-secret_key")

    # load secrets toml per pipeline
    provider.get_value("secret_key", AnyType, "pipeline", "api")
    assert provider.get_value("secret_key", AnyType, "pipeline", "api") == (
        "ABCDE",
        "pipeline-api-secret_key",
    )
    assert provider.get_value("credentials", AnyType, "pipeline") == (
        {"project_id": "mock-credentials-pipeline"},
        "pipeline-credentials",
    )

    # load source test_source which should also load "sources", "pipeline-sources", "sources-test_source" and "pipeline-sources-test_source"
    assert provider.get_value("only_pipeline", AnyType, "pipeline", "sources", "test_source") == (
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

    # try a single secret value
    assert provider.get_value("secret", TSecretValue, "pipeline") == (None, "pipeline-secret")

    # enable the single secrets
    provider.only_toml_fragments = False
    # but request as not secret value -> still not found
    assert provider.get_value("secret", str, "pipeline") == (None, "pipeline-secret")
    provider.only_secrets = False
    # non secrets allowed
    assert provider.get_value("secret", str, "pipeline") == (
        "THIS IS SECRET VALUE",
        "pipeline-secret",
    )

    # request json
    # print(provider._toml.as_string())
    assert provider.get_value("halo", str, None, "halo") == ({"halo": True}, "halo-halo")
    assert provider.get_value("halo", str, None, "halo", "halo") == (True, "halo-halo-halo")


# def test_special_sections() -> None:
#     pass
# with custom_environ({"GOOGLE_APPLICATION_CREDENTIALS": "_secrets/pipelines-ci-secrets-65c0517a9b30.json"}):
#     provider = _google_secrets_provider()
#     print(provider.get_value("credentials", GcpServiceAccountCredentials, None, "destination", "bigquery"))
#     print(provider._toml.as_string())
#     print(provider.get_value("subdomain", AnyType, None, "sources", "zendesk", "credentials"))
#     print(provider._toml.as_string())


# def test_provider_insertion() -> None:
#     with custom_environ({
#         "GOOGLE_APPLICATION_CREDENTIALS": "_secrets/project1234_service.json"
#         "PROVIDERS__ENABLE_GOOGLE_SECRETS_MANAGER": "true"
#         }):

#         #
