import dlt
from dlt import TSecretValue
from dlt.common import logger
# from dlt.common.configuration.specs import GcpServiceAccountCredentials
# from dlt.common.configuration.providers import GoogleSecretsProvider
# from dlt.common.configuration.accessors import secrets
from dlt.common.configuration.specs.config_providers_context import _google_secrets_provider
from dlt.common.configuration.specs.run_configuration import RunConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.typing import AnyType
from dlt.common.utils import custom_environ


# def test_regular_keys() -> None:
#     logger.init_logging(RunConfiguration())
    # print(secrets["credentials"])
    # c = secrets.get("credentials", GcpServiceAccountCredentials)
    # print(c)
    # provider = _google_secrets_provider()
    # assert provider.get_value("secret_value", AnyType) == ("2137", "secret_value")
    # print(provider.get_value("halo", TSecretValue, "halo"))
    # print(provider._toml.as_string())
    # print(provider.get_value("halo", TSecretValue, "halo"))
    # will get special sections
    # print(provider.get_value("halo", TSecretValue, "sources"))
    # print(provider.get_value("sources", TSecretValue))
    # print(provider.get_value("halo", TSecretValue, "sources", "pipedrive"))

    # with pipeline
    # print(provider.get_value("halo", TSecretValue, "dlt_pipeline", "sources"))
    # print(provider.get_value("sources", TSecretValue), "dlt_pipeline")
    # print(provider.get_value("halo", TSecretValue, "dlt_pipeline", "sources", "pipedrive"))


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

