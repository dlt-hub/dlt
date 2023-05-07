

from typing import List
from dlt.common.configuration.exceptions import DuplicateConfigProviderException
from dlt.common.configuration.providers import ConfigProvider, EnvironProvider, ContextProvider, SecretsTomlProvider, ConfigTomlProvider, GoogleSecretsProvider
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.configuration.specs import GcpServiceAccountCredentials, BaseConfiguration, configspec, known_sections
from dlt.common.runtime.exec_info import is_running_in_airflow_task


@configspec
class ConfigProvidersConfiguration(BaseConfiguration):
    enable_airflow_secrets: bool = True
    enable_google_secrets: bool = False
    only_toml_fragments: bool = True

    # always look in providers
    __section__ = known_sections.PROVIDERS


@configspec
class ConfigProvidersContext(ContainerInjectableContext):
    """Injectable list of providers used by the configuration `resolve` module"""
    providers: List[ConfigProvider]
    context_provider: ConfigProvider

    def __init__(self) -> None:
        super().__init__()
        # add default providers
        self.providers = ConfigProvidersContext.initial_providers()
        # ContextProvider will provide contexts when embedded in configurations
        self.context_provider = ContextProvider()

    def add_extras(self) -> None:
        """Adds extra providers using the initial ones."""
        self.providers += _extra_providers()

    def __getitem__(self, name: str) -> ConfigProvider:
        try:
            return next(p for p in self.providers if p.name == name)
        except StopIteration:
            raise KeyError(name)

    def __contains__(self, name: object) -> bool:
        try:
            self.__getitem__(name)  # type: ignore
            return True
        except KeyError:
            return False

    def add_provider(self, provider: ConfigProvider) -> None:
        if provider.name in self:
            raise DuplicateConfigProviderException(provider.name)
        self.providers.append(provider)

    @staticmethod
    def initial_providers() -> List[ConfigProvider]:
        providers = [
            EnvironProvider(),
            SecretsTomlProvider(add_global_config=True),
            ConfigTomlProvider(add_global_config=True)
        ]
        return providers


def _extra_providers() -> List[ConfigProvider]:
    from dlt.common.configuration.resolve import resolve_configuration
    providers_config = resolve_configuration(ConfigProvidersConfiguration())
    extra_providers = []
    if providers_config.enable_airflow_secrets:
        extra_providers.extend(_airflow_providers())
    if providers_config.enable_google_secrets:
        extra_providers.append(_google_secrets_provider(only_toml_fragments=providers_config.only_toml_fragments))
    return extra_providers


def _google_secrets_provider(only_secrets: bool = True, only_toml_fragments: bool = True) -> ConfigProvider:
    from dlt.common.configuration.resolve import resolve_configuration

    c = resolve_configuration(GcpServiceAccountCredentials(), sections=(known_sections.PROVIDERS, "google_secrets"))
    return GoogleSecretsProvider(c, only_secrets=only_secrets, only_toml_fragments=only_toml_fragments)


def _airflow_providers() -> List[ConfigProvider]:
    """Returns a list of configuration providers for an Airflow environment.

    This function attempts to import Airflow to determine whether it
    is running in an Airflow environment. If Airflow is not installed,
    an empty list is returned. If Airflow is installed, the function
    returns a list containing the Airflow providers.
    """
    if not is_running_in_airflow_task():
        return []

    from airflow.models import Variable # noqa
    from airflow.operators.python import get_current_context  # noqa

    from dlt.common.configuration.providers.airflow import (
        AirflowSecretsTomlProvider,
        AIRFLOW_SECRETS_TOML_VARIABLE_KEY
    )

    secrets_toml_var = Variable.get(
        AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
        default_var=None
    )

    if secrets_toml_var is not None:
        return [AirflowSecretsTomlProvider()]
    else:
        ti = get_current_context()["ti"]
        ti.log.warning(
            f"Airflow variable '{AIRFLOW_SECRETS_TOML_VARIABLE_KEY}' "
            "not found. AirflowSecretsTomlProvider will not be used."
        )
        return []
