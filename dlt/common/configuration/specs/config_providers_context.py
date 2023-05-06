

from typing import List
from dlt.common.configuration.exceptions import DuplicateConfigProviderException
from dlt.common.configuration.providers import ConfigProvider, EnvironProvider, ContextProvider, SecretsTomlProvider, ConfigTomlProvider
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec
from dlt.common.runtime.exec_info import is_running_in_airflow_task


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
        providers += _extra_providers()
        return providers


def _extra_providers() -> List[ConfigProvider]:
    return _airflow_providers()


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


# TODO: implement ConfigProvidersConfiguration and
# @configspec
# class ConfigProvidersConfiguration(BaseConfiguration):
#     with_aws_secrets: bool = False
#     with_google_secrets: bool = False
