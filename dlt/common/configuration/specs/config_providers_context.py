

from typing import List

from dlt.common.configuration.exceptions import DuplicateConfigProviderException
from dlt.common.configuration.providers import ConfigProvider, EnvironProvider, ContextProvider, SecretsTomlProvider, ConfigTomlProvider
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec


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

        # Attempt to import Airflow and add AirflowTomlProvider if successful.
        # Successful import of Airflow means we are running in an Airflow environment
        # and the AirflowTomlProvider will be able to read configuration
        # from Airflow's Connections
        try:
            import airflow  # noqa
            from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider
            providers.append(AirflowSecretsTomlProvider())
        except ImportError:
            pass

        return providers


# TODO: implement ConfigProvidersConfiguration and
# @configspec
# class ConfigProvidersConfiguration(BaseConfiguration):
#     with_aws_secrets: bool = False
#     with_google_secrets: bool = False
