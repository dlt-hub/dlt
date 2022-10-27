

from typing import List

from dlt.common.configuration.providers import Provider
from dlt.common.configuration.providers.environ import EnvironProvider
from dlt.common.configuration.providers.container import ContextProvider
from dlt.common.configuration.providers.toml import SecretsTomlProvider, ConfigTomlProvider
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, ContainerInjectableContext, configspec


@configspec
class ConfigProvidersContext(ContainerInjectableContext):
    """Injectable list of providers used by the configuration `resolve` module"""
    providers: List[Provider]

    def __init__(self) -> None:
        super().__init__()
        # add default providers, ContextProvider must be always first - it will provide contexts
        self.providers = [ContextProvider(), EnvironProvider(), SecretsTomlProvider(), ConfigTomlProvider()]

    def __getitem__(self, name: str) -> Provider:
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

    def add_provider(self, provider: Provider) -> None:
        if provider.name in self:
            raise DuplicateProviderException(provider.name)
        self.providers.append(provider)


@configspec
class ConfigProvidersConfiguration(BaseConfiguration):
    with_aws_secrets: bool = False
    with_google_secrets: bool = False
