

from typing import List

from dlt.common.configuration.providers import Provider
from dlt.common.configuration.container import ContainerInjectableConfiguration
from dlt.common.configuration.providers.environ import EnvironProvider
from dlt.common.configuration.providers.container import ContainerProvider
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec
class ConfigProvidersListConfiguration(ContainerInjectableConfiguration):
    providers: List[Provider]

    def __init__(self) -> None:
        super().__init__()
        # add default providers, ContainerProvider must be always first - it will provide injectable configs
        self.providers = [ContainerProvider(), EnvironProvider()]


@configspec
class ConfigProvidersConfiguration(BaseConfiguration):
    with_aws_secrets: bool = False
    with_google_secrets: bool = False
