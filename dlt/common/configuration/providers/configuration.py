

from typing import List

from dlt.common.configuration.providers import Provider
from dlt.common.configuration.providers.environ import EnvironProvider
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec


@configspec
class ConfigProvidersConfiguration(BaseConfiguration):
    providers: List[Provider]

    def __init__(self) -> None:
        super().__init__()
        # add default providers
        self.providers = [EnvironProvider()]
