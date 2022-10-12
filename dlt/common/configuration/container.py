from contextlib import contextmanager
from typing import Dict, Iterator, Type, TypeVar

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.exceptions import ContainerInjectableConfigurationMangled


@configspec
class ContainerInjectableConfiguration(BaseConfiguration):
    """Base class for all configurations that may be injected from Container."""
    pass


TConfiguration = TypeVar("TConfiguration", bound=ContainerInjectableConfiguration)


class Container:

    _INSTANCE: "Container" = None

    configurations: Dict[Type[ContainerInjectableConfiguration], ContainerInjectableConfiguration]

    def __new__(cls: Type["Container"]) -> "Container":
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
            cls._INSTANCE.configurations = {}
        return cls._INSTANCE

    def __init__(self) -> None:
        pass

    def __getitem__(self, spec: Type[TConfiguration]) -> TConfiguration:
        # return existing config object or create it from spec
        if not issubclass(spec, ContainerInjectableConfiguration):
            raise KeyError(f"{spec.__name__} is not injectable")

        return self.configurations.setdefault(spec, spec())  # type: ignore

    def __contains__(self, spec: Type[TConfiguration]) -> bool:
        return spec in self.configurations

    @contextmanager
    def injectable_configuration(self, config: TConfiguration) -> Iterator[TConfiguration]:
        spec = type(config)
        previous_config: ContainerInjectableConfiguration = None
        if spec in self.configurations:
            previous_config = self.configurations[spec]
        # set new config and yield context
        try:
            self.configurations[spec] = config
            yield config
        finally:
            # before setting the previous config for given spec, check if there was no overlapping modification
            if self.configurations[spec] is config:
                # config is injected for spec so restore previous
                if previous_config is None:
                    del self.configurations[spec]
                else:
                    self.configurations[spec] = previous_config
            else:
                # value was modified in the meantime and not restored
                raise ContainerInjectableConfigurationMangled(spec, self.configurations[spec], config)
