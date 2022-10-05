from typing import Dict, Type, TypeVar

from dlt.common.configuration.specs.base_configuration import BaseConfiguration

TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


class Container:

    _INSTANCE: "Container" = None

    def __new__(cls: Type["Container"]) -> "Container":
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
        return cls._INSTANCE


    def __init__(self) -> None:
        self.configurations: Dict[Type[BaseConfiguration], BaseConfiguration] = {}


    def __getitem__(self, spec: Type[TConfiguration]) -> TConfiguration:
        # return existing config object or create it from spec
        return self.configurations.setdefault(spec, spec())

