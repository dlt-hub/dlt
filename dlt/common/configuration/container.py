from contextlib import contextmanager
from typing import Dict, Iterator, Type, TypeVar

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext
from dlt.common.configuration.exceptions import ContainerInjectableContextMangled, ContextDefaultCannotBeCreated

TConfiguration = TypeVar("TConfiguration", bound=ContainerInjectableContext)


class Container:

    _INSTANCE: "Container" = None

    contexts: Dict[Type[ContainerInjectableContext], ContainerInjectableContext]

    def __new__(cls: Type["Container"]) -> "Container":
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
            cls._INSTANCE.contexts = {}
        return cls._INSTANCE

    def __init__(self) -> None:
        pass

    def __getitem__(self, spec: Type[TConfiguration]) -> TConfiguration:
        # return existing config object or create it from spec
        if not issubclass(spec, ContainerInjectableContext):
            raise KeyError(f"{spec.__name__} is not a context")

        item = self.contexts.get(spec)
        if item is None:
            if spec.can_create_default:
                item = spec()
                self.contexts[spec] = item
            else:
                raise ContextDefaultCannotBeCreated(spec)

        return item  # type: ignore

    def __setitem__(self, spec: Type[TConfiguration], value: TConfiguration) -> None:
        # value passed to container must be final
        value.__is_resolved__ = True
        # put it into context
        self.contexts[spec] = value

    def __contains__(self, spec: Type[TConfiguration]) -> bool:
        return spec in self.contexts


    @contextmanager
    def injectable_context(self, config: TConfiguration) -> Iterator[TConfiguration]:
        spec = type(config)
        previous_config: ContainerInjectableContext = None
        if spec in self.contexts:
            previous_config = self.contexts[spec]
        # set new config and yield context
        try:
            self[spec] = config
            yield config
        finally:
            # before setting the previous config for given spec, check if there was no overlapping modification
            if self.contexts[spec] is config:
                # config is injected for spec so restore previous
                if previous_config is None:
                    del self.contexts[spec]
                else:
                    self.contexts[spec] = previous_config
            else:
                # value was modified in the meantime and not restored
                raise ContainerInjectableContextMangled(spec, self.contexts[spec], config)
