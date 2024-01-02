import contextlib
from typing import Any, ClassVar, Optional, Type, Tuple

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import ContainerInjectableContext

from .provider import ConfigProvider


class ContextProvider(ConfigProvider):
    NAME: ClassVar[str] = "Injectable Context"

    def __init__(self) -> None:
        self.container = Container()

    @property
    def name(self) -> str:
        return ContextProvider.NAME

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str = None, *sections: str
    ) -> Tuple[Optional[Any], str]:
        assert sections == ()

        # only context is a valid hint
        with contextlib.suppress(KeyError, TypeError):
            if issubclass(hint, ContainerInjectableContext):
                # contexts without defaults will raise ContextDefaultCannotBeCreated
                return self.container[hint], hint.__name__

        return None, str(hint)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_sections(self) -> bool:
        return False
