from typing import Any, Optional, Type, Tuple

from dlt.common.configuration.container import Container

from .provider import Provider


class ContainerProvider(Provider):

    NAME = "Injectable Configuration"

    @property
    def name(self) -> str:
        return ContainerProvider.NAME

    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        assert namespaces == ()
        # get container singleton
        container = Container()
        if hint in container:
            return Container()[hint], hint.__name__
        else:
            return None, str(hint)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_namespaces(self) -> bool:
        return False
