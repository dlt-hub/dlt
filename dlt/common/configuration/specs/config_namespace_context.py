from typing import List, Optional, Tuple, TYPE_CHECKING

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec


@configspec(init=True)
class ConfigNamespacesContext(ContainerInjectableContext):
    pipeline_name: Optional[str]
    namespaces: Tuple[str, ...] = ()

    if TYPE_CHECKING:
        # provide __init__ signature when type checking
        def __init__(self, pipeline_name:str = None, namespaces: Tuple[str, ...] = ()) -> None:
            ...
