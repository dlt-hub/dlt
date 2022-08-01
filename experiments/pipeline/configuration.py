from typing import Any, Type

from dlt.common.typing import DictStrAny, TAny
from dlt.common.configuration.utils import make_configuration


def get_config(spec: Type[TAny], key: str = None, namespace: str = None, initial_values: Any = None, accept_partial: bool = False) -> Type[TAny]:
    # TODO: implement key and namespace
    return make_configuration(spec, spec, initial_values=initial_values, accept_partial=accept_partial)

