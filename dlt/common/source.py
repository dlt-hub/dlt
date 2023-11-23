import threading
from types import ModuleType
from typing import Dict, NamedTuple, Optional, Type

from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import ResourceNameNotAvailable
from dlt.common.typing import AnyFun
from dlt.common.utils import get_callable_name


class SourceInfo(NamedTuple):
    """Runtime information on the source/resource"""

    SPEC: Type[BaseConfiguration]
    f: AnyFun
    module: ModuleType


_SOURCES: Dict[str, SourceInfo] = {}
"""A registry of all the decorated sources and resources discovered when importing modules"""

_CURRENT_PIPE_NAME: Dict[int, str] = {}
"""Name of currently executing pipe per thread id set during execution of a gen in pipe"""


def set_current_pipe_name(name: str) -> None:
    """Set pipe name in current thread"""
    _CURRENT_PIPE_NAME[threading.get_ident()] = name


def unset_current_pipe_name() -> None:
    """Unset pipe name in current thread"""
    _CURRENT_PIPE_NAME[threading.get_ident()] = None


def get_current_pipe_name() -> str:
    """Gets pipe name associated with current thread"""
    name = _CURRENT_PIPE_NAME.get(threading.get_ident())
    if name is None:
        raise ResourceNameNotAvailable()
    return name


def _get_source_for_inner_function(f: AnyFun) -> Optional[SourceInfo]:
    # find source function
    parts = get_callable_name(f, "__qualname__").split(".")
    parent_fun = ".".join(parts[:-2])
    return _SOURCES.get(parent_fun)
