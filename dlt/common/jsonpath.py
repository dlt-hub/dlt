from typing import Iterable, Union, List, Any
from itertools import chain

from dlt.common.typing import DictStrAny

from jsonpath_ng import parse as _parse, JSONPath, Fields as JSONPathFields


TJsonPath = Union[str, JSONPath]  # Jsonpath compiled or str
TAnyJsonPath = Union[TJsonPath, Iterable[TJsonPath]]  # A single or multiple jsonpaths


def compile_path(s: TJsonPath) -> JSONPath:
    if isinstance(s, JSONPath):
        return s
    return _parse(s)


def compile_paths(s: TAnyJsonPath) -> List[JSONPath]:
    if isinstance(s, str) or not isinstance(s, Iterable):
        s = [s]
    return [compile_path(p) for p in s]


def delete_matches(paths: TAnyJsonPath, data: DictStrAny) -> None:
    """Remove all keys from `data` matching any of given json path(s).
    Filtering is done in place."""
    paths = compile_paths(paths)
    for p in paths:
        p.filter(lambda _: True, data)


def find_values(path: TJsonPath, data: DictStrAny) -> List[Any]:
    """Return a list of values found under the given json path"""
    path = compile_path(path)
    return [m.value for m in path.find(data)]


def resolve_paths(paths: TAnyJsonPath, data: DictStrAny) -> List[str]:
    """Return a list of paths resolved against `data`. The return value is a list of strings.

    Example:
    >>> resolve_paths('$.a.items[*].b', {'a': {'items': [{'b': 2}, {'b': 3}]}})
    >>> # ['a.items.[0].b', 'a.items.[1].b']
    """
    paths = compile_paths(paths)
    p: JSONPath
    return list(chain.from_iterable((str(r.full_path) for r in p.find(data)) for p in paths))
