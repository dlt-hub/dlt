from typing import Iterable, Union, List, Any, Optional, cast
from itertools import chain

from dlt.common.typing import DictStrAny

from jsonpath_ng import JSONPath, Fields as JSONPathFields
from jsonpath_ng.ext import parse as _parse

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
    return list(chain.from_iterable((str(r.full_path) for r in p.find(data)) for p in paths))


def is_simple_field_path(path: JSONPath) -> bool:
    """Checks if the given path represents a simple single field name.

    Example:
    >>> is_simple_field_path(compile_path('id'))
    True
    >>> is_simple_field_path(compile_path('$.id'))
    False
    """
    return isinstance(path, JSONPathFields) and len(path.fields) == 1 and path.fields[0] != "*"


def extract_simple_field_name(path: Union[str, JSONPath]) -> Optional[str]:
    """
    Extracts a simple field name from a JSONPath if it represents a single field access.
    Returns None if the path is complex (contains wildcards, array indices, or multiple fields).

    Args:
        path: A JSONPath object or string

    Returns:
        Optional[str]: The field name if path represents a simple field access, None otherwise

    Example:
        >>> extract_simple_field_name('name')
        'name'
        >>> extract_simple_field_name('"name"')
        'name'
        >>> extract_simple_field_name('"na$me"') # Escaped characters are preserved
        'na$me'
        >>> extract_simple_field_name('"na.me"') # Escaped characters are preserved
        'na.me'
        >>> extract_simple_field_name('$.name')  # Returns None
        >>> extract_simple_field_name('$.items[*].name')  # Returns None
        >>> extract_simple_field_name('*')  # Returns None
    """
    if isinstance(path, str):
        path = compile_path(path)

    if is_simple_field_path(path):
        return cast(str, path.fields[0])

    return None


def set_value_at_path(obj: dict[str, Any], path: TJsonPath, value: Any) -> None:
    """Sets a value in a nested dictionary at the specified path.

    Args:
        obj: The dictionary to modify
        path: The dot-separated path to the target location
        value: The value to set
    """
    path_parts = str(path).split(".")
    current = obj

    for part in path_parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]

    current[path_parts[-1]] = value
