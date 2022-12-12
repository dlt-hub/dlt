import os
from pathlib import Path
import sys
import base64
import hashlib
import secrets
from contextlib import contextmanager
from functools import wraps
from os import environ

from typing import Any, Dict, Iterable, Iterator, Optional, Sequence, TypeVar, Mapping, List, TypedDict, Union

from dlt.common.typing import AnyFun, StrAny, DictStrAny, StrStr, TAny, TDataItem, TDataItems, TFun

T = TypeVar("T")


def chunks(seq: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def uniq_id(len_: int = 16) -> str:
    return secrets.token_hex(len_)


def digest128(v: str) -> str:
    return base64.b64encode(
        hashlib.shake_128(
            v.encode("utf-8")
        ).digest(15)
    ).decode('ascii')


def digest256(v: str) -> str:
    digest = hashlib.sha3_256(v.encode("utf-8")).digest()
    return base64.b64encode(digest).decode('ascii')


def str2bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError('Boolean value expected.')


def flatten_list_of_dicts(dicts: Sequence[StrAny]) -> StrAny:
    """
    Transforms a list of objects [{K: {...}}, {L: {....}}, ...] -> {K: {...}, L: {...}...}
    """
    o: DictStrAny = {}
    for d in dicts:
        for k,v in d.items():
            if k in o:
                raise KeyError(f"Cannot flatten with duplicate key {k}")
            o[k] = v
    return o


def flatten_list_of_str_or_dicts(seq: Sequence[Union[StrAny, str]]) -> StrAny:
    """
    Transforms a list of objects or strings [{K: {...}}, L, ...] -> {K: {...}, L: None, ...}
    """
    o: DictStrAny = {}
    for e in seq:
        if isinstance(e, dict):
            for k,v in e.items():
                if k in o:
                    raise KeyError(f"Cannot flatten with duplicate key {k}")
                o[k] = v
        else:
            key = str(e)
            if key in o:
                raise KeyError(f"Cannot flatten with duplicate key {k}")
            o[key] = None
    return o


def flatten_dicts_of_dicts(dicts: Mapping[str, Any]) -> Sequence[Any]:
    """
    Transform and object {K: {...}, L: {...}...} -> [{key:K, ....}, {key: L, ...}, ...]
    """
    o: List[Any] = []
    for k, v in dicts.items():
        if isinstance(v, list):
            # if v is a list then add "key" to each list element
            for lv in v:
                lv["key"] = k
        else:
            # add as "key" to dict
            v["key"] = k

        o.append(v)
    return o


def tuplify_list_of_dicts(dicts: Sequence[DictStrAny]) -> Sequence[DictStrAny]:
    """
    Transform list of dictionaries with single key into single dictionary of {"key": orig_key, "value": orig_value}
    """
    for d in dicts:
        if len(d) > 1:
            raise ValueError(f"Tuplify requires one key dicts {d}")
        if len(d) == 1:
            key = next(iter(d))
            # delete key first to avoid name clashes
            value = d[key]
            del d[key]
            d["key"] = key
            d["value"] = value

    return dicts


def flatten_list_or_items(_iter: Iterator[TDataItems]) -> Iterator[TDataItem]:
    for items in _iter:
        if isinstance(items, list):
            yield from items
        else:
            yield items


def filter_env_vars(envs: List[str]) -> StrStr:
    return {k.lower(): environ[k] for k in envs if k in environ}


def update_dict_with_prune(dest: DictStrAny, update: StrAny) -> None:
    """Updates values that are both in `dest` and `update` and deletes `dest` values that are None in `update`"""
    for k, v in update.items():
        if v is not None:
            dest[k] = v
        elif k in dest:
            del dest[k]


def map_nested_in_place(func: AnyFun, _complex: TAny) -> TAny:
    """Applies `func` to all elements in `_dict` recursively, replacing elements in nested dictionaries and lists in place."""
    if isinstance(_complex, dict):
        for k, v in _complex.items():
            if isinstance(v, (dict, list)):
                map_nested_in_place(func, v)
            else:
                _complex[k] = func(v)
    elif isinstance(_complex, list):
        for idx, _l in enumerate(_complex):
            if isinstance(_l, (dict, list)):
                map_nested_in_place(func, _l)
            else:
                _complex[idx] = func(_l)
    else:
        raise ValueError(_complex, "Not a complex type")
    return _complex


def is_interactive() -> bool:
    import __main__ as main
    return not hasattr(main, '__file__')


def dict_remove_nones_in_place(d: Dict[Any, Any]) -> Dict[Any, Any]:
    for k in list(d.keys()):
        if d[k] is None:
            del d[k]
    return d


@contextmanager
def custom_environ(env: StrStr) -> Iterator[None]:
    """Temporarily set environment variables inside the context manager and
    fully restore previous environment afterwards
    """
    original_env = {key: os.getenv(key) for key in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value


def with_custom_environ(f: TFun) -> TFun:

    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        saved_environ = os.environ.copy()
        try:
            return f(*args, **kwargs)
        finally:
            os.environ.clear()
            os.environ.update(saved_environ)

    return _wrap  # type: ignore


def encoding_for_mode(mode: str) -> Optional[str]:
    if "b" in mode:
        return None
    else:
        return "utf-8"


def main_module_file_path() -> str:
    if len(sys.argv) > 0 and os.path.isfile(sys.argv[0]):
        return str(Path(sys.argv[0]))
    return None


@contextmanager
def set_working_dir(path: str) -> Iterator[str]:
    curr_dir = os.path.abspath(os.getcwd())
    try:
        if path:
            os.chdir(path)
        yield path
    finally:
        os.chdir(curr_dir)


def get_callable_name(f: AnyFun, name_attr: str = "__name__") -> Optional[str]:
    # check first if __name__ is present directly (function), if not then look for type name
    name: str = getattr(f, name_attr, None)
    if not name:
        name = getattr(f.__class__, name_attr, None)
    return name


def is_inner_callable(f: AnyFun) -> bool:
    """Checks if f is defined within other function"""
    # inner functions have full nesting path in their qualname
    return "<locals>" in get_callable_name(f, name_attr="__qualname__")


def obfuscate_pseudo_secret(pseudo_secret: str, pseudo_key: bytes) -> str:
    return base64.b64encode(bytes([_a ^ _b for _a, _b in zip(pseudo_secret.encode("utf-8"), pseudo_key*250)])).decode()


def reveal_pseudo_secret(obfuscated_secret: str, pseudo_key: bytes) -> str:
    return bytes([_a ^ _b for _a, _b in zip(base64.b64decode(obfuscated_secret.encode("ascii"), validate=True), pseudo_key*250)]).decode("utf-8")
