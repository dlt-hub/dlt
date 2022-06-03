import hashlib
from os import environ
from uuid import uuid4
from typing import Any, Iterator, Sequence, TypeVar, Mapping, List, Union

from dlt.common.typing import StrAny, DictStrAny, StrStr

T = TypeVar("T")


def chunks(list: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    for i in range(0, len(list), n):
        yield list[i:i + n]


def uniq_id() -> str:
    return uuid4().hex


def digest128(v: str) -> str:
    return hashlib.shake_128(v.encode("utf-8")).hexdigest(16)


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
        if type(e) is dict:
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
        if type(v) is list:
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
    Transform dicts with single key into {"key": orig_key, "value": orig_value}
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


def filter_env_vars(vars: List[str]) -> StrStr:
    return {k.lower(): environ[k] for k in vars if k in environ}


def update_dict_with_prune(dest: DictStrAny, update: StrAny) -> None:
    for k, v in update.items():
        if v is not None:
            dest[k] = v
        elif k in dest:
            del dest[k]


def is_interactive() -> bool:
    import __main__ as main
    return not hasattr(main, '__file__')
