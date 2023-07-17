import typing as t

import orjson

from dlt.common.json import custom_encode, custom_pua_decode_nested, custom_pua_encode
from dlt.common.typing import AnyFun

_impl_name = "orjson"


def _dumps(
    obj: t.Any, sort_keys: bool, pretty: bool, default: AnyFun = custom_encode, options: int = 0
) -> bytes:
    options = options | orjson.OPT_UTC_Z | orjson.OPT_NON_STR_KEYS
    if pretty:
        options |= orjson.OPT_INDENT_2
    if sort_keys:
        options |= orjson.OPT_SORT_KEYS
    return orjson.dumps(obj, default=default, option=options)


def dump(obj: t.Any, fp: t.IO[bytes], sort_keys: bool = False, pretty: bool = False) -> None:
    fp.write(_dumps(obj, sort_keys, pretty))


def typed_dump(obj: t.Any, fp: t.IO[bytes], pretty: bool = False) -> None:
    fp.write(typed_dumpb(obj, pretty=pretty))


def typed_dumpb(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return _dumps(obj, sort_keys, pretty, custom_pua_encode, orjson.OPT_PASSTHROUGH_DATETIME)


def typed_dumps(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> str:
    return typed_dumpb(obj, sort_keys, pretty).decode("utf-8")


def typed_loads(s: str) -> t.Any:
    return custom_pua_decode_nested(loads(s))


def typed_loadb(s: t.Union[bytes, bytearray, memoryview]) -> t.Any:
    return custom_pua_decode_nested(loadb(s))


def dumps(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> str:
    return _dumps(obj, sort_keys, pretty).decode("utf-8")


def dumpb(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return _dumps(obj, sort_keys, pretty)


def load(fp: t.IO[bytes]) -> t.Any:
    return orjson.loads(fp.read())


def loads(s: str) -> t.Any:
    return orjson.loads(s.encode("utf-8"))


def loadb(s: t.Union[bytes, bytearray, memoryview]) -> t.Any:
    return orjson.loads(s)
