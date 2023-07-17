import codecs
import platform
import typing as t

import simplejson

from dlt.common.json import custom_encode, custom_pua_decode_nested, custom_pua_encode

if platform.python_implementation() == "PyPy":
    # disable speedups on PyPy, it can be actually faster than Python C
    simplejson._toggle_speedups(False)  # type: ignore


_impl_name = "simplejson"


def dump(obj: t.Any, fp: t.IO[bytes], sort_keys: bool = False, pretty: bool = False) -> None:
    if pretty:
        indent = 2
    else:
        indent = None
    # prevent default decimal serializer (use_decimal=False) and binary serializer (encoding=None)
    return simplejson.dump(
        obj,
        codecs.getwriter("utf-8")(fp),  # type: ignore
        use_decimal=False,
        default=custom_encode,
        encoding=None,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        indent=indent,
    )


def typed_dump(obj: t.Any, fp: t.IO[bytes], pretty: bool = False) -> None:
    if pretty:
        indent = 2
    else:
        indent = None
    # prevent default decimal serializer (use_decimal=False) and binary serializer (encoding=None)
    return simplejson.dump(
        obj,
        codecs.getwriter("utf-8")(fp),  # type: ignore
        use_decimal=False,
        default=custom_pua_encode,
        encoding=None,
        ensure_ascii=False,
        separators=(",", ":"),
        indent=indent,
    )


def typed_dumps(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> str:
    indent = 2 if pretty else None
    return simplejson.dumps(
        obj,
        use_decimal=False,
        default=custom_pua_encode,
        encoding=None,
        ensure_ascii=False,
        separators=(",", ":"),
        indent=indent,
    )


def typed_loads(s: str) -> t.Any:
    return custom_pua_decode_nested(loads(s))


def typed_dumpb(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return typed_dumps(obj, sort_keys, pretty).encode("utf-8")


def typed_loadb(s: t.Union[bytes, bytearray, memoryview]) -> t.Any:
    return custom_pua_decode_nested(loadb(s))


def dumps(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> str:
    if pretty:
        indent = 2
    else:
        indent = None
    return simplejson.dumps(
        obj,
        use_decimal=False,
        default=custom_encode,
        encoding=None,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        indent=indent,
    )


def dumpb(obj: t.Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return dumps(obj, sort_keys, pretty).encode("utf-8")


def load(fp: t.IO[bytes]) -> t.Any:
    return simplejson.load(fp, use_decimal=False)  # type: ignore


def loads(s: str) -> t.Any:
    return simplejson.loads(s, use_decimal=False)


def loadb(s: t.Union[bytes, bytearray, memoryview]) -> t.Any:
    return loads(bytes(s).decode("utf-8"))
