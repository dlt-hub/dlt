import codecs
from typing import IO, Any, Union

import simplejson
import platform

from dlt.common.json import (
    custom_pua_encode,
    custom_pua_decode_nested,
    custom_encode,
    set_custom_encoder_impl,
    TPuaDecoders,
    DECODERS,
)

if platform.python_implementation() == "PyPy":
    # disable speedups on PyPy, it can be actually faster than Python C
    simplejson._toggle_speedups(False)  # type: ignore

from dlt.common.arithmetics import Decimal

_impl_name = "simplejson"

set_custom_encoder = set_custom_encoder_impl


def dump(obj: Any, fp: IO[bytes], sort_keys: bool = False, pretty: bool = False) -> None:
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


def typed_dump(obj: Any, fp: IO[bytes], pretty: bool = False) -> None:
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


def typed_dumps(obj: Any, sort_keys: bool = False, pretty: bool = False) -> str:
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


def typed_loads(s: str) -> Any:
    return custom_pua_decode_nested(loads(s))


def typed_dumpb(obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return typed_dumps(obj, sort_keys, pretty).encode("utf-8")


def typed_loadb(s: Union[bytes, bytearray, memoryview], decoders: TPuaDecoders = DECODERS) -> Any:
    return custom_pua_decode_nested(loadb(s), decoders)


def dumps(obj: Any, sort_keys: bool = False, pretty: bool = False) -> str:
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


def dumpb(obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return dumps(obj, sort_keys, pretty).encode("utf-8")


def load(fp: IO[bytes]) -> Any:
    return simplejson.load(fp, use_decimal=False)  # type: ignore


def loads(s: str) -> Any:
    return simplejson.loads(s, use_decimal=False)


def loadb(s: Union[bytes, bytearray, memoryview]) -> Any:
    return loads(bytes(s).decode("utf-8"))
