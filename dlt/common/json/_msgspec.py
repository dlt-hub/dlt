from typing import IO, Any, Union
import msgspec.json

from dlt.common.json import custom_pua_encode, custom_pua_decode_nested, custom_encode
from dlt.common.typing import AnyFun

_impl_name = "msgspec"


def _dumps(
    obj: Any, sort_keys: bool, pretty: bool, default: AnyFun = custom_encode, options: int = 0
) -> bytes:
    return msgspec.json.encode(obj, enc_hook=default, order="sorted" if sort_keys else None)


def dump(obj: Any, fp: IO[bytes], sort_keys: bool = False, pretty: bool = False) -> None:
    fp.write(_dumps(obj, sort_keys, pretty))


def typed_dump(obj: Any, fp: IO[bytes], pretty: bool = False) -> None:
    fp.write(typed_dumpb(obj, pretty=pretty))


def typed_dumpb(obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return _dumps(obj, sort_keys, pretty, custom_pua_encode)


def typed_dumps(obj: Any, sort_keys: bool = False, pretty: bool = False) -> str:
    return typed_dumpb(obj, sort_keys, pretty).decode("utf-8")


def typed_loads(s: str) -> Any:
    return custom_pua_decode_nested(loads(s))


def typed_loadb(s: Union[bytes, bytearray, memoryview]) -> Any:
    return custom_pua_decode_nested(loadb(s))


def dumps(obj: Any, sort_keys: bool = False, pretty: bool = False) -> str:
    return _dumps(obj, sort_keys, pretty).decode("utf-8")


def dumpb(obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
    return _dumps(obj, sort_keys, pretty)


def load(fp: IO[bytes]) -> Any:
    return msgspec.json.decode(fp.read())


def loads(s: str) -> Any:
    return msgspec.json.decode(s.encode("utf-8"))


def loadb(s: Union[bytes, bytearray, memoryview]) -> Any:
    return msgspec.json.decode(s)
