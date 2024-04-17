import os
import base64
import dataclasses
from datetime import date, datetime, time  # noqa: I251
from typing import Any, Callable, List, Protocol, IO, Union
from uuid import UUID
from hexbytes import HexBytes
from enum import Enum

try:
    from pydantic import BaseModel as PydanticBaseModel
except ImportError:
    PydanticBaseModel = None  # type: ignore[misc]

from dlt.common.pendulum import pendulum
from dlt.common.arithmetics import Decimal
from dlt.common.wei import Wei
from dlt.common.utils import map_nested_in_place


class SupportsJson(Protocol):
    """Minimum adapter for different json parser implementations"""

    _impl_name: str
    """Implementation name"""

    def dump(
        self, obj: Any, fp: IO[bytes], sort_keys: bool = False, pretty: bool = False
    ) -> None: ...

    def typed_dump(self, obj: Any, fp: IO[bytes], pretty: bool = False) -> None: ...

    def typed_dumps(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> str: ...

    def typed_loads(self, s: str) -> Any: ...

    def typed_dumpb(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes: ...

    def typed_loadb(self, s: Union[bytes, bytearray, memoryview]) -> Any: ...

    def dumps(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> str: ...

    def dumpb(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes: ...

    def load(self, fp: Union[IO[bytes], IO[str]]) -> Any: ...

    def loads(self, s: str) -> Any: ...

    def loadb(self, s: Union[bytes, bytearray, memoryview]) -> Any: ...


def custom_encode(obj: Any) -> str:
    if isinstance(obj, Decimal):
        # always return decimals as string so they are not deserialized back to float
        return str(obj)
    # this works both for standard datetime and pendulum
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, time):
        return obj.isoformat()
    elif isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, HexBytes):
        return obj.hex()
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    elif hasattr(obj, "asdict"):
        return obj.asdict()  # type: ignore
    elif hasattr(obj, "_asdict"):
        return obj._asdict()  # type: ignore
    elif PydanticBaseModel and isinstance(obj, PydanticBaseModel):
        return obj.dict()  # type: ignore[return-value]
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore
    elif isinstance(obj, Enum):
        return obj.value  # type: ignore[no-any-return]
    raise TypeError(repr(obj) + " is not JSON serializable")


# use PUA range to encode additional types
PUA_START = int(os.environ.get("DLT_JSON_TYPED_PUA_START", "0xf026"), 16)

_DECIMAL = chr(PUA_START)
_DATETIME = chr(PUA_START + 1)
_DATE = chr(PUA_START + 2)
_UUIDT = chr(PUA_START + 3)
_HEXBYTES = chr(PUA_START + 4)
_B64BYTES = chr(PUA_START + 5)
_WEI = chr(PUA_START + 6)
_TIME = chr(PUA_START + 7)

PUA_START_UTF8_MAGIC = _DECIMAL.encode("utf-8")[:2]


def _datetime_decoder(obj: str) -> datetime:
    if obj.endswith("Z"):
        # Backwards compatibility for data encoded with previous dlt version
        # fromisoformat does not support Z suffix (until py3.11)
        obj = obj[:-1] + "+00:00"
    return pendulum.DateTime.fromisoformat(obj)


# define decoder for each prefix
DECODERS: List[Callable[[Any], Any]] = [
    Decimal,
    _datetime_decoder,
    pendulum.Date.fromisoformat,
    UUID,
    HexBytes,
    base64.b64decode,
    Wei,
    pendulum.Time.fromisoformat,
]
# how many decoders?
PUA_CHARACTER_MAX = len(DECODERS)


def custom_pua_encode(obj: Any) -> str:
    # wei is subclass of decimal and must be checked first
    if isinstance(obj, Wei):
        return _WEI + str(obj)
    elif isinstance(obj, Decimal):
        return _DECIMAL + str(obj)
    # this works both for standard datetime and pendulum
    elif isinstance(obj, datetime):
        return _DATETIME + obj.isoformat()
    elif isinstance(obj, date):
        return _DATE + obj.isoformat()
    elif isinstance(obj, time):
        return _TIME + obj.isoformat()
    elif isinstance(obj, UUID):
        return _UUIDT + str(obj)
    elif isinstance(obj, HexBytes):
        return _HEXBYTES + obj.hex()
    elif isinstance(obj, bytes):
        return _B64BYTES + base64.b64encode(obj).decode("ascii")
    elif hasattr(obj, "asdict"):
        return obj.asdict()  # type: ignore
    elif hasattr(obj, "_asdict"):
        return obj._asdict()  # type: ignore
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore
    elif PydanticBaseModel and isinstance(obj, PydanticBaseModel):
        return obj.dict()  # type: ignore[return-value]
    elif isinstance(obj, Enum):
        # Enum value is just int or str
        return obj.value  # type: ignore[no-any-return]
    raise TypeError(repr(obj) + " is not JSON serializable")


def custom_pua_decode(obj: Any) -> Any:
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - PUA_START
        # decode only the PUA space defined in DECODERS
        if c >= 0 and c <= PUA_CHARACTER_MAX:
            try:
                return DECODERS[c](obj[1:])
            except Exception:
                # return strings that cannot be parsed
                # this may be due
                # (1) someone exposing strings with PUA characters to external systems (ie. via API)
                # (2) using custom types ie. DateTime that does not create correct iso strings
                return obj
    return obj


def custom_pua_decode_nested(obj: Any) -> Any:
    if isinstance(obj, str):
        return custom_pua_decode(obj)
    elif isinstance(obj, (list, dict)):
        return map_nested_in_place(custom_pua_decode, obj)
    return obj


def custom_pua_remove(obj: Any) -> Any:
    """Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is."""
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - PUA_START
        # decode only the PUA space defined in DECODERS
        if c >= 0 and c <= PUA_CHARACTER_MAX:
            return obj[1:]
    return obj


def may_have_pua(line: bytes) -> bool:
    """Checks if bytes string contains pua marker"""
    return PUA_START_UTF8_MAGIC in line


# pick the right impl
json: SupportsJson = None
if os.environ.get("DLT_USE_JSON") == "simplejson":
    from dlt.common.json import _simplejson as _json_d

    json = _json_d  # type: ignore[assignment]
else:
    try:
        from dlt.common.json import _orjson as _json_or

        json = _json_or  # type: ignore[assignment]
    except ImportError:
        from dlt.common.json import _simplejson as _json_simple

        json = _json_simple  # type: ignore[assignment]


__all__ = [
    "json",
    "custom_encode",
    "custom_pua_encode",
    "custom_pua_decode",
    "custom_pua_decode_nested",
    "custom_pua_remove",
    "SupportsJson",
    "may_have_pua",
]
