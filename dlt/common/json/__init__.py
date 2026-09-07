import os
import base64
import dataclasses
from datetime import date, datetime, time  # noqa: I251
from typing import Any, Callable, List, Optional, Protocol, IO, Union, Dict
from uuid import UUID
from enum import Enum

from dlt.common import known_env
from dlt.common.libs import is_pydantic_model
from dlt.common.arithmetics import Decimal
from dlt.common.wei import Wei
from dlt.common.utils import map_nested_values_in_place  # noqa: F401
from dlt.common.libs.hexbytes import HexBytes

TPuaDecoders = List[Callable[[Any], Any]]

JsonSerializable = Union[str, Dict[str, Any]]
"""
Type representing a JSON-serializable object.
"""

JsonEncoder = Callable[[Any], JsonSerializable]
"""
A callable that takes an object and returns a JSON-serializable representation of it.
Should raise `TypeError` if the object cannot be serialized.
"""


_custom_encoder: Union[None, JsonEncoder] = None
"""
Holds the custom encoder function, if set.
This is used as a last-resort fallback for encoding objects.
"""


def _custom_encode(obj: Any) -> JsonSerializable:
    """Returns a JSON-serializable representation of `obj`"""
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
    elif is_pydantic_model(obj):
        return obj.model_dump()  # type: ignore[no-any-return]
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore
    elif isinstance(obj, Enum):
        return obj.value  # type: ignore[no-any-return]
    elif _custom_encoder is not None:
        return _custom_encoder(obj)
    raise TypeError(f"`{repr(obj)}` is not JSON serializable")


custom_encode: JsonEncoder = _custom_encode

DatetimeEncoder = Callable[[datetime], Any]
"""A callable that renders a `datetime` for JSON instead of `isoformat`."""


def custom_encode_datetimes(datetime_encoder: DatetimeEncoder) -> JsonEncoder:
    """`custom_encode` that hands every `datetime` to `datetime_encoder`"""

    def _encode(obj: Any) -> JsonSerializable:
        if isinstance(obj, datetime):
            return datetime_encoder(obj)  # type: ignore[no-any-return]
        return custom_encode(obj)

    return _encode


# use PUA range to encode additional types
PUA_START = int(os.environ.get(known_env.DLT_JSON_TYPED_PUA_START, "0xf026"), 16)

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
        # backwards compatibility for data encoded with previous dlt version
        # fromisoformat does not support Z suffix (until py3.11)
        obj = obj[:-1] + "+00:00"
    # stays naive when the string carries no offset
    return datetime.fromisoformat(obj)


# BREAKING: decoders return stdlib types, not pendulum. the encoded form is unchanged
DECODERS: TPuaDecoders = [
    Decimal,
    _datetime_decoder,
    date.fromisoformat,
    UUID,
    HexBytes,
    base64.b64decode,
    Wei,
    time.fromisoformat,
]
PUA_CHARACTER_MAX = len(DECODERS)


def _custom_pua_encode(obj: Any) -> JsonSerializable:
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
        return obj.asdict()  # type: ignore[no-any-return]
    elif hasattr(obj, "_asdict"):
        return obj._asdict()  # type: ignore[no-any-return]
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore[arg-type]
    elif is_pydantic_model(obj):
        return obj.dict(by_alias=True)  # type: ignore[no-any-return]
    elif isinstance(obj, Enum):
        # Enum value is just int or str
        return obj.value  # type: ignore[no-any-return]
    elif _custom_encoder is not None:
        return _custom_encoder(obj)
    raise TypeError(f"`{repr(obj)}` is not JSON serializable")


custom_pua_encode: JsonEncoder = _custom_pua_encode


def custom_pua_decode(obj: Any, decoders: TPuaDecoders = DECODERS) -> Any:
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - PUA_START
        # decode only the PUA space defined in DECODERS
        if c >= 0 and c <= PUA_CHARACTER_MAX:
            try:
                return decoders[c](obj[1:])
            except Exception:
                # return strings that cannot be parsed
                # this may be due
                # (1) someone exposing strings with PUA characters to external systems (ie. via API)
                # (2) using custom types ie. DateTime that does not create correct iso strings
                return obj
    return obj


def custom_pua_decode_nested(obj: Any, decoders: TPuaDecoders = DECODERS) -> Any:
    """Decodes PUA markers in `obj`, recursing into dicts and lists in place."""
    if isinstance(obj, str):
        if len(obj) > 1:
            c = ord(obj[0]) - PUA_START
            if c >= 0 and c <= PUA_CHARACTER_MAX:
                try:
                    return decoders[c](obj[1:])
                except Exception:
                    return obj
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                if len(v) > 1:
                    c = ord(v[0]) - PUA_START
                    if c >= 0 and c <= PUA_CHARACTER_MAX:
                        try:
                            obj[k] = decoders[c](v[1:])
                        except Exception:
                            pass
            elif isinstance(v, (dict, list)):
                custom_pua_decode_nested(v, decoders)
        return obj
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            if isinstance(v, str):
                if len(v) > 1:
                    c = ord(v[0]) - PUA_START
                    if c >= 0 and c <= PUA_CHARACTER_MAX:
                        try:
                            obj[idx] = decoders[c](v[1:])
                        except Exception:
                            pass
            elif isinstance(v, (dict, list)):
                custom_pua_decode_nested(v, decoders)
        return obj
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


class SupportsJson(Protocol):
    """Minimum adapter for different json parser implementations"""

    _impl_name: str
    """Implementation name"""

    def set_custom_encoder(self, encoder: JsonEncoder) -> None: ...

    """
    Set user-defined custom encoder.
    This encoder will be called if none of the built-in encoders can handle the object.
    """

    def dump(
        self, obj: Any, fp: IO[bytes], sort_keys: bool = False, pretty: bool = False
    ) -> None: ...

    def typed_dump(self, obj: Any, fp: IO[bytes], pretty: bool = False) -> None: ...

    def typed_dumps(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> str: ...

    def typed_loads(self, s: str) -> Any: ...

    def typed_dumpb(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes: ...

    def typed_loadb(
        self, s: Union[bytes, bytearray, memoryview], decoders: TPuaDecoders = DECODERS
    ) -> Any: ...

    def dumps(
        self,
        obj: Any,
        sort_keys: bool = False,
        pretty: bool = False,
        utc_z: bool = False,
        datetime_encoder: Optional[DatetimeEncoder] = None,
    ) -> str:
        """`utc_z` renders a UTC datetime with `Z` where the backend supports it, the pre-1.29 form.
        `datetime_encoder` renders every `datetime` instead of `isoformat`."""

    def dumpb(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes: ...

    def load(self, fp: Union[IO[bytes], IO[str]]) -> Any: ...

    def loads(self, s: str) -> Any: ...

    def loadb(self, s: Union[bytes, bytearray, memoryview]) -> Any: ...


def set_custom_encoder_impl(encoder: JsonEncoder) -> None:
    """
    This is passed through to each SupportsJson implementation and mutates the global custom encoder.
    """
    global _custom_encoder
    _custom_encoder = encoder


# pick the right impl
json: SupportsJson = None
if os.environ.get(known_env.DLT_USE_JSON) == "simplejson":
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
    "custom_encode_datetimes",
    "DatetimeEncoder",
    "custom_pua_encode",
    "custom_pua_decode",
    "custom_pua_decode_nested",
    "custom_pua_remove",
    "SupportsJson",
    "JsonSerializable",
    "JsonEncoder",
    "may_have_pua",
    "TPuaDecoders",
    "DECODERS",
]
