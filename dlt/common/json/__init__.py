import os
import base64
import pendulum
import dataclasses
from datetime import date, datetime  # noqa: I251
from typing import Any, Callable, List, Protocol, IO, Union
from uuid import UUID
from hexbytes import HexBytes


from dlt.common.arithmetics import Decimal
from dlt.common.wei import Wei
from dlt.common.utils import map_nested_in_place


class SupportsJson(Protocol):
    """Minimum adapter for different json parser implementations"""

    _impl_name: str
    """Implementation name"""

    def dump(self, obj: Any, fp: IO[bytes], sort_keys: bool = False, pretty:bool = False) -> None:
        ...

    def typed_dump(self, obj: Any, fp: IO[bytes], pretty:bool = False) -> None:
        ...

    def typed_dumps(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> str:
        ...

    def typed_loads(self, s: str) -> Any:
        ...

    def typed_dumpb(self, obj: Any, sort_keys: bool = False, pretty: bool = False) -> bytes:
        ...

    def typed_loadb(self, s: Union[bytes, bytearray, memoryview]) -> Any:
        ...

    def dumps(self, obj: Any, sort_keys: bool = False, pretty:bool = False) -> str:
        ...

    def dumpb(self, obj: Any, sort_keys: bool = False, pretty:bool = False) -> bytes:
        ...

    def load(self, fp: IO[bytes]) -> Any:
        ...

    def loads(self, s: str) -> Any:
        ...

    def loadb(self, s: Union[bytes, bytearray, memoryview]) -> Any:
        ...


def custom_encode(obj: Any) -> str:
    if isinstance(obj, Decimal):
        # always return decimals as string so they are not deserialized back to float
        return str(obj)
    # this works both for standard datetime and pendulum
    elif isinstance(obj, datetime):
        # See "Date Time String Format" in the ECMA-262 specification.
        r = obj.isoformat()
        # leave microseconds alone
        # if obj.microsecond:
        #     r = r[:23] + r[26:]
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, HexBytes):
        return obj.hex()
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')
    elif hasattr(obj, 'asdict'):
        return obj.asdict()  # type: ignore
    elif hasattr(obj, '_asdict'):
        return obj._asdict()  # type: ignore
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore
    raise TypeError(repr(obj) + " is not JSON serializable")


# use PUA range to encode additional types
_DECIMAL = '\uF026'
_DATETIME = '\uF027'
_DATE = '\uF028'
_UUIDT = '\uF029'
_HEXBYTES = '\uF02A'
_B64BYTES = '\uF02B'
_WEI = '\uF02C'

DECODERS: List[Callable[[Any], Any]] = [
    Decimal,
    pendulum.parse,
    lambda s: pendulum.parse(s).date(),  # type: ignore
    UUID,
    HexBytes,
    base64.b64decode,
    Wei
]


def custom_pua_encode(obj: Any) -> str:
    # wei is subclass of decimal and must be checked first
    if isinstance(obj, Wei):
        return _WEI + str(obj)
    elif isinstance(obj, Decimal):
        return _DECIMAL + str(obj)
    # this works both for standard datetime and pendulum
    elif isinstance(obj, datetime):
        r = obj.isoformat()
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return _DATETIME + r
    elif isinstance(obj, date):
        return _DATE + obj.isoformat()
    elif isinstance(obj, UUID):
        return _UUIDT + str(obj)
    elif isinstance(obj, HexBytes):
        return _HEXBYTES + obj.hex()
    elif isinstance(obj, bytes):
        return _B64BYTES + base64.b64encode(obj).decode('ascii')
    elif hasattr(obj, 'asdict'):
        return obj.asdict()  # type: ignore
    elif hasattr(obj, '_asdict'):
        return obj._asdict()  # type: ignore
    elif dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)  # type: ignore
    raise TypeError(repr(obj) + " is not JSON serializable")


def custom_pua_decode(obj: Any) -> Any:
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - 0xF026
        # decode only the PUA space defined in DECODERS
        if c >=0 and c <= 6:
            return DECODERS[c](obj[1:])
    return obj


def custom_pua_decode_nested(obj: Any) -> Any:
    if isinstance(obj, str):
        return custom_pua_decode
    elif isinstance(obj, (list, dict)):
        return map_nested_in_place(custom_pua_decode, obj)
    return obj


def custom_pua_remove(obj: Any) -> Any:
    """Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is."""
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - 0xF026
        # decode only the PUA space defined in DECODERS
        if c >=0 and c <= 6:
            return obj[1:]
    return obj


# pick the right impl
json: SupportsJson = None
if os.environ.get("DLT_USE_JSON") == "simplejson":
    from dlt.common.json import _simplejson as _json_d
    json = _json_d
else:
    try:
        from dlt.common.json import _orjson as _json_or
        json = _json_or
    except ImportError:
        from dlt.common.json import _simplejson as _json_simple
        json = _json_simple
