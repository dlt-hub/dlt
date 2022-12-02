import base64
import pendulum
from datetime import date, datetime  # noqa: I251
from functools import partial
from typing import Any, Callable, List, Union
from uuid import UUID
from hexbytes import HexBytes
import simplejson
from simplejson.raw_json import RawJSON
import platform

if platform.python_implementation() == "PyPy":
    # disable speedups on PyPy, it can be actually faster than Python C
    simplejson._toggle_speedups(False)  # type: ignore

from dlt.common.arithmetics import Decimal
from dlt.common.wei import Wei


def custom_encode(obj: Any) -> str:
    if isinstance(obj, Decimal):
        # always return decimals as string (not RawJSON) so they are not deserialized back to float
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


def custom_pua_encode(obj: Any) -> Union[RawJSON, str]:
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
    raise TypeError(repr(obj) + " is not JSON serializable")


def custom_pua_decode(obj: Any) -> Any:
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - 0xF026
        # decode only the PUA space defined in DECODERS
        if c >=0 and c <= 6:
            return DECODERS[c](obj[1:])
    return obj


def custom_pua_remove(obj: Any) -> Any:
    """Removes the PUA data type marker and leaves the correctly serialized type representation. Unmarked values are returned as-is."""
    if isinstance(obj, str) and len(obj) > 1:
        c = ord(obj[0]) - 0xF026
        # decode only the PUA space defined in DECODERS
        if c >=0 and c <= 6:
            return obj[1:]
    return obj


simplejson.loads = partial(simplejson.loads, use_decimal=False)
simplejson.load = partial(simplejson.load, use_decimal=False)
# prevent default decimal serializer (use_decimal=False) and binary serializer (encoding=None)
simplejson.dumps = partial(simplejson.dumps, use_decimal=False, default=custom_encode, encoding=None, ensure_ascii=False, separators=(',', ':'))
simplejson.dump = partial(simplejson.dump, use_decimal=False, default=custom_encode, encoding=None, ensure_ascii=False, separators=(',', ':'))

# provide drop-in replacement
json = simplejson
# helpers for typed dump
json_typed_dumps: Callable[..., str] = partial(simplejson.dumps, use_decimal=False, default=custom_pua_encode, encoding=None, ensure_ascii=False, separators=(',', ':'))
json_typed_dump: Callable[..., None] = partial(simplejson.dump, use_decimal=False, default=custom_pua_encode, encoding=None, ensure_ascii=False, separators=(',', ':'))
