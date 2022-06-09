import base64
import pendulum
from datetime import date, datetime  # noqa: I251
from functools import partial
from typing import Any, Callable, Union
from uuid import UUID, uuid4
from hexbytes import HexBytes
import simplejson
from simplejson.raw_json import RawJSON

from dlt.common.arithmetics import Decimal

# simplejson._toggle_speedups(False)

def custom_encode(obj: Any) -> Union[RawJSON, str]:
    if isinstance(obj, Decimal):
        # always return decimals as string (not RawJSON) so they are not deserialized back to float
        return str(obj.normalize())
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
_DECIMAL = u'\uF026'
_DATETIME = u'\uF027'
_DATE = u'\uF028'
_UUIDT = u'\uF029'
_HEXBYTES = u'\uF02A'
_B64BYTES = u'\uF02B'

DECODERS = [
    lambda s: Decimal(s),
    lambda s: pendulum.parse(s),
    lambda s: pendulum.parse(s).date(),  # type: ignore
    lambda s: UUID(s),
    lambda s: HexBytes(s),
    lambda s: base64.b64decode(s)
]


def custom_pua_encode(obj: Any) -> Union[RawJSON, str]:
    if isinstance(obj, Decimal):
        return _DECIMAL + str(obj.normalize())
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
        if c >= 0 and c <= 5:
            return DECODERS[c](obj[1:])
    return obj


simplejson.loads = partial(simplejson.loads, use_decimal=False)
simplejson.load = partial(simplejson.load, use_decimal=False)
# prevent default decimal serializer (use_decimal=False) and binary serializer (encoding=None)
simplejson.dumps = partial(simplejson.dumps, use_decimal=False, default=custom_encode, encoding=None)
simplejson.dump = partial(simplejson.dump, use_decimal=False, default=custom_encode, encoding=None)

# provide drop-in replacement
json = simplejson
# helpers for typed dump
json_typed_dumps: Callable[..., str] = partial(simplejson.dumps, use_decimal=False, default=custom_pua_encode, encoding=None)
json_typed_dump: Callable[..., None] = partial(simplejson.dump, use_decimal=False, default=custom_pua_encode, encoding=None)
