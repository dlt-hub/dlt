import base64
from datetime import date, datetime  # noqa: I251
from functools import partial
from typing import Any, Union
from uuid import UUID
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


simplejson.loads = partial(simplejson.loads, use_decimal=False)
simplejson.load = partial(simplejson.load, use_decimal=False)
# prevent default decimal serializer (use_decimal=False) and binary serializer (encoding=None)
simplejson.dumps = partial(simplejson.dumps, use_decimal=False, default=custom_encode, encoding=None)
simplejson.dump = partial(simplejson.dump, use_decimal=False, default=custom_encode, encoding=None)

# provide drop-in replacement
json = simplejson
