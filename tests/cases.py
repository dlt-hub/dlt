from typing import Dict
from uuid import UUID
from hexbytes import HexBytes

from dlt.common import Decimal, pendulum
from dlt.common.data_types import TDataType
from dlt.common.typing import StrAny
from dlt.common.wei import Wei


# _UUID = "c8209ee7-ee95-4b90-8c9f-f7a0f8b51014"
JSON_TYPED_DICT: StrAny = {
    "str": "string",
    "decimal": Decimal("21.37"),
    "big_decimal": Decimal("115792089237316195423570985008687907853269984665640564039457584007913129639935.1"),
    "datetime": pendulum.parse("2005-04-02T20:37:37.358236Z"),
    "date": pendulum.parse("2022-02-02").date(),
    # "uuid": UUID(_UUID),
    "hexbytes": HexBytes("0x2137"),
    "bytes": b'2137',
    "wei": Wei.from_int256(2137, decimals=2)
}

JSON_TYPED_DICT_TYPES: Dict[str, TDataType] = {
    "str": "text",
    "decimal": "decimal",
    "big_decimal": "decimal",
    "datetime": "timestamp",
    "date": "date",
    # "uuid": "text",
    "hexbytes": "binary",
    "bytes": "binary",
    "wei": "wei"
}
