from uuid import UUID
from hexbytes import HexBytes

from dlt.common import Decimal, pendulum

_UUID = "c8209ee7-ee95-4b90-8c9f-f7a0f8b51014"
JSON_TYPED_DICT = {
        "decimal": Decimal("21.37"),
        "datetime": pendulum.parse("2005-04-02T20:37:37.358236Z"),
        "date": pendulum.parse("2022-02-02").date(),
        "uuid": UUID(_UUID),
        "hexbytes": HexBytes("0x2137"),
        "bytes": b'2137'
    }

JSON_TYPED_DICT_TYPES = {
    "decimal": "decimal",
    "datetime": "timestamp",
    "date": "text",
    "uuid": "text",
    "hexbytes": "binary",
    "bytes": "binary"
}
