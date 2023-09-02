from typing import Dict, List, Any
import base64
from hexbytes import HexBytes

from dlt.common import Decimal, pendulum, json
from dlt.common.data_types import TDataType
from dlt.common.typing import StrAny
from dlt.common.wei import Wei
from dlt.common.time import ensure_pendulum_datetime, reduce_pendulum_datetime_precision
from dlt.common.schema import TColumnSchema, TTableSchemaColumns


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
    "wei": Wei.from_int256(2137, decimals=2),
    "time": pendulum.parse("2005-04-02T20:37:37.358236Z").time()
}
# TODO: a version after PUA decoder (time is not yet implemented end to end)
JSON_TYPED_DICT_DECODED = dict(JSON_TYPED_DICT)
JSON_TYPED_DICT_DECODED["time"] = JSON_TYPED_DICT["time"].isoformat()

JSON_TYPED_DICT_TYPES: Dict[str, TDataType] = {
    "str": "text",
    "decimal": "decimal",
    "big_decimal": "decimal",
    "datetime": "timestamp",
    "date": "date",
    # "uuid": "text",
    "hexbytes": "binary",
    "bytes": "binary",
    "wei": "wei",
    "time": "text"
}

JSON_TYPED_DICT_NESTED = {
    "dict": dict(JSON_TYPED_DICT),
    "list_dicts": [dict(JSON_TYPED_DICT), dict(JSON_TYPED_DICT)],
    "list": list(JSON_TYPED_DICT.values()),
    **JSON_TYPED_DICT
}
JSON_TYPED_DICT_NESTED_DECODED = {
    "dict": dict(JSON_TYPED_DICT_DECODED),
    "list_dicts": [dict(JSON_TYPED_DICT_DECODED), dict(JSON_TYPED_DICT_DECODED)],
    "list": list(JSON_TYPED_DICT_DECODED.values()),
    **JSON_TYPED_DICT_DECODED
}

TABLE_UPDATE: List[TColumnSchema] = [
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    },
    {
        "name": "col2",
        "data_type": "double",
        "nullable": False
    },
    {
        "name": "col3",
        "data_type": "bool",
        "nullable": False
    },
    {
        "name": "col4",
        "data_type": "timestamp",
        "nullable": False
    },
    {
        "name": "col5",
        "data_type": "text",
        "nullable": False
    },
    {
        "name": "col6",
        "data_type": "decimal",
        "nullable": False
    },
    {
        "name": "col7",
        "data_type": "binary",
        "nullable": False
    },
    {
        "name": "col8",
        "data_type": "wei",
        "nullable": False
    },
    {
        "name": "col9",
        "data_type": "complex",
        "nullable": False,
        "variant": True
    },
    {
        "name": "col10",
        "data_type": "date",
        "nullable": False
    },
    {
        "name": "col11",
        "data_type": "time",
        "nullable": False
    },
    {
        "name": "col1_null",
        "data_type": "bigint",
        "nullable": True
    },
    {
        "name": "col2_null",
        "data_type": "double",
        "nullable": True
    },
    {
        "name": "col3_null",
        "data_type": "bool",
        "nullable": True
    },
    {
        "name": "col4_null",
        "data_type": "timestamp",
        "nullable": True
    },
    {
        "name": "col5_null",
        "data_type": "text",
        "nullable": True
    },
    {
        "name": "col6_null",
        "data_type": "decimal",
        "nullable": True
    },
    {
        "name": "col7_null",
        "data_type": "binary",
        "nullable": True
    },
    {
        "name": "col8_null",
        "data_type": "wei",
        "nullable": True
    },
    {
        "name": "col9_null",
        "data_type": "complex",
        "nullable": True,
        "variant": True
    },
    {
        "name": "col10_null",
        "data_type": "date",
        "nullable": True
    },
    {
        "name": "col11_null",
        "data_type": "time",
        "nullable": True
    },
]
TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {t["name"]:t for t in TABLE_UPDATE}

TABLE_ROW_ALL_DATA_TYPES  = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45.176451+00:00",
    "col5": "string data \n \r \x8e 🦆",
    "col6": Decimal("2323.34"),
    "col7": b'binary data \n \r \x8e',
    "col8": 2**56 + 92093890840,
    "col9": {"complex":[1,2,3,"a"], "link": "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6 \\vity%3A69'08444473\n\n551163392%2C6n \r \x8e9085"},
    "col10": "2023-02-27",
    "col11": "13:26:45.176451",
    "col1_null": None,
    "col2_null": None,
    "col3_null": None,
    "col4_null": None,
    "col5_null": None,
    "col6_null": None,
    "col7_null": None,
    "col8_null": None,
    "col9_null": None,
    "col10_null": None,
    "col11_null": None,
}


def assert_all_data_types_row(
    db_row: List[Any],
    parse_complex_strings: bool = False,
    allow_base64_binary: bool = False,
    timestamp_precision:int = 6
) -> None:
    # content must equal
    # print(db_row)
    # prepare date to be compared: convert into pendulum instance, adjust microsecond precision
    expected_rows = list(TABLE_ROW_ALL_DATA_TYPES.values())
    parsed_date = pendulum.instance(db_row[3])
    db_row[3] = reduce_pendulum_datetime_precision(parsed_date, timestamp_precision)
    expected_rows[3] = reduce_pendulum_datetime_precision(ensure_pendulum_datetime(expected_rows[3]), timestamp_precision)

    if isinstance(db_row[6], str):
        try:
            db_row[6] = bytes.fromhex(db_row[6])  # redshift returns binary as hex string
        except ValueError:
            if not allow_base64_binary:
                raise
            db_row[6] = base64.b64decode(db_row[6], validate=True)
    else:
        db_row[6] = bytes(db_row[6])
    # redshift and bigquery return strings from structured fields
    if isinstance(db_row[8], str):
        # then it must be json
        db_row[8] = json.loads(db_row[8])
    # parse again
    if parse_complex_strings and isinstance(db_row[8], str):
        # then it must be json
        db_row[8] = json.loads(db_row[8])

    db_row[9] = db_row[9].isoformat()
    db_row[10] = db_row[10].isoformat()
    # print(db_row)
    # print(expected_rows)
    for expected, actual in zip(expected_rows, db_row):
        assert expected == actual
    assert db_row == expected_rows
