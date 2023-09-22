from typing import Dict, List, Any, Sequence, Tuple
import base64
from hexbytes import HexBytes
from copy import deepcopy

from dlt.common import Decimal, pendulum, json
from dlt.common.data_types import TDataType
from dlt.common.typing import StrAny
from dlt.common.wei import Wei
from dlt.common.time import ensure_pendulum_datetime, reduce_pendulum_datetime_precision, ensure_pendulum_time
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
    "time": "time"
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
    {
        "name": "col1_precision",
        "data_type": "bigint",
        "precision": 16,
        "nullable": False
    },
    {
        "name": "col4_precision",
        "data_type": "timestamp",
        "precision": 3,
        "nullable": False
    },
    {
        "name": "col5_precision",
        "data_type": "text",
        "precision": 21,
        "nullable": False
    },
    {
        "name": "col6_precision",
        "data_type": "decimal",
        "precision": 6,
        "scale": 2,
        "nullable": False
    },
    {
        "name": "col7_precision",
        "data_type": "binary",
        "precision": 19,
        "nullable": False
    },
    {
        "name": "col11_precision",
        "data_type": "time",
        "precision": 3,
        "nullable": False
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
    "col1_precision": 22324,
    "col4_precision": "2022-05-23T13:26:46.167231+00:00",
    "col5_precision": "string data 2 \n \r \x8e 🦆",
    "col6_precision": Decimal("2323.34"),
    "col7_precision": b'binary data 2 \n \r \x8e',
    "col11_precision": "13:26:45.176451",
}


def table_update_and_row(exclude_types: Sequence[TDataType] = None) -> Tuple[TTableSchemaColumns, StrAny]:
    """Get a table schema and a row with all possible data types.
    Optionally exclude some data types from the schema and row.
    """
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)
    data_row = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    if exclude_types:
        exclude_col_names = [key for key, value in column_schemas.items() if value["data_type"] in exclude_types]
        for col_name in exclude_col_names:
            del column_schemas[col_name]
            del data_row[col_name]
    return column_schemas, data_row


def assert_all_data_types_row(
    db_row: List[Any],
    parse_complex_strings: bool = False,
    allow_base64_binary: bool = False,
    timestamp_precision:int = 6,
    schema: TTableSchemaColumns = None,
) -> None:
    # content must equal
    # print(db_row)
    schema = schema or TABLE_UPDATE_COLUMNS_SCHEMA

    # Include only columns requested in schema
    db_mapping = {col_name: db_row[i] for i, col_name in enumerate(schema)}
    expected_rows = {key: value for key, value in TABLE_ROW_ALL_DATA_TYPES.items() if key in schema}
    # prepare date to be compared: convert into pendulum instance, adjust microsecond precision
    if "col4" in expected_rows:
        parsed_date = pendulum.instance(db_mapping["col4"])
        db_mapping["col4"] = reduce_pendulum_datetime_precision(parsed_date, timestamp_precision)
        expected_rows['col4'] = reduce_pendulum_datetime_precision(
            ensure_pendulum_datetime(expected_rows["col4"]),  # type: ignore[arg-type]
            timestamp_precision
        )
    if "col4_precision" in expected_rows:
        parsed_date = pendulum.instance(db_mapping["col4_precision"])
        db_mapping["col4_precision"] = reduce_pendulum_datetime_precision(parsed_date, 3)
        expected_rows['col4_precision'] = reduce_pendulum_datetime_precision(
            ensure_pendulum_datetime(expected_rows["col4_precision"]),  # type: ignore[arg-type]
            3
        )

    if "col11_precision" in expected_rows:
        parsed_time = ensure_pendulum_time(db_mapping["col11_precision"])
        db_mapping["col11_precision"] = reduce_pendulum_datetime_precision(parsed_time, 3)
        expected_rows['col11_precision'] = reduce_pendulum_datetime_precision(
            ensure_pendulum_time(expected_rows["col11_precision"]),  # type: ignore[arg-type]
            3
        )

    # binary column
    if "col7" in db_mapping:
        if isinstance(db_mapping["col7"], str):
            try:
                db_mapping["col7"] = bytes.fromhex(db_mapping["col7"])  # redshift returns binary as hex string
            except ValueError:
                if not allow_base64_binary:
                    raise
                db_mapping["col7"] = base64.b64decode(db_mapping["col7"], validate=True)
        else:
            db_mapping["col7"] = bytes(db_mapping["col7"])

    # redshift and bigquery return strings from structured fields
    if "col9" in db_mapping:
        if isinstance(db_mapping["col9"], str):
            # then it must be json
            db_mapping["col9"] = json.loads(db_mapping["col9"])
        # parse again
        if parse_complex_strings and isinstance(db_mapping["col9"], str):
            # then it must be json
            db_mapping["col9"] = json.loads(db_mapping["col9"])

    if "col10" in db_mapping:
        db_mapping["col10"] = db_mapping["col10"].isoformat()
    if "col11" in db_mapping:
        db_mapping["col11"] = db_mapping["col11"].isoformat()

    for expected, actual in zip(expected_rows.values(), db_mapping.values()):
        assert expected == actual
    assert db_mapping == expected_rows
