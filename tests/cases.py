import datetime  # noqa: I251
import hashlib
from typing import Dict, List, Any, Sequence, Tuple, Literal, Union
import base64
from hexbytes import HexBytes
from copy import deepcopy
from string import ascii_lowercase
import random
import secrets

from dlt.common import Decimal, pendulum, json
from dlt.common.data_types import TDataType
from dlt.common.schema.utils import new_column
from dlt.common.typing import StrAny, TDataItems
from dlt.common.wei import Wei
from dlt.common.time import (
    ensure_pendulum_datetime,
    reduce_pendulum_datetime_precision,
    ensure_pendulum_time,
    ensure_pendulum_date,
)
from dlt.common.schema import TColumnSchema, TTableSchemaColumns

from tests.utils import TPythonTableFormat, TestDataItemFormat, arrow_item_from_pandas

# _UUID = "c8209ee7-ee95-4b90-8c9f-f7a0f8b51014"
JSON_TYPED_DICT: StrAny = {
    "str": "string",
    "decimal": Decimal("21.37"),
    "big_decimal": Decimal(
        "115792089237316195423570985008687907853269984665640564039457584007913129639935.1"
    ),
    "datetime": pendulum.parse("2005-04-02T20:37:37.358236Z"),
    "date": ensure_pendulum_date("2022-02-02"),
    # "uuid": UUID(_UUID),
    "hexbytes": HexBytes("0x2137"),
    "bytes": b"2137",
    "wei": Wei.from_int256(2137, decimals=2),
    "time": ensure_pendulum_time("20:37:37.358236"),
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
    "time": "time",
}

JSON_TYPED_DICT_NESTED = {
    "dict": dict(JSON_TYPED_DICT),
    "list_dicts": [dict(JSON_TYPED_DICT), dict(JSON_TYPED_DICT)],
    "list": list(JSON_TYPED_DICT.values()),
    **JSON_TYPED_DICT,
}
JSON_TYPED_DICT_NESTED_DECODED = {
    "dict": dict(JSON_TYPED_DICT_DECODED),
    "list_dicts": [dict(JSON_TYPED_DICT_DECODED), dict(JSON_TYPED_DICT_DECODED)],
    "list": list(JSON_TYPED_DICT_DECODED.values()),
    **JSON_TYPED_DICT_DECODED,
}

TABLE_UPDATE: List[TColumnSchema] = [
    {"name": "col1", "data_type": "bigint", "nullable": False},
    {"name": "col2", "data_type": "double", "nullable": False},
    {"name": "col3", "data_type": "bool", "nullable": False},
    {"name": "col4", "data_type": "timestamp", "nullable": False},
    {"name": "col5", "data_type": "text", "nullable": False},
    {"name": "col6", "data_type": "decimal", "nullable": False},
    {"name": "col7", "data_type": "binary", "nullable": False},
    {"name": "col8", "data_type": "wei", "nullable": False},
    {"name": "col9", "data_type": "json", "nullable": False, "variant": True},
    {"name": "col10", "data_type": "date", "nullable": False},
    {"name": "col11", "data_type": "time", "nullable": False},
    {"name": "col1_null", "data_type": "bigint", "nullable": True},
    {"name": "col2_null", "data_type": "double", "nullable": True},
    {"name": "col3_null", "data_type": "bool", "nullable": True},
    {"name": "col4_null", "data_type": "timestamp", "nullable": True},
    {"name": "col5_null", "data_type": "text", "nullable": True},
    {"name": "col6_null", "data_type": "decimal", "nullable": True},
    {"name": "col7_null", "data_type": "binary", "nullable": True},
    {"name": "col8_null", "data_type": "wei", "nullable": True},
    {"name": "col9_null", "data_type": "json", "nullable": True, "variant": True},
    {"name": "col10_null", "data_type": "date", "nullable": True},
    {"name": "col11_null", "data_type": "time", "nullable": True},
    {"name": "col1_precision", "data_type": "bigint", "precision": 16, "nullable": False},
    {"name": "col4_precision", "data_type": "timestamp", "precision": 3, "nullable": False},
    {"name": "col5_precision", "data_type": "text", "precision": 25, "nullable": False},
    {
        "name": "col6_precision",
        "data_type": "decimal",
        "precision": 6,
        "scale": 2,
        "nullable": False,
    },
    {"name": "col7_precision", "data_type": "binary", "precision": 19, "nullable": False},
    {"name": "col11_precision", "data_type": "time", "precision": 3, "nullable": False},
]
TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {c["name"]: c for c in TABLE_UPDATE}

TABLE_ROW_ALL_DATA_TYPES = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45.176451+00:00",
    "col5": "string data \n \r  🦆",
    "col6": Decimal("2323.34"),
    "col7": b"binary data \n \r ",
    "col8": 2**56 + 92093890840,
    "col9": {
        "nested": [1, 2, 3, "a"],
        "link": (
            "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6"
            " \\vity%3A69'08444473\n\n551163392%2C6n \r 9085"
        ),
    },
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
    "col5_precision": "string data 2 \n \r  🦆",
    "col6_precision": Decimal("2323.34"),
    "col7_precision": b"binary data 2 \n \r A",
    "col11_precision": "13:26:45.176451",
}


TABLE_ROW_ALL_DATA_TYPES_DATETIMES = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col4"] = ensure_pendulum_datetime(TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col4"])  # type: ignore[arg-type]
TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col10"] = ensure_pendulum_date(TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col10"])  # type: ignore[arg-type]
TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col11"] = pendulum.Time.fromisoformat(TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col11"])  # type: ignore[arg-type]
TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col4_precision"] = ensure_pendulum_datetime(TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col4_precision"])  # type: ignore[arg-type]
TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col11_precision"] = pendulum.Time.fromisoformat(TABLE_ROW_ALL_DATA_TYPES_DATETIMES["col11_precision"])  # type: ignore[arg-type]


TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS = [
    new_column("col1_ts", "timestamp", precision=0),
    new_column("col2_ts", "timestamp", precision=3),
    new_column("col3_ts", "timestamp", precision=6),
    new_column("col4_ts", "timestamp", precision=9),
]
TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS: TTableSchemaColumns = {
    c["name"]: c for c in TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS
}

TABLE_UPDATE_ALL_INT_PRECISIONS = [
    new_column("col1_int", "bigint", precision=8),
    new_column("col2_int", "bigint", precision=16),
    new_column("col3_int", "bigint", precision=32),
    new_column("col4_int", "bigint", precision=64),
    new_column("col5_int", "bigint", precision=128),
]
TABLE_UPDATE_ALL_INT_PRECISIONS_COLUMNS: TTableSchemaColumns = {
    c["name"]: c for c in TABLE_UPDATE_ALL_INT_PRECISIONS
}


def table_update_and_row(
    exclude_types: Sequence[TDataType] = None, exclude_columns: Sequence[str] = None
) -> Tuple[TTableSchemaColumns, Dict[str, Any]]:
    """Get a table schema and a row with all possible data types.
    Optionally exclude some data types from the schema and row.
    """
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)
    data_row = deepcopy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    exclude_col_names = list(exclude_columns or [])
    if exclude_types:
        exclude_col_names.extend(
            [key for key, value in column_schemas.items() if value["data_type"] in exclude_types]
        )
    for col_name in set(exclude_col_names):
        del column_schemas[col_name]
        del data_row[col_name]
    return column_schemas, data_row


def assert_all_data_types_row(
    db_row: Union[List[Any], TDataItems],
    expected_row: Dict[str, Any] = None,
    parse_json_strings: bool = False,
    allow_base64_binary: bool = False,
    timestamp_precision: int = 6,
    schema: TTableSchemaColumns = None,
    expect_filtered_null_columns=False,
    allow_string_binary: bool = False,
) -> None:
    # content must equal
    # print(db_row)
    schema = schema or TABLE_UPDATE_COLUMNS_SCHEMA
    expected_row = expected_row or TABLE_ROW_ALL_DATA_TYPES_DATETIMES

    # Include only columns requested in schema
    if isinstance(db_row, dict):
        db_mapping = db_row.copy()
    else:
        db_mapping = {col_name: db_row[i] for i, col_name in enumerate(schema)}

    expected_rows = {key: value for key, value in expected_row.items() if key in schema}
    # prepare date to be compared: convert into pendulum instance, adjust microsecond precision
    if "col4" in expected_rows:
        parsed_date = ensure_pendulum_datetime((db_mapping["col4"]))
        db_mapping["col4"] = reduce_pendulum_datetime_precision(parsed_date, timestamp_precision)
        expected_rows["col4"] = reduce_pendulum_datetime_precision(
            ensure_pendulum_datetime(expected_rows["col4"]),  # type: ignore[arg-type]
            timestamp_precision,
        )
    if "col4_precision" in expected_rows:
        parsed_date = ensure_pendulum_datetime((db_mapping["col4_precision"]))
        db_mapping["col4_precision"] = reduce_pendulum_datetime_precision(parsed_date, 3)
        expected_rows["col4_precision"] = reduce_pendulum_datetime_precision(
            ensure_pendulum_datetime(expected_rows["col4_precision"]), 3  # type: ignore[arg-type]
        )

    if "col10" in expected_rows:
        db_mapping["col10"] = ensure_pendulum_date(db_mapping["col10"])

    if "col11" in expected_rows:
        expected_rows["col11"] = reduce_pendulum_datetime_precision(
            ensure_pendulum_time(expected_rows["col11"]), timestamp_precision  # type: ignore[arg-type]
        ).isoformat()

    if "col11_precision" in expected_rows:
        parsed_time = ensure_pendulum_time(db_mapping["col11_precision"])
        db_mapping["col11_precision"] = reduce_pendulum_datetime_precision(parsed_time, 3)
        expected_rows["col11_precision"] = reduce_pendulum_datetime_precision(
            ensure_pendulum_time(expected_rows["col11_precision"]), 3  # type: ignore[arg-type]
        )

    # redshift and bigquery return strings from structured fields
    for binary_col in ["col7", "col7_precision"]:
        if binary_col in db_mapping:
            if isinstance(db_mapping[binary_col], str):
                try:
                    db_mapping[binary_col] = bytes.fromhex(
                        db_mapping[binary_col]
                    )  # redshift returns binary as hex string
                except ValueError:
                    if allow_string_binary:
                        db_mapping[binary_col] = db_mapping[binary_col].encode("utf-8")
                    elif allow_base64_binary:
                        db_mapping[binary_col] = base64.b64decode(
                            db_mapping[binary_col], validate=True
                        )
                    else:
                        raise
            else:
                db_mapping[binary_col] = bytes(db_mapping[binary_col])

    # `delta` table format stores `wei` type as string
    if "col8" in db_mapping:
        if isinstance(db_mapping["col8"], str):
            db_mapping["col8"] = int(db_mapping["col8"])

    # redshift and bigquery return strings from structured fields
    if "col9" in db_mapping:
        if isinstance(db_mapping["col9"], str):
            # then it must be json
            db_mapping["col9"] = json.loads(db_mapping["col9"])
        # parse again
        if parse_json_strings and isinstance(db_mapping["col9"], str):
            # then it must be json
            db_mapping["col9"] = json.loads(db_mapping["col9"])

    # if "col10" in db_mapping:
    #     db_mapping["col10"] = db_mapping["col10"].isoformat()
    if "col11" in db_mapping:
        db_mapping["col11"] = ensure_pendulum_time(db_mapping["col11"]).isoformat()

    if expect_filtered_null_columns:
        for key, expected in expected_rows.items():
            if expected is None:
                assert db_mapping.get(key, None) is None
                db_mapping[key] = None

    for key, expected in expected_rows.items():
        actual = db_mapping[key]
        assert expected == actual, f"Expected {expected} but got {actual} for column {key}"

    assert db_mapping == expected_rows


def arrow_table_all_data_types(
    object_format: TestDataItemFormat,
    include_json: bool = True,
    include_time: bool = True,
    include_binary: bool = True,
    include_decimal: bool = True,
    include_decimal_default_precision: bool = False,
    include_decimal_arrow_max_precision: bool = False,
    include_date: bool = True,
    include_not_normalized_name: bool = True,
    include_name_clash: bool = False,
    include_null: bool = True,
    num_rows: int = 3,
    tz="UTC",
) -> Tuple[Any, List[Dict[str, Any]], Dict[str, List[Any]]]:
    """Create an arrow object or pandas dataframe with all supported data types.

    Returns the table and its records in python format
    """
    import pandas as pd
    import numpy as np

    data = {
        "string": [secrets.token_urlsafe(8) + "\"'\\🦆\n\r" for _ in range(num_rows)],
        "float": [round(random.uniform(0, 100), 4) for _ in range(num_rows)],
        "int": [random.randrange(0, 100) for _ in range(num_rows)],
        "datetime": pd.date_range("2021-01-01T01:02:03.1234", periods=num_rows, tz=tz, unit="us"),
        "bool": [random.choice([True, False]) for _ in range(num_rows)],
        "string_null": [random.choice(ascii_lowercase) for _ in range(num_rows - 1)] + [None],
        "float_null": [round(random.uniform(0, 100), 4) for _ in range(num_rows - 1)] + [
            None
        ],  # decrease precision
    }

    if include_null:
        data["null"] = pd.Series([None for _ in range(num_rows)])

    if include_name_clash:
        data["pre Normalized Column"] = [random.choice(ascii_lowercase) for _ in range(num_rows)]
        include_not_normalized_name = True

    if include_not_normalized_name:
        data["Pre Normalized Column"] = [random.choice(ascii_lowercase) for _ in range(num_rows)]

    if include_json:
        data["json"] = [{"a": random.randrange(0, 100)} for _ in range(num_rows)]

    if include_time:
        # data["time"] = pd.date_range("2021-01-01", periods=num_rows, tz="UTC").time
        # data["time"] = pd.date_range("2021-01-01T01:02:03.1234", periods=num_rows, tz=tz, unit="us").time
        # random time objects with different hours/minutes/seconds/microseconds
        data["time"] = [
            datetime.time(
                random.randrange(0, 24),
                random.randrange(0, 60),
                random.randrange(0, 60),
                random.randrange(0, 1000000),
            )
            for _ in range(num_rows)
        ]

    if include_binary:
        # "binary": [hashlib.sha3_256(random.choice(ascii_lowercase).encode()).digest() for _ in range(num_rows)],
        data["binary"] = [random.choice(ascii_lowercase).encode() for _ in range(num_rows)]

    if include_decimal:
        data["decimal"] = [Decimal(str(round(random.uniform(0, 100), 4))) for _ in range(num_rows)]

    if include_decimal_default_precision:
        from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION

        data["decimal_default_precision"] = [
            Decimal(int("1" * DEFAULT_NUMERIC_PRECISION)) for _ in range(num_rows)
        ]

    if include_decimal_arrow_max_precision:
        from dlt.common.libs.pyarrow import ARROW_DECIMAL_MAX_PRECISION

        data["decimal_arrow_max_precision"] = [
            Decimal(int("1" * ARROW_DECIMAL_MAX_PRECISION)) for _ in range(num_rows)
        ]

    if include_date:
        data["date"] = pd.date_range("2021-01-01", periods=num_rows, tz=tz).date

    df = pd.DataFrame(data)
    # None integers/floats are converted to nan, also replaces floats with objects and loses precision
    df = df.replace(np.nan, None)
    # records have normalized identifiers for comparing
    rows = (
        df.rename(
            columns={
                "Pre Normalized Column": "pre_normalized_column",
            }
        )
        .drop(columns=(["null"] if include_null else []))
        .to_dict("records")
    )
    if object_format == "object":
        return rows, rows, data
    else:
        return arrow_item_from_pandas(df, object_format), rows, data


def prepare_shuffled_tables() -> Tuple[Any, Any, Any]:
    from dlt.common.libs.pyarrow import remove_columns
    from dlt.common.libs.pyarrow import pyarrow as pa

    table, _, _ = arrow_table_all_data_types(
        "arrow-table",
        include_json=False,
        include_not_normalized_name=False,
        tz="Europe/Berlin",
        num_rows=5432,
    )
    # remove null column from table (it will be removed in extract)
    table = remove_columns(table, "null")
    # shuffled_columns = table.schema.names
    shuffled_indexes = list(range(len(table.schema.names)))
    random.shuffle(shuffled_indexes)
    shuffled_table = pa.Table.from_arrays(
        [table.column(idx) for idx in shuffled_indexes],
        schema=pa.schema([table.schema.field(idx) for idx in shuffled_indexes]),
    )
    shuffled_removed_column = remove_columns(shuffled_table, ["binary"])
    assert shuffled_table.schema.names != table.schema.names
    return table, shuffled_table, shuffled_removed_column
