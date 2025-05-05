from typing import Optional

import pytest
import sqlglot.expressions as sge

from dlt.common.libs.sqlglot import (
    from_sqlglot_type,
    to_sqlglot_type,
)
from dlt.common.schema.typing import TDataType


@pytest.mark.parametrize(
    "sqlglot_type", ["BIGINT", "bigint", sge.DataType.build("bigint"), sge.DataType.Type.BIGINT]
)
def test_to_sqlglot_input_types(sqlglot_type):
    dlt_hints = from_sqlglot_type(sqlglot_type)
    assert dlt_hints.get("data_type") == "bigint"


@pytest.mark.parametrize(
    "dlt_type, expected_sqlglot_type",
    [
        ("json", sge.DataType.Type.JSON),
        ("text", sge.DataType.Type.TEXT),
        ("double", sge.DataType.Type.DOUBLE),
        ("bool", sge.DataType.Type.BOOLEAN),
        ("date", sge.DataType.Type.DATE),
        ("timestamp", sge.DataType.Type.TIMESTAMP),
        ("bigint", sge.DataType.Type.BIGINT),
        ("binary", sge.DataType.Type.VARBINARY),
        ("time", sge.DataType.Type.TIME),
        ("decimal", sge.DataType.Type.DECIMAL),
        ("wei", sge.DataType.Type.UINT256),
    ],
)
def test_to_sqlglot(
    dlt_type: TDataType,
    expected_sqlglot_type: sge.DataType.Type,
) -> None:
    sqlglot_type = to_sqlglot_type(dlt_type)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


def _from_sqlglot_cases() -> list[tuple[sge.DataType.Type, Optional[TDataType]]]:
    mapping: dict[sge.DataType.Type, Optional[TDataType]] = {
        sge.DataType.Type.OBJECT: "json",
        sge.DataType.Type.STRUCT: "json",
        sge.DataType.Type.NESTED: "json",
        sge.DataType.Type.UNION: "json",
        sge.DataType.Type.ARRAY: "json",
        sge.DataType.Type.LIST: "json",
        sge.DataType.Type.JSON: "json",
        sge.DataType.Type.VECTOR: "json",
        # TEXT
        sge.DataType.Type.CHAR: "text",
        sge.DataType.Type.NCHAR: "text",
        sge.DataType.Type.NVARCHAR: "text",
        sge.DataType.Type.TEXT: "text",
        sge.DataType.Type.VARCHAR: "text",
        sge.DataType.Type.NAME: "text",
        # SIGNED_INTEGER
        sge.DataType.Type.BIGINT: "bigint",
        sge.DataType.Type.INT: "bigint",
        sge.DataType.Type.INT128: "bigint",
        sge.DataType.Type.INT256: "bigint",
        sge.DataType.Type.MEDIUMINT: "bigint",
        sge.DataType.Type.SMALLINT: "bigint",
        sge.DataType.Type.TINYINT: "bigint",
        # UNSIGNED_INTEGER
        sge.DataType.Type.UBIGINT: "bigint",
        sge.DataType.Type.UINT: "bigint",
        sge.DataType.Type.UINT128: "bigint",
        sge.DataType.Type.UINT256: "bigint",
        sge.DataType.Type.UMEDIUMINT: "bigint",
        sge.DataType.Type.USMALLINT: "bigint",
        sge.DataType.Type.UTINYINT: "bigint",
        # other INTEGER
        sge.DataType.Type.BIT: "bigint",
        # FLOAT
        sge.DataType.Type.DOUBLE: "double",
        sge.DataType.Type.FLOAT: "double",
        # DECIMAL
        sge.DataType.Type.BIGDECIMAL: "decimal",
        sge.DataType.Type.DECIMAL: "decimal",
        sge.DataType.Type.DECIMAL32: "decimal",
        sge.DataType.Type.DECIMAL64: "decimal",
        sge.DataType.Type.DECIMAL128: "decimal",
        sge.DataType.Type.DECIMAL256: "decimal",
        sge.DataType.Type.MONEY: "decimal",
        sge.DataType.Type.SMALLMONEY: "decimal",
        sge.DataType.Type.UDECIMAL: "decimal",
        sge.DataType.Type.UDOUBLE: "decimal",
        # TEMPORAL
        sge.DataType.Type.DATE: "date",
        sge.DataType.Type.DATE32: "date",
        sge.DataType.Type.DATETIME: "date",
        sge.DataType.Type.DATETIME2: "date",
        sge.DataType.Type.DATETIME64: "date",
        sge.DataType.Type.SMALLDATETIME: "date",
        sge.DataType.Type.TIMESTAMP: "timestamp",
        sge.DataType.Type.TIMESTAMPNTZ: "timestamp",
        sge.DataType.Type.TIMESTAMPLTZ: "timestamp",
        sge.DataType.Type.TIMESTAMPTZ: "timestamp",
        sge.DataType.Type.TIMESTAMP_MS: "timestamp",
        sge.DataType.Type.TIMESTAMP_NS: "timestamp",
        sge.DataType.Type.TIMESTAMP_S: "timestamp",
        sge.DataType.Type.TIME: "time",
        sge.DataType.Type.TIMETZ: "time",
        # binary
        sge.DataType.Type.VARBINARY: "binary",
        # BOOLEAN
        sge.DataType.Type.BOOLEAN: "bool",
        # UNKNOWN
        sge.DataType.Type.UNKNOWN: None,
    }

    # "text" is the default dlt data_type
    return [(sqlglot_type, mapping.get(sqlglot_type, "text")) for sqlglot_type in sge.DataType.Type]


@pytest.mark.parametrize(
    "sqlglot_type, expected_dlt_type",
    _from_sqlglot_cases(),
)
def test_from_sqlglot(sqlglot_type: sge.DataType.Type, expected_dlt_type: TDataType) -> None:
    dlt_hints = from_sqlglot_type(sqlglot_type)
    assert dlt_hints.get("data_type") == expected_dlt_type


@pytest.mark.parametrize(
    "precision, expected_sqlglot_type",
    [
        (-1, sge.DataType.Type.BIGINT),
        (0, sge.DataType.Type.BIGINT),
        (3, sge.DataType.Type.TINYINT),
        (5, sge.DataType.Type.SMALLINT),
        (8, sge.DataType.Type.MEDIUMINT),
        (10, sge.DataType.Type.INT),
        (19, sge.DataType.Type.BIGINT),
        (39, sge.DataType.Type.INT128),
        (78, sge.DataType.Type.INT256),
    ],
)
def test_to_sqlglot_integer_with_precision(
    precision: int,
    expected_sqlglot_type: sge.DataType.Type,
) -> None:
    sqlglot_type = to_sqlglot_type(dlt_type="bigint", precision=precision)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_precision",
    [
        (sge.DataType.Type.TINYINT, 3),
        (sge.DataType.Type.SMALLINT, 5),
        (sge.DataType.Type.MEDIUMINT, 8),
        (sge.DataType.Type.INT, 10),
        (sge.DataType.Type.BIGINT, None),  # expect None because BIGINT is default
        (sge.DataType.Type.INT128, 39),
        (sge.DataType.Type.INT256, 78),
        (sge.DataType.Type.UTINYINT, 3),
        (sge.DataType.Type.USMALLINT, 5),
        (sge.DataType.Type.UMEDIUMINT, 8),
        (sge.DataType.Type.UINT, 10),
        (sge.DataType.Type.UBIGINT, 19),
        (sge.DataType.Type.UINT128, 39),
        (sge.DataType.Type.UINT256, 78),
    ],
)
def test_from_sqlglot_integer_with_precision(
    sqlglot_type: sge.DataType.Type,
    expected_precision: int,
) -> None:
    dlt_hints = from_sqlglot_type(sqlglot_type)
    expected_hints = {"data_type": "bigint"}
    if expected_precision:
        expected_hints["precision"] = expected_precision

    assert dlt_hints == expected_hints


@pytest.mark.parametrize("nullable", [None, True, False])
def test_to_sqlglot_with_nullable(nullable: Optional[bool]) -> None:
    sqlglot_type = to_sqlglot_type("bigint", nullable=nullable)
    if nullable is None:
        assert "nullable" not in sqlglot_type.args
    else:
        assert sqlglot_type.args.get("nullable") == nullable


@pytest.mark.parametrize("nullable", [None, True, False])
def test_from_sqlglot_with_nullable(nullable: Optional[bool]) -> None:
    sqlglot_type = (
        sge.DataType.build("bigint")
        if nullable is None
        else sge.DataType.build("bigint", nullable=nullable)
    )

    dlt_type = from_sqlglot_type(sqlglot_type)
    if nullable is None:
        assert "nullable" not in dlt_type
    else:
        assert dlt_type.get("nullable") == nullable


@pytest.mark.parametrize(
    "timezone, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIMESTAMP),
        (True, sge.DataType.Type.TIMESTAMPTZ),
        (False, sge.DataType.Type.TIMESTAMPNTZ),
    ]
)
def test_to_sqlglot_timestamp_with_timezone(timezone, expected_sqlglot_type) -> None:
    sqlglot_type = to_sqlglot_type("timestamp", timezone=timezone)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_timezone",
    [
        (sge.DataType.Type.TIMESTAMP, None),  # default value
        (sge.DataType.Type.TIMESTAMPNTZ, False),
        (sge.DataType.Type.TIMESTAMPLTZ, True),
        (sge.DataType.Type.TIMESTAMPTZ, True),
        (sge.DataType.Type.TIMESTAMP_S, False),
        (sge.DataType.Type.TIMESTAMP_MS, False),
        (sge.DataType.Type.TIMESTAMP_NS, False),
    ],
)
def test_from_sqlglot_timestamp_with_timezone(sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]) -> None:
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_timezone is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_timezone


@pytest.mark.parametrize(
    "timezone, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIME),
        (True, sge.DataType.Type.TIMETZ),
        (False, sge.DataType.Type.TIME),
    ]
)
def test_to_sqlglot_time_with_timezone(timezone, expected_sqlglot_type) -> None:
    sqlglot_type = to_sqlglot_type("time", timezone=timezone)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_timezone",
    [
        (sge.DataType.Type.TIME, None),  # default value
        (sge.DataType.Type.TIMETZ, True),
    ],
)
def test_from_sqlglot_time_with_timezone(sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]) -> None:
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_timezone is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_timezone


@pytest.mark.parametrize(
    "precision, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIMESTAMP),  # default value
        (1, sge.DataType.Type.TIMESTAMP_S),
        (3, sge.DataType.Type.TIMESTAMP_MS),
        (9, sge.DataType.Type.TIMESTAMP_NS),
    ]
)
def test_to_sqlglot_timestamp_with_precision(precision: Optional[int], expected_sqlglot_type: sge.DataType.Type) -> None:
    sqlglot_type = to_sqlglot_type("timestamp", precision=precision)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_precision",
    [
        (sge.DataType.Type.TIMESTAMP, None),  # default value
        (sge.DataType.Type.TIMESTAMPNTZ, False),
        (sge.DataType.Type.TIMESTAMPLTZ, True),
        (sge.DataType.Type.TIMESTAMPTZ, True),
        (sge.DataType.Type.TIMESTAMP_S, False),
        (sge.DataType.Type.TIMESTAMP_MS, False),
        (sge.DataType.Type.TIMESTAMP_NS, False),
    ],
)
def test_from_sqlglot_timestamp_with_precision(sqlglot_type: sge.DataType.Type, expected_precision: Optional[int]) -> None:
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_precision is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_precision