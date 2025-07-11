from typing import Optional

import pytest
import sqlglot.expressions as sge

from dlt.common.libs.sqlglot import from_sqlglot_type, to_sqlglot_type
from dlt.common.schema.typing import TDataType, TColumnType


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
    ],
)
@pytest.mark.parametrize("use_named_type", [True, False])
def test_to_sqlglot(
    dlt_type: TDataType,
    expected_sqlglot_type: sge.DataType.Type,
    use_named_type: bool,
) -> None:
    """Test basic dlt type to SQLGlot type enum"""
    sqlglot_type = to_sqlglot_type(dlt_type, use_named_types=use_named_type)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


def _from_sqlglot_cases() -> list[tuple[sge.DataType.Type, Optional[TDataType]]]:
    """Define explicit SQLGlot enum to dlt type mapping.Other types default to `text`"""
    mapping: dict[sge.DataType.Type, Optional[TDataType]] = {
        sge.DataType.Type.OBJECT: "json",
        sge.DataType.Type.STRUCT: "json",
        sge.DataType.Type.NESTED: "json",
        sge.DataType.Type.ARRAY: "json",
        sge.DataType.Type.LIST: "json",
        sge.DataType.Type.JSON: "json",
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
        sge.DataType.Type.MONEY: "decimal",
        sge.DataType.Type.SMALLMONEY: "decimal",
        sge.DataType.Type.UDECIMAL: "decimal",
        # TEMPORAL
        sge.DataType.Type.DATE: "date",
        sge.DataType.Type.DATE32: "date",
        sge.DataType.Type.DATETIME: "date",
        sge.DataType.Type.DATETIME64: "date",
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

    try:
        mapping[sge.DataType.Type.UDOUBLE] = "decimal"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.DATETIME2] = "date"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.SMALLDATETIME] = "date"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.UNION] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.LIST] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.VECTOR] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.DECIMAL32] = "decimal"
        mapping[sge.DataType.Type.DECIMAL64] = "decimal"
        mapping[sge.DataType.Type.DECIMAL128] = "decimal"
        mapping[sge.DataType.Type.DECIMAL256] = "decimal"
    except AttributeError:
        pass

    # "text" is the default dlt data_type
    return [(sqlglot_type, mapping.get(sqlglot_type, "text")) for sqlglot_type in sge.DataType.Type]


@pytest.mark.parametrize("sqlglot_type, expected_dlt_type", _from_sqlglot_cases())
def test_from_sqlglot(sqlglot_type: sge.DataType.Type, expected_dlt_type: TDataType) -> None:
    """Test SQLGlot enum to dlt type mapping"""
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
    """Test dlt `bigint` with precision to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type(dlt_type="bigint", precision=precision, use_named_types=True)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize("nullable", [None, True, False])
@pytest.mark.parametrize("use_named_type", [True, False])
def test_to_sqlglot_with_nullable(nullable: Optional[bool], use_named_type: bool) -> None:
    """Test dlt `nullable` hint to SQLGlot data type object."""
    sqlglot_type = to_sqlglot_type("bigint", nullable=nullable, use_named_types=use_named_type)
    if nullable is None:
        assert "nullable" not in sqlglot_type.args
    else:
        assert sqlglot_type.args.get("nullable") == nullable


@pytest.mark.parametrize("nullable", [None, True, False])
def test_from_sqlglot_with_nullable(nullable: Optional[bool]) -> None:
    """Test SQLGlot data type object to dlt hints (nullable)"""
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
    ],
)
def test_to_sqlglot_timestamp_with_timezone(timezone, expected_sqlglot_type) -> None:
    """Test dlt `timestamp` with timezone to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("timestamp", timezone=timezone, use_named_types=False)
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
def test_from_sqlglot_timestamp_with_timezone(
    sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]
) -> None:
    """Test named SQLGlot type to dlt hints (timezone)"""
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
    ],
)
def test_to_sqlglot_time_with_timezone(timezone, expected_sqlglot_type) -> None:
    """Test dlt `time` with timezone to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("time", timezone=timezone, use_named_types=False)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_timezone",
    [
        (sge.DataType.Type.TIME, False),
        (sge.DataType.Type.TIMETZ, True),
    ],
)
def test_from_sqlglot_time_with_timezone(
    sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]
) -> None:
    """Test named SQLGlot type to dlt hints (timezone)"""
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_timezone is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_timezone


@pytest.mark.parametrize(
    "precision, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIMESTAMP),  # default value
        (0, sge.DataType.Type.TIMESTAMP_S),
        (3, sge.DataType.Type.TIMESTAMP_MS),
        (9, sge.DataType.Type.TIMESTAMP_NS),
    ],
)
def test_to_sqlglot_timestamp_with_precision(
    precision: Optional[int], expected_sqlglot_type: sge.DataType.Type
) -> None:
    """Test dlt `timestamp` with precision to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("timestamp", precision=precision, use_named_types=True)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_precision",
    [
        (sge.DataType.Type.TIMESTAMP, None),  # default value
        (sge.DataType.Type.TIMESTAMP_S, None),
        (sge.DataType.Type.TIMESTAMP_MS, None),
        (sge.DataType.Type.TIMESTAMP_NS, None),
    ],
)
def test_from_sqlglot_timestamp_with_precision(
    sqlglot_type: sge.DataType.Type, expected_precision: Optional[int]
) -> None:
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_precision is None:
        assert "precision" not in dlt_type
    else:
        assert dlt_type.get("precision") == expected_precision


@pytest.mark.parametrize(
    "hints, expected_sqlglot_type",
    [
        ({"data_type": "decimal", "precision": 10, "scale": 2}, sge.DataType.Type.DECIMAL),
        ({"data_type": "bigint", "precision": 17}, sge.DataType.Type.INT),
        ({"data_type": "timestamp", "precision": 0}, sge.DataType.Type.TIMESTAMP),
        (
            {"data_type": "timestamp", "precision": 5, "timezone": True},
            sge.DataType.Type.TIMESTAMPTZ,
        ),
        (
            {"data_type": "timestamp", "precision": 4, "timezone": False},
            sge.DataType.Type.TIMESTAMPNTZ,
        ),
        (
            {"data_type": "timestamp", "precision": 4, "timezone": False},
            sge.DataType.Type.TIMESTAMPNTZ,
        ),
        (
            {"data_type": "time", "precision": 4, "timezone": False},
            sge.DataType.Type.TIME,
        ),
        (
            {"data_type": "time", "precision": 5, "timezone": True},
            sge.DataType.Type.TIMETZ,
        ),
        ({"data_type": "text", "precision": 100}, sge.DataType.Type.TEXT),
        ({"data_type": "binary", "precision": 98}, sge.DataType.Type.VARBINARY),
    ],
)
def test_from_and_to_sqlglot_parameterized_types(
    hints: TColumnType,
    expected_sqlglot_type: sge.DataType.Type,
) -> None:
    """Test dlt hints to SQLGlot and SQLGlot to dlt hints
    using parameterized SQLGlot types.
    """
    dlt_type: TDataType = hints.pop("data_type")

    # create a parameterized SQLGlot DataType
    annotated_sqlglot_type = to_sqlglot_type(
        dlt_type=dlt_type,
        nullable=hints.get("nullable"),
        precision=hints.get("precision"),
        scale=hints.get("scale"),
        timezone=hints.get("timezone"),
        use_named_types=False,
    )
    assert annotated_sqlglot_type.this == expected_sqlglot_type

    # retrieve hints from DataType object; hints are not passed to `from_sqlglot_type()`
    inferred_dlt_type = from_sqlglot_type(annotated_sqlglot_type)
    assert inferred_dlt_type == {"data_type": dlt_type, **hints}
