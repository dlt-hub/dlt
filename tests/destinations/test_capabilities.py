import pytest

from dlt.common.data_types.typing import TDataType
from dlt.common.destination.capabilities import (
    DestinationCapabilitiesContext,
    adjust_column_schema_to_capabilities,
    adjust_schema_to_capabilities,
)
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns


def _create_default_caps() -> DestinationCapabilitiesContext:
    """Helper to create capabilities with common defaults"""
    caps = DestinationCapabilitiesContext()
    caps.max_timestamp_precision = 6
    caps.timestamp_precision = 6
    caps.supports_naive_datetime = True
    caps.supports_nested_types = True
    caps.parquet_format = ParquetFormatConfiguration()
    return caps


def _create_timestamp_column(precision: int = None, timezone: bool = None) -> TColumnSchema:
    """Helper to create timestamp column"""
    column: TColumnSchema = {"name": "test_col", "data_type": "timestamp"}
    if precision is not None:
        column["precision"] = precision
    if timezone is not None:
        column["timezone"] = timezone
    return column


def _create_json_column(nested_type: str = None) -> TColumnSchema:
    """Helper to create json column"""
    column: TColumnSchema = {"name": "test_col", "data_type": "json"}
    if nested_type is not None:
        column["x-nested-type"] = nested_type  # type: ignore[typeddict-unknown-key]
    return column


@pytest.mark.parametrize("data_type", ["timestamp", "time"])
def test_timestamp_precision_limited_to_max(data_type: TDataType) -> None:
    """Test timestamp column precision is limited to max_timestamp_precision"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3

    column: TColumnSchema = {"name": "test_col", "data_type": data_type, "precision": 9}
    adjust_column_schema_to_capabilities(column, caps)

    assert column["precision"] == 3


@pytest.mark.parametrize("data_type", ["timestamp", "time"])
def test_timestamp_default_precision_removed(data_type: TDataType) -> None:
    """Test timestamp column precision removed when equals default"""
    caps = _create_default_caps()
    caps.timestamp_precision = 6

    column: TColumnSchema = {"name": "test_col", "data_type": data_type, "precision": 6}
    adjust_column_schema_to_capabilities(column, caps)

    assert "precision" not in column


@pytest.mark.parametrize(
    "timezone_value", [True, False, None], ids=["tz-true", "tz-false", "tz-missing"]
)
def test_timestamp_timezone_handling(timezone_value: bool) -> None:
    """Test timezone is always removed from timestamp/time columns regardless of capabilities"""
    caps = _create_default_caps()

    column = _create_timestamp_column(timezone=timezone_value)
    adjust_column_schema_to_capabilities(column, caps)

    # timezone should always be removed (or remain absent)
    assert "timezone" not in column


@pytest.mark.parametrize(
    "supports_nested", [True, False], ids=["nested-supported", "nested-not-supported"]
)
def test_json_nested_type_handling(supports_nested: bool) -> None:
    """Test json columns remain unchanged regardless of supports_nested_types"""
    caps = _create_default_caps()
    caps.supports_nested_types = supports_nested

    column = _create_json_column(nested_type="some_nested_type")
    before = column.copy()
    adjust_column_schema_to_capabilities(column, caps)

    if supports_nested:
        assert column == before
    else:
        assert "x-nested-type" not in column


@pytest.mark.parametrize(
    "data_type",
    ["text", "bigint", "double", "boolean"],
    ids=["text", "bigint", "double", "boolean"],
)
def test_non_timestamp_non_json_unchanged(data_type: TDataType) -> None:
    """Test non-timestamp, non-json columns are not modified"""
    caps = _create_default_caps()

    column: TColumnSchema = {"name": "test_col", "data_type": data_type}
    before = column.copy()
    adjust_column_schema_to_capabilities(column, caps)

    assert column == before


def test_no_parquet_format_fallback() -> None:
    """Test behavior when no parquet format is configured"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3
    caps.parquet_format = None

    column = _create_timestamp_column(precision=9)
    adjust_column_schema_to_capabilities(column, caps)

    assert column["precision"] == 3


def test_precision_limited_by_parquet_format() -> None:
    """Test precision is limited by parquet format when destination allows higher precision"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 9  # destination allows high precision
    caps.timestamp_precision = 6

    # configure parquet format with lower precision limit
    parquet_config = ParquetFormatConfiguration()
    parquet_config.version = "2.4"  # supports microsecond precision (6)
    caps.parquet_format = parquet_config

    column = _create_timestamp_column(precision=9)
    adjust_column_schema_to_capabilities(column, caps)

    # should be limited to parquet format's max precision (6), not destination's (9)
    assert "precision" not in column


def test_precision_limited_by_parquet_coerce_timestamps() -> None:
    """Test precision is limited by parquet coerce_timestamps setting"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 9  # destination allows high precision
    caps.timestamp_precision = 6

    # configure parquet format to coerce to millisecond precision
    parquet_config = ParquetFormatConfiguration()
    parquet_config.version = "2.6"  # would normally support nanosecond
    parquet_config.coerce_timestamps = "ms"  # force millisecond precision (3)
    caps.parquet_format = parquet_config

    column = _create_timestamp_column(precision=9)
    adjust_column_schema_to_capabilities(column, caps)

    # should be limited to coerce_timestamps precision (3)
    assert column["precision"] == 3


def test_no_columns_need_adjustment() -> None:
    """Test when no columns need adjustment"""
    caps = _create_default_caps()

    columns: TTableSchemaColumns = {
        "text_col": {"name": "text_col", "data_type": "text"},
        "int_col": {"name": "int_col", "data_type": "bigint"},
    }

    result = adjust_schema_to_capabilities(columns, caps)
    assert result == columns


@pytest.mark.parametrize(
    "max_precision,input_precision,expected_precision",
    [
        (6, 9, 6),  # limit high precision
        (6, 3, 3),  # keep low precision
        (6, 6, None),  # remove default precision
    ],
    ids=["limit-to-6", "keep-3", "remove-default-6"],
)
def test_precision_adjustment_scenarios(
    max_precision: int, input_precision: int, expected_precision: int
) -> None:
    """Test various precision adjustment scenarios"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = max_precision
    caps.timestamp_precision = 6

    columns: TTableSchemaColumns = {"ts_col": _create_timestamp_column(precision=input_precision)}

    adjust_schema_to_capabilities(columns, caps)

    if expected_precision is None:
        assert "precision" not in columns["ts_col"]
    else:
        assert columns["ts_col"].get("precision", 6) == expected_precision
