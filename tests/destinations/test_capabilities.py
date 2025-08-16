import pytest

from dlt.common.data_types.typing import TDataType
from dlt.common.destination.capabilities import (
    DestinationCapabilitiesContext,
    adjust_column_schema_to_capabilities,
    adjust_columns_schema_to_capabilities,
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
def test_temporal_precision_limited_to_max(data_type: TDataType) -> None:
    """Test temporal column precision is limited to max_timestamp_precision"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3

    column: TColumnSchema = {"name": "test_col", "data_type": data_type, "precision": 9}
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is not None
    assert column["precision"] == 3


@pytest.mark.parametrize("data_type", ["timestamp", "time"])
def test_temporal_default_precision_removed(data_type: TDataType) -> None:
    """Test temporal column precision removed when equals default"""
    caps = _create_default_caps()
    caps.timestamp_precision = 6

    column: TColumnSchema = {"name": "test_col", "data_type": data_type, "precision": 6}
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is not None
    assert "precision" not in column


@pytest.mark.parametrize(
    "supports_naive,drop_timezone,timezone_value,should_remove",
    [
        (False, False, True, False),  # Not supported, timezone=True -> keep
        (False, False, False, True),  # Not supported, timezone=False -> remove
        (False, False, None, False),  # Not supported, no timezone -> no change
        (True, True, True, True),  # Supported but drop=True, timezone=True -> remove
        (True, True, False, True),  # Supported but drop=True, timezone=False -> remove
        (True, True, None, False),  # Supported but drop=True, no timezone -> no change
        (True, False, True, False),  # Supported and drop=False, timezone=True -> keep
        (True, False, False, False),  # Supported and drop=False, timezone=False -> keep
        (True, False, None, False),  # Supported and drop=False, no timezone -> no change
    ],
)
def test_timestamp_timezone_handling(
    supports_naive: bool, drop_timezone: bool, timezone_value: bool, should_remove: bool
) -> None:
    """Test timezone handling based on capabilities, drop_timezone flag, and existing timezone value"""
    caps = _create_default_caps()
    caps.supports_naive_datetime = supports_naive

    column = _create_timestamp_column(timezone=timezone_value)
    result = adjust_column_schema_to_capabilities(column, caps, drop_timezone=drop_timezone)

    if should_remove:
        assert result is not None
        assert "timezone" not in column
    else:
        if timezone_value is not None:
            # If column originally had timezone, it should still be there
            assert "timezone" in column
            assert column["timezone"] == timezone_value
        else:
            # If column didn't have timezone originally, it shouldn't be added
            assert "timezone" not in column
        # Result can be None (no changes) or not None (other changes like precision)


@pytest.mark.parametrize("supports_nested", [True, False])
def test_json_nested_type_handling(supports_nested: bool) -> None:
    """Test x-nested-type handling based on supports_nested_types"""
    caps = _create_default_caps()
    caps.supports_nested_types = supports_nested

    column = _create_json_column(nested_type="some_nested_type")
    result = adjust_column_schema_to_capabilities(column, caps)

    if supports_nested:
        assert result is None  # No modification needed
        assert column["x-nested-type"] == "some_nested_type"  # type: ignore[typeddict-item]
    else:
        assert result is not None
        assert "x-nested-type" not in column


@pytest.mark.parametrize("data_type", ["text", "bigint", "double", "boolean"])
def test_non_temporal_non_json_unchanged(data_type: TDataType) -> None:
    """Test non-temporal, non-json columns are not modified"""
    caps = _create_default_caps()

    column: TColumnSchema = {"name": "test_col", "data_type": data_type}
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is None


def test_no_parquet_format_fallback() -> None:
    """Test behavior when no parquet format is configured"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3
    caps.parquet_format = None

    column = _create_timestamp_column(precision=9)
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is not None
    assert column["precision"] == 3


def test_precision_limited_by_parquet_format() -> None:
    """Test precision is limited by parquet format when destination allows higher precision"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 9  # Destination allows high precision
    caps.timestamp_precision = 6

    # Configure parquet format with lower precision limit
    parquet_config = ParquetFormatConfiguration()
    parquet_config.version = "2.4"  # Supports microsecond precision (6)
    caps.parquet_format = parquet_config

    column = _create_timestamp_column(precision=9)
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is not None
    # Should be limited to parquet format's max precision (6), not destination's (9)
    assert "precision" not in column


def test_precision_limited_by_parquet_coerce_timestamps() -> None:
    """Test precision is limited by parquet coerce_timestamps setting"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 9  # Destination allows high precision
    caps.timestamp_precision = 6

    # Configure parquet format to coerce to millisecond precision
    parquet_config = ParquetFormatConfiguration()
    parquet_config.version = "2.6"  # Would normally support nanosecond
    parquet_config.coerce_timestamps = "ms"  # Force millisecond precision (3)
    caps.parquet_format = parquet_config

    column = _create_timestamp_column(precision=9)
    result = adjust_column_schema_to_capabilities(column, caps)

    assert result is not None
    # Should be limited to coerce_timestamps precision (3)
    assert column["precision"] == 3


def test_multiple_columns_adjustment() -> None:
    """Test adjustment of multiple different column types"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3
    caps.supports_naive_datetime = False
    caps.supports_nested_types = False

    columns: TTableSchemaColumns = {
        "timestamp_col": _create_timestamp_column(precision=9, timezone=True),
        "json_col": _create_json_column(nested_type="nested"),
        "text_col": {"name": "text_col", "data_type": "text"},
    }

    result = adjust_columns_schema_to_capabilities(columns, caps)

    assert len(result) == 2  # timestamp and json modified
    assert columns["timestamp_col"]["precision"] == 3
    assert columns["timestamp_col"]["timezone"] is True
    assert "x-nested-type" not in columns["json_col"]


def test_no_columns_need_adjustment() -> None:
    """Test when no columns need adjustment"""
    caps = _create_default_caps()

    columns: TTableSchemaColumns = {
        "text_col": {"name": "text_col", "data_type": "text"},
        "int_col": {"name": "int_col", "data_type": "bigint"},
    }

    result = adjust_columns_schema_to_capabilities(columns, caps)
    assert len(result) == 0


def test_empty_columns_dict() -> None:
    """Test with empty columns dictionary"""
    caps = _create_default_caps()
    result = adjust_columns_schema_to_capabilities({}, caps)
    assert len(result) == 0


@pytest.mark.parametrize("drop_timezone", [True, False])
def test_drop_timezone_parameter(drop_timezone: bool) -> None:
    """Test drop_timezone parameter affects all timestamp columns"""
    caps = _create_default_caps()
    caps.supports_naive_datetime = True

    columns: TTableSchemaColumns = {
        "timestamp1": _create_timestamp_column(timezone=True),
        "timestamp2": _create_timestamp_column(timezone=False),
        "time_col": {"name": "time_col", "data_type": "time", "timezone": True},
    }

    result = adjust_columns_schema_to_capabilities(columns, caps, drop_timezone=drop_timezone)

    if drop_timezone:
        assert len(result) == 3  # All temporal columns modified
        for col_name in ["timestamp1", "timestamp2", "time_col"]:
            assert "timezone" not in columns[col_name]
    else:
        assert len(result) == 0  # No modifications needed


def test_return_value_contains_modified_columns() -> None:
    """Test that return value contains references to actual modified columns"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = 3

    columns: TTableSchemaColumns = {
        "timestamp_col": _create_timestamp_column(precision=9),
        "text_col": {"name": "text_col", "data_type": "text"},
    }

    result = adjust_columns_schema_to_capabilities(columns, caps)

    assert len(result) == 1
    assert result[0] is columns["timestamp_col"]
    assert result[0]["precision"] == 3


@pytest.mark.parametrize(
    "max_precision,input_precision,expected_precision,should_modify",
    [
        (6, 9, 6, True),  # Limit high precision
        (6, 3, 3, False),  # Keep low precision
        (6, 6, None, True),  # Remove default precision
    ],
)
def test_precision_adjustment_scenarios(
    max_precision: int, input_precision: int, expected_precision: int, should_modify: bool
) -> None:
    """Test various precision adjustment scenarios"""
    caps = _create_default_caps()
    caps.max_timestamp_precision = max_precision
    caps.timestamp_precision = 6

    columns: TTableSchemaColumns = {"ts_col": _create_timestamp_column(precision=input_precision)}

    result = adjust_columns_schema_to_capabilities(columns, caps)

    if should_modify:
        assert len(result) == 1
        if expected_precision is None:
            assert "precision" not in columns["ts_col"]
        else:
            assert columns["ts_col"].get("precision", 6) == expected_precision
    else:
        assert len(result) == 0
