from datetime import datetime

from dlt.common import pendulum
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import style_cell
from dlt._workspace.helpers.dashboard.utils.formatters import (
    filter_empty_values,
    align_dict_keys,
    humanize_datetime,
    humanize_datetime_values,
    format_duration,
    format_exception_message,
    dict_to_table_items,
)


def test_style_cell():
    """Test style cell function"""
    # Even row
    result = style_cell("0", "test_col", "test_value")
    assert result["background-color"] == "white"

    # Odd row
    result = style_cell("1", "test_col", "test_value")
    assert result["background-color"] == "#f4f4f9"

    # Name column (case insensitive)
    result = style_cell("0", "name", "test_value")
    assert result["font-weight"] == "bold"

    result = style_cell("0", "NAME", "test_value")
    assert result["font-weight"] == "bold"


def test_humanize_datetime():
    """Test humanize_datetime with various input types"""
    fmt = "YYYY-MM-DD HH:mm:ss Z"

    # datetime object
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert humanize_datetime(dt, fmt) == "2023-01-01 12:00:00 +00:00"

    # pendulum datetime
    pdt = pendulum.datetime(2023, 6, 15, 10, 30, 0, tz="UTC")
    assert humanize_datetime(pdt, fmt) == "2023-06-15 10:30:00 +00:00"

    # unix timestamp as int
    assert humanize_datetime(1672574400, fmt) == "2023-01-01 12:00:00 +00:00"

    # unix timestamp as float
    assert humanize_datetime(1672574400.0, fmt) == "2023-01-01 12:00:00 +00:00"

    # numeric string timestamp
    assert humanize_datetime("1672574400.123", fmt) == "2023-01-01 12:00:00 +00:00"

    # ISO string
    result = humanize_datetime("2023-01-01T12:00:00", fmt)
    assert "2023-01-01" in result

    # sentinel values return "-"
    assert humanize_datetime("", fmt) == "-"
    assert humanize_datetime(None, fmt) == "-"
    assert humanize_datetime("-", fmt) == "-"


def test_format_duration():
    """Test format_duration with various ranges"""
    # milliseconds
    assert format_duration(0) == "0ms"
    assert format_duration(500) == "500ms"
    assert format_duration(999) == "999ms"

    # seconds
    assert format_duration(1000) == "1.0s"
    assert format_duration(1500) == "1.5s"
    assert format_duration(59999) == "60.0s"

    # minutes
    assert format_duration(60000) == "1.0m"
    assert format_duration(120000) == "2.0m"
    assert format_duration(90000) == "1.5m"


def test_format_exception_message():
    """Test format_exception_message produces user-friendly messages"""
    # generic exception
    assert format_exception_message(ValueError("bad value")) == "bad value"

    # runtime exception preserves message
    assert format_exception_message(RuntimeError("something broke")) == "something broke"


def test_filter_empty_values():
    """Test removing None and empty string values"""
    input_dict = {
        "key1": "value1",
        "key2": None,
        "key3": "",
        "key4": "value4",
        "key5": 0,  # Should not be removed
        "key6": False,  # Should not be removed
    }

    result = filter_empty_values(input_dict)

    expected = {
        "key1": "value1",
        "key4": "value4",
        "key5": 0,
        "key6": False,
    }

    assert result == expected


def test_align_dict_keys():
    """Test aligning dictionary keys"""
    items = [
        {"key1": "value1", "key2": "value2"},
        {"key2": "value2", "key3": "value3"},
        {"key1": "value1", "key3": "value3"},
    ]

    result = align_dict_keys(items)

    # All items should have all keys
    assert len(result) == 3
    for item in result:
        assert "key1" in item
        assert "key2" in item
        assert "key3" in item

    # Check missing values are filled with "-"
    assert result[0]["key3"] == "-"
    assert result[1]["key1"] == "-"
    assert result[2]["key2"] == "-"


def test_align_dict_keys_with_none_values():
    """Test aligning dictionary keys with None values filtered out"""
    items = [
        {"key1": "value1", "key2": None, "key3": ""},
        {"key1": None, "key2": "value2"},
    ]

    result = align_dict_keys(items)

    # None and empty values should be filtered out first
    assert len(result) == 2
    assert result[0]["key1"] == "value1"
    assert result[0]["key2"] == "-"
    assert "key3" not in result[0]
    assert result[1]["key2"] == "value2"
    assert result[1]["key1"] == "-"


def test_humanize_datetime_values():
    """Test humanizing datetime values"""
    config = DashboardConfiguration()
    config.datetime_format = "YYYY-MM-DD HH:mm:ss Z"

    input_dict = {
        "started_at": pendulum.parse("2023-01-01T12:00:00"),
        "finished_at": pendulum.parse("2023-01-01T12:30:00"),
        "created": 1672574400,  # Unix timestamp
        "last_modified": "1672574400.123",  # String timestamp
        "inserted_at": datetime(2023, 1, 1, 12, 0, 0),
        "load_id": 1672574400,  # Unix timestamp for 2023-01-01T12:00:00
        "other_field": "unchanged",
        "numeric_field": 42,
    }

    result = humanize_datetime_values(config, input_dict)

    # Should have duration calculated
    assert "duration" in result

    # Original non-datetime fields should be preserved
    assert result["other_field"] == "unchanged"
    assert result["numeric_field"] == 42

    assert result["created"] == "2023-01-01 12:00:00 +00:00"
    assert result["last_modified"] == "2023-01-01 12:00:00 +00:00"
    assert result["inserted_at"] == "2023-01-01 12:00:00 +00:00"
    assert result["load_package_created_at"] == "2023-01-01 12:00:00 +00:00"


def test_dict_to_table_items():
    """Test converting dict to table items"""
    input_dict = {
        "pipeline_name": "success_pipeline",
        "destination": "duckdb",
        "status": "completed",
    }

    result = dict_to_table_items(input_dict)

    expected = [
        {"name": "pipeline_name", "value": "success_pipeline"},
        {"name": "destination", "value": "duckdb"},
        {"name": "status", "value": "completed"},
    ]

    # Sort both by name for comparison since dict order may vary
    result_sorted = sorted(result, key=lambda x: x["name"])
    expected_sorted = sorted(expected, key=lambda x: x["name"])

    assert result_sorted == expected_sorted
