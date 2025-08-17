from typing import Dict, List, Any, Optional
from datetime import timezone, datetime, timedelta  # noqa: I251
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from dlt.common.libs.pyarrow import (
    normalize_py_arrow_item,
    normalize_py_arrow_item_column,
    NameNormalizationCollision,
    py_arrow_to_table_schema_columns,
    should_normalize_arrow_schema,
)

from dlt.common.schema.utils import new_column, TColumnSchema
from dlt.common.schema.normalizers import configured_normalizers, import_normalizers
from dlt.common.destination import DestinationCapabilitiesContext


def _normalize(item: Any, columns: List[TColumnSchema]) -> Any:
    _, naming, _ = import_normalizers(configured_normalizers())
    caps = DestinationCapabilitiesContext()
    columns_schema = {c["name"]: c for c in columns}

    # first normalization
    result = normalize_py_arrow_item(item, columns_schema, naming, caps)
    columns_schema = py_arrow_to_table_schema_columns(result.schema)

    # verify that should_normalize_arrow_schema returns False after first normalization
    norm_info = should_normalize_arrow_schema(result.schema, columns_schema, naming)
    assert not norm_info[0], "should_normalize_arrow_schema should return False after normalization"

    # second normalization should return exactly the same object
    second_result = normalize_py_arrow_item(result, columns_schema, naming, caps)
    assert second_result is result, "Second normalization should return the same object"

    return result


def _row_at_index(item: Any, index: int) -> List[Any]:
    # works for both pa.Table and pa.RecordBatch
    return [item.column(i)[index].as_py() for i in range(item.num_columns)]


def _make_item(rows: List[Dict[str, Any]], use_record_batch: bool) -> Any:
    table = pa.Table.from_pylist(rows)
    if use_record_batch:
        return table.to_batches(max_chunksize=len(rows))[0]
    return table


def _with_load_id(rows: List[Dict[str, Any]], include: bool) -> List[Dict[str, Any]]:
    if not include:
        return rows
    new_rows: List[Dict[str, Any]] = []
    for r in rows:
        r2 = dict(r)
        r2["_dlt_load_id"] = "L1"
        new_rows.append(r2)
    return new_rows


def test_quick_return_if_nothing_to_do() -> None:
    table = pa.Table.from_pylist(
        [
            {"a": 1, "b": 2},
        ]
    )
    columns = [new_column("a", "bigint"), new_column("b", "bigint")]
    result = _normalize(table, columns)
    # same object returned
    assert result == table


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_quick_return_if_nothing_to_do_param(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"a": 1, "b": 2},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [new_column("a", "bigint"), new_column("b", "bigint")]
    result = _normalize(item, columns)
    # same object returned
    assert result == item


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_pyarrow_reorder_columns(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"col_new": "hello", "col1": 1, "col2": "a"},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [new_column("col2", "text"), new_column("col1", "bigint")]
    result = _normalize(item, columns)
    # new columns appear at the end
    expected_cols = ["col2", "col1", "col_new"] + (["_dlt_load_id"] if with_load_id else [])
    assert result.column_names == expected_cols
    expected_row = ["a", 1, "hello"] + (["L1"] if with_load_id else [])
    assert _row_at_index(result, 0) == expected_row


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_pyarrow_add_empty_types(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"col1": 1},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [new_column("col1", "bigint"), new_column("col2", "text")]
    result = _normalize(item, columns)
    # new columns appear at the end
    expected_cols = ["col1", "col2"] + (["_dlt_load_id"] if with_load_id else [])
    assert result.column_names == expected_cols
    expected_row = [1, None] + (["L1"] if with_load_id else [])
    assert _row_at_index(result, 0) == expected_row
    assert result.schema.field(1).type == "string"


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_field_normalization_clash(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"col^New": "hello", "col_new": 1},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    with pytest.raises(NameNormalizationCollision):
        _normalize(item, [])


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_field_normalization(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"col^New": "hello", "col2": 1},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    result = _normalize(item, [])
    expected_cols = ["col_new", "col2"] + (["_dlt_load_id"] if with_load_id else [])
    assert result.column_names == expected_cols
    expected_row = ["hello", 1] + (["L1"] if with_load_id else [])
    assert _row_at_index(result, 0) == expected_row


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_default_dlt_columns_not_added(use_record_batch: bool, with_load_id: bool) -> None:
    rows = _with_load_id(
        [
            {"col1": 1},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [
        new_column("_dlt_something", "bigint"),
        new_column("_dlt_id", "text"),
        new_column("_dlt_load_id", "text"),
        new_column("col2", "text"),
        new_column("col1", "text"),
    ]
    result = _normalize(item, columns)
    # no dlt_id column; _dlt_load_id only present when in input
    if with_load_id:
        assert result.column_names == ["_dlt_something", "_dlt_load_id", "col2", "col1"]
        assert _row_at_index(result, 0) == [None, "L1", None, 1]
    else:
        assert result.column_names == ["_dlt_something", "col2", "col1"]
        assert _row_at_index(result, 0) == [None, None, 1]


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_non_nullable_columns(use_record_batch: bool, with_load_id: bool) -> None:
    """Tests the case where arrow table is created with incomplete schema info,
    such as when converting pandas dataframe to arrow. In this case normalize
    should update not-null constraints in the arrow schema.
    """
    rows = _with_load_id(
        [
            {
                "col1": 1,
                "col2": "hello",
                # include column that will be renamed by normalize
                # to ensure nullable flag mapping is correct
                "Col 3": "world",
            },
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [
        new_column("col1", "bigint", nullable=False),
        new_column("col2", "text"),
        new_column("col_3", "text", nullable=False),
    ]
    result = _normalize(item, columns)

    expected_cols = ["col1", "col2", "col_3"] + (["_dlt_load_id"] if with_load_id else [])
    assert result.column_names == expected_cols
    # not-null columns are updated in arrow
    assert result.schema.field("col1").nullable is False
    assert result.schema.field("col_3").nullable is False
    # col2 is still nullable
    assert result.schema.field("col2").nullable is True


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_passthrough_if_adding_non_nullable_column(
    use_record_batch: bool, with_load_id: bool
) -> None:
    rows = _with_load_id(
        [
            {"col1": 1},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [
        new_column("col1", "bigint", nullable=False),
        new_column("col2", "text", nullable=False),
    ]
    _normalize(item, columns)


@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_passthrough_if_passing_null_in_non_nullable_column(
    use_record_batch: bool, with_load_id: bool
) -> None:
    rows = _with_load_id(
        [
            {"col1": None},
        ],
        with_load_id,
    )
    item = _make_item(rows, use_record_batch)
    columns = [new_column("col1", "bigint", nullable=False)]
    _normalize(item, columns)


def _ts_columns(timezone_setting: Optional[Any]) -> List[TColumnSchema]:
    col = new_column("ts_col", "timestamp")
    if timezone_setting is not None:
        col["timezone"] = timezone_setting
    return [col]


def _ts_field_and_array(
    source_tz: Optional[str], values: List[datetime]
) -> tuple[pa.Field, pa.Array]:
    field = pa.field("ts_col", pa.timestamp("us", tz=source_tz))
    column = pa.array(values, type=field.type)
    return field, column


def _ts_schema(timezone_setting: bool) -> TColumnSchema:
    schema: TColumnSchema = {"name": "ts_col", "data_type": "timestamp"}
    if timezone_setting is not None:
        schema["timezone"] = timezone_setting
    return schema


def _make_ts_item(value: datetime, use_record_batch: bool, with_load_id: bool) -> Any:
    row: Dict[str, Any] = {"ts_col": value}
    if with_load_id:
        row["_dlt_load_id"] = "L1"
    table = pa.Table.from_pylist([row])
    if use_record_batch:
        return table.to_batches(max_chunksize=1)[0]
    return table


@pytest.mark.parametrize(
    "value, timezone_setting, expected_tz, expected_value",
    [
        # explicit timezone=False - should convert aware UTC to naive
        (
            datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            False,
            None,
            datetime(2021, 1, 1, 12, 0, 0),
        ),
        # implicit timezone (default True) - should convert naive to UTC-aware
        (
            datetime(2021, 1, 1, 12, 0, 0),
            None,  # omitted in schema => defaults to True
            "UTC",
            datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ),
    ],
)
@pytest.mark.parametrize("use_record_batch", [False, True])
@pytest.mark.parametrize("with_load_id", [False, True])
def test_normalize_py_arrow_item_timestamp_type_casting_param(
    value: datetime,
    timezone_setting: Optional[Any],
    expected_tz: Any,
    expected_value: datetime,
    use_record_batch: bool,
    with_load_id: bool,
) -> None:
    item = _make_ts_item(value, use_record_batch, with_load_id)
    columns = _ts_columns(timezone_setting)

    result = _normalize(item, columns)

    assert result.schema.field("ts_col").type.tz == expected_tz
    assert _row_at_index(result, 0)[0] == expected_value


@pytest.mark.parametrize(
    "source_tz, values, timezone_setting, expected_tz, expected_values",
    [
        # naive timestamp with timezone=True (should become UTC)
        (
            None,
            [datetime(2021, 1, 1, 12, 0, 0)],
            True,
            "UTC",
            [datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        ),
        # UTC timestamp with timezone=False (should become naive)
        (
            "UTC",
            [datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
            False,
            None,
            [datetime(2021, 1, 1, 12, 0, 0)],
        ),
        # Berlin timestamp with timezone=False (should become naive in UTC)
        (
            "Europe/Berlin",
            [datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))],
            False,
            None,
            [datetime(2021, 1, 1, 10, 0, 0)],
        ),
        # default timezone in schema (omitted) should behave like True
        (
            None,
            [datetime(2021, 1, 1, 12, 0, 0)],
            None,  # omitted => default True
            "UTC",
            [datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        ),
    ],
)
def test_normalize_py_arrow_item_column_timestamp_param(
    source_tz: Optional[str],
    values: List[datetime],
    timezone_setting: Optional[Any],
    expected_tz: Any,
    expected_values: List[datetime],
) -> None:
    field, column = _ts_field_and_array(source_tz, values)
    column_schema = _ts_schema(timezone_setting)

    modified_type, modified_column = normalize_py_arrow_item_column(
        column_schema, field.type, column
    )

    assert modified_type.tz == expected_tz
    for i, expected in enumerate(expected_values):
        assert modified_column[i].as_py() == expected


@pytest.mark.parametrize(
    "source_tz, values, timezone_setting",
    [
        # no change needed - UTC with timezone=True
        ("UTC", [datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc)], True),
        # no change needed - naive with timezone=False
        (None, [datetime(2021, 1, 1, 12, 0, 0)], False),
    ],
)
def test_normalize_py_arrow_item_column_timestamp_identity(
    source_tz: Optional[str], values: List[datetime], timezone_setting: Any
) -> None:
    field, column = _ts_field_and_array(source_tz, values)
    column_schema = _ts_schema(timezone_setting)

    original_type, original_column = normalize_py_arrow_item_column(
        column_schema, field.type, column
    )

    # should return original objects unchanged
    assert original_type is field.type
    assert original_column is column


def test_normalize_py_arrow_item_column_non_timestamp() -> None:
    """Test normalize_py_arrow_item_column with non-timestamp columns."""
    # test time column - should return unchanged (time columns don't have timezone info)
    time_field = pa.field("time_col", pa.time64("us"))
    time_column = pa.array([12345678])
    column_schema: TColumnSchema = {"name": "time_col", "data_type": "time"}

    original_type, original_column = normalize_py_arrow_item_column(
        column_schema, time_field.type, time_column
    )

    assert original_type is time_field.type
    assert original_column is time_column


def test_normalize_py_arrow_item_column_timezone_conversion() -> None:
    """Test timezone conversion scenarios with actual timezone data."""
    # test Europe/Berlin to UTC conversion
    berlin_field = pa.field("ts_col", pa.timestamp("us", tz="Europe/Berlin"))
    # create a timestamp that would be affected by timezone conversion
    berlin_time = datetime(
        2021, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("Europe/Berlin")
    )  # 2:30 PM Berlin time (summer)
    berlin_column = pa.array([berlin_time], type=berlin_field.type)

    column_schema: TColumnSchema = {"name": "ts_col", "data_type": "timestamp", "timezone": True}

    modified_type, modified_column = normalize_py_arrow_item_column(
        column_schema, berlin_field.type, berlin_column
    )

    # should be converted to UTC
    assert modified_type.tz == "UTC"
    # the actual timestamp value should now be in UTC
    assert modified_column[0].as_py().tzinfo.tzname(None) == "UTC"
    assert modified_column[0].as_py() == berlin_time


def test_normalize_py_arrow_item_column_with_multiple_timestamps() -> None:
    """Test normalize_py_arrow_item_column with multiple timestamp values."""
    # test with multiple timestamp values
    naive_field = pa.field("ts_col", pa.timestamp("us", tz=None))
    timestamps = [
        datetime(2021, 1, 1, 12, 0, 0),
        datetime(2021, 6, 15, 14, 30, 0),
        datetime(2022, 12, 31, 23, 59, 59),
    ]
    naive_column = pa.array(timestamps, type=naive_field.type)
    column_schema: TColumnSchema = {"name": "ts_col", "data_type": "timestamp", "timezone": True}

    modified_type, modified_column = normalize_py_arrow_item_column(
        column_schema, naive_field.type, naive_column
    )

    # should be UTC timezone
    assert modified_type.tz == "UTC"

    # all timestamps should be converted to UTC-aware
    for i, original_ts in enumerate(timestamps):
        converted_ts = modified_column[i].as_py()
        assert converted_ts == original_ts.replace(tzinfo=timezone.utc)
        assert converted_ts.tzinfo.tzname(None) == "UTC"
