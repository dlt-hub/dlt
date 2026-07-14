import os
from typing import Any, List, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import datetime  # noqa: 251
import time
import math

from dlt.common import pendulum, Decimal, json
from dlt.common.configuration import inject_section
from dlt.common.data_writers.writers import ArrowToParquetWriter, ParquetDataWriter
from dlt.common.data_writers.exceptions import IncompatibleArrowSchema
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.schema.utils import new_column
from dlt.common.schema.typing import TDataType
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext

from tests.common.data_writers.utils import get_writer
from tests.cases import (
    TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
    table_update_and_row,
)


def test_parquet_writer_schema_evolution_with_big_buffer() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_writer(ParquetDataWriter) as writer:
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3}
        )
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}],
            {"col1": c1, "col2": c2, "col3": c3, "col4": c4},
        )

    with open(writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == [1, 1]
        assert table.column("col2").to_pylist() == [2, 2]
        assert table.column("col3").to_pylist() == ["3", "3"]
        assert table.column("col4").to_pylist() == [None, "4"]


def test_parquet_writer_schema_evolution_with_small_buffer() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_writer(ParquetDataWriter, buffer_max_items=4, file_max_items=50) as writer:
        for _ in range(0, 20):
            writer.write_data_item(
                [{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3}
            )
        for _ in range(0, 20):
            writer.write_data_item(
                [{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}],
                {"col1": c1, "col2": c2, "col3": c3, "col4": c4},
            )

    assert len(writer.closed_files) == 2

    with open(writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)
        assert len(table.schema) == 3

    with open(writer.closed_files[1].file_path, "rb") as f:
        table = pq.read_table(f)
        assert len(table.schema) == 4


def test_parquet_writer_json_serialization() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "json")

    with get_writer(ParquetDataWriter) as writer:
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": {"hello": "dave"}}],
            {"col1": c1, "col2": c2, "col3": c3},
        )
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": {"hello": "marcin"}}],
            {"col1": c1, "col2": c2, "col3": c3},
        )
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": {}}], {"col1": c1, "col2": c2, "col3": c3}
        )
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": []}], {"col1": c1, "col2": c2, "col3": c3}
        )

    with open(writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == [1, 1, 1, 1]
        assert table.column("col2").to_pylist() == [2, 2, 2, 2]
        assert table.column("col3").to_pylist() == [
            """{"hello":"dave"}""",
            """{"hello":"marcin"}""",
            """{}""",
            """[]""",
        ]


def test_parquet_writer_all_data_fields() -> None:
    data = dict(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    columns_schema, _ = table_update_and_row()

    # this modifies original `data`
    with get_writer(ParquetDataWriter) as writer:
        writer.write_data_item([dict(data)], columns_schema)

    # We want to test precision for these fields is trimmed to millisecond
    data["col4_precision"] = data["col4_precision"].replace(  # type: ignore[attr-defined]
        microsecond=int(str(data["col4_precision"].microsecond)[:3] + "000")  # type: ignore[attr-defined]
    )
    data["col11_precision"] = data["col11_precision"].replace(  # type: ignore[attr-defined]
        microsecond=int(str(data["col11_precision"].microsecond)[:3] + "000")  # type: ignore[attr-defined]
    )

    with open(writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)

    for key, value in data.items():
        actual = table.column(key).to_pylist()[0]
        if isinstance(value, dict):
            actual = json.loads(actual)
        assert actual == value

    assert table.schema.field("col1_precision").type == pa.int16()
    assert table.schema.field("col4_precision").type == pa.timestamp("ms", tz="UTC")
    assert table.schema.field("col5_precision").type == pa.string()
    assert table.schema.field("col6_precision").type == pa.decimal128(6, 2)
    assert table.schema.field("col7_precision").type == pa.binary(19)
    assert table.schema.field("col11_precision").type == pa.time32("ms")


def test_parquet_writer_items_file_rotation() -> None:
    columns = {
        "col1": new_column("col1", "bigint"),
    }

    with get_writer(ParquetDataWriter, file_max_items=10) as writer:
        for i in range(0, 100):
            writer.write_data_item([{"col1": i}], columns)

    assert len(writer.closed_files) == 10
    with open(writer.closed_files[4].file_path, "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == list(range(40, 50))


def test_parquet_writer_size_file_rotation() -> None:
    columns = {
        "col1": new_column("col1", "bigint"),
    }

    with get_writer(ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
        for i in range(0, 100):
            writer.write_data_item([{"col1": i}], columns)

    # different arrow version create different file sizes
    no_files = len(writer.closed_files)
    i_per_file = int(math.ceil(100 / no_files))
    assert no_files >= 17 and no_files <= 25

    with open(writer.closed_files[4].file_path, "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == list(range(4 * i_per_file, 5 * i_per_file))


def test_parquet_writer_default_compression() -> None:
    with inject_section(ConfigSectionContext(pipeline_name=None, sections=("normalize",))):
        with get_writer(ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
            writer.write_data_item([{"col1": 1}], {"col1": new_column("col1", "bigint")})
            writer._flush_items()

            assert writer._writer.parquet_format.compression == "snappy"

        with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
            assert reader.metadata.row_group(0).column(0).compression == "SNAPPY"


def test_parquet_writer_config() -> None:
    os.environ["NORMALIZE__DATA_WRITER__VERSION"] = "1.0"
    os.environ["NORMALIZE__DATA_WRITER__COMPRESSION"] = "gzip"
    os.environ["NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE"] = str(1024 * 512)
    os.environ["NORMALIZE__DATA_WRITER__TIMESTAMP_TIMEZONE"] = "America/New York"
    os.environ["NORMALIZE__DATA_WRITER__WRITE_PAGE_INDEX"] = "true"

    with inject_section(ConfigSectionContext(pipeline_name=None, sections=("normalize",))):
        with get_writer(ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
            for i in range(0, 5):
                writer.write_data_item(
                    [{"col1": i, "col2": pendulum.now()}],
                    {"col1": new_column("col1", "bigint"), "col2": new_column("col2", "timestamp")},
                )
            # force the parquet writer to be created
            writer._flush_items()

            # flavor can't be tested
            assert writer._writer.parquet_format.version == "1.0"
            assert writer._writer.parquet_format.compression == "gzip"
            assert writer._writer.parquet_format.data_page_size == 1024 * 512
            assert writer._writer.parquet_format.timestamp_timezone == "America/New York"
            assert writer._writer.parquet_format.write_page_index is True

            # tz can
            column_type = writer._writer.schema.field("col2").type
            assert column_type.tz == "America/New York"
        # read parquet back and check
        with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
            # parquet schema is utc adjusted
            col2_info = json.loads(reader.metadata.schema.column(1).logical_type.to_json())
            assert col2_info["isAdjustedToUTC"] is True
            assert col2_info["timeUnit"] == "microseconds"
            assert reader.schema_arrow.field(1).type.tz == "America/New York"
            # page index is written (https://github.com/apache/parquet-format/blob/master/PageIndex.md)
            assert reader.metadata.row_group(0).column(0).has_column_index is True
            assert reader.metadata.row_group(0).column(0).has_offset_index is True
            assert reader.metadata.row_group(0).column(0).compression == "GZIP"


def test_parquet_writer_config_spark() -> None:
    os.environ["NORMALIZE__DATA_WRITER__FLAVOR"] = "spark"
    os.environ["NORMALIZE__DATA_WRITER__TIMESTAMP_TIMEZONE"] = "Europe/Berlin"

    now = pendulum.now(tz="Europe/Berlin")
    with inject_section(ConfigSectionContext(pipeline_name=None, sections=("normalize",))):
        with get_writer(ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
            for i in range(0, 5):
                writer.write_data_item(
                    [{"col1": i, "col2": now}],
                    {"col1": new_column("col1", "bigint"), "col2": new_column("col2", "timestamp")},
                )
            # force the parquet writer to be created
            writer._flush_items()
        with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
            # no logical type for timestamp
            col2_info = json.loads(reader.metadata.schema.column(1).logical_type.to_json())
            assert col2_info == {"Type": "None"}
            table = reader.read()
            # when compared as naive UTC adjusted timestamps it works
            assert table.column(1)[0].as_py() == now.in_timezone(tz="UTC").replace(tzinfo=None)


def test_parquet_writer_schema_from_caps() -> None:
    # store nanoseconds
    os.environ["DATA_WRITER__VERSION"] = "2.6"
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.decimal_precision = (18, 9)
    caps.wei_precision = (156, 78)  # will be trimmed to dec256
    caps.timestamp_precision = 9  # nanoseconds

    with get_writer(
        ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2, caps=caps
    ) as writer:
        for _ in range(0, 5):
            writer.write_data_item(
                [{"col1": Decimal("2617.27"), "col2": pendulum.now(), "col3": Decimal(2**250)}],
                {
                    "col1": new_column("col1", "decimal"),
                    "col2": new_column("col2", "timestamp"),
                    "col3": new_column("col3", "wei"),
                },
            )
        # force the parquet writer to be created
        writer._flush_items()

        column_type = writer._writer.schema.field("col2").type
        assert column_type == pa.timestamp("ns", tz="UTC")
        assert column_type.tz == "UTC"
        column_type = writer._writer.schema.field("col1").type
        assert isinstance(column_type, pa.Decimal128Type)
        assert column_type.precision == 18
        assert column_type.scale == 9
        column_type = writer._writer.schema.field("col3").type
        assert isinstance(column_type, pa.Decimal256Type)
        # got scaled down to maximum
        assert column_type.precision == 76
        assert column_type.scale == 0

    with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
        col2_info = json.loads(reader.metadata.schema.column(1).logical_type.to_json())
        assert col2_info["isAdjustedToUTC"] is True
        assert col2_info["timeUnit"] == "nanoseconds"


@pytest.mark.parametrize("tz", ["UTC", "Europe/Berlin", ""])
def test_parquet_writer_timestamp_precision(tz: str) -> None:
    now = pendulum.now()
    now_ns = time.time_ns()

    # store nanoseconds
    os.environ["DATA_WRITER__VERSION"] = "2.6"
    os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = tz

    adjusted = tz != ""

    with get_writer(ParquetDataWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
        for _ in range(0, 5):
            writer.write_data_item(
                [{"col1": now, "col2": now, "col3": now, "col4": now_ns}],
                TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
            )
        # force the parquet writer to be created
        writer._flush_items()

        def _assert_arrow_field(field: int, prec: str) -> None:
            column_type = writer._writer.schema.field(field).type
            assert column_type == pa.timestamp(prec, tz=tz)
            if adjusted:
                assert column_type.tz == tz
            else:
                assert column_type.tz is None

        _assert_arrow_field(0, "s")
        _assert_arrow_field(1, "ms")
        _assert_arrow_field(2, "us")
        _assert_arrow_field(3, "ns")

    with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
        print(reader.metadata.schema)

        def _assert_pq_column(col: int, prec: str) -> None:
            info = json.loads(reader.metadata.schema.column(col).logical_type.to_json())
            print(info)
            assert info["isAdjustedToUTC"] is adjusted
            assert info["timeUnit"] == prec

        # apparently storting seconds is not supported
        _assert_pq_column(0, "milliseconds")
        _assert_pq_column(1, "milliseconds")
        _assert_pq_column(2, "microseconds")
        _assert_pq_column(3, "nanoseconds")


def test_arrow_parquet_row_group_size() -> None:
    import pyarrow as pa

    c1 = {"col1": new_column("col1", "bigint")}

    id_ = -1

    def get_id_() -> int:
        nonlocal id_
        id_ += 1
        return id_

    single_elem_table = lambda: pa.Table.from_pylist([{"col1": get_id_()}])
    single_elem_batch = lambda: pa.RecordBatch.from_pylist([{"col1": get_id_()}])

    with get_writer(ArrowToParquetWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
        writer.write_data_item(single_elem_table(), columns=c1)
        writer._flush_items()
        assert writer._writer.items_count == 1

    with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
        assert reader.num_row_groups == 1
        assert reader.metadata.row_group(0).num_rows == 1

    # should be packages into single group
    with get_writer(ArrowToParquetWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
        writer.write_data_item(
            [
                single_elem_table(),
                single_elem_batch(),
                single_elem_batch(),
                single_elem_table(),
                single_elem_batch(),
            ],
            columns=c1,
        )
        writer._flush_items()
        assert writer._writer.items_count == 5

    with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
        assert reader.num_row_groups == 1
        assert reader.metadata.row_group(0).num_rows == 5

    with open(writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)
        # all ids are there and in order
        assert table["col1"].to_pylist() == list(range(1, 6))

    # pass also empty and make it to be written with a separate call to parquet writer (by buffer_max_items)
    with get_writer(ArrowToParquetWriter, file_max_bytes=2**8, buffer_max_items=1) as writer:
        pq_batch = single_elem_batch()
        writer.write_data_item(pq_batch, columns=c1)
        # writer._flush_items()
        # assert writer._writer.items_count == 5
        # this will also create arrow schema
        print(pq_batch.schema)
        writer.write_data_item(pa.RecordBatch.from_pylist([], schema=pq_batch.schema), columns=c1)

    with pa.parquet.ParquetFile(writer.closed_files[0].file_path) as reader:
        assert reader.num_row_groups == 2
        assert reader.metadata.row_group(0).num_rows == 1
        # row group with size 0 for an empty item
        assert reader.metadata.row_group(1).num_rows == 0


def _caps_with_promote(promote_options: str) -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.parquet_format = ParquetFormatConfiguration(arrow_concat_promote_options=promote_options)  # type: ignore[arg-type]
    return caps


@pytest.mark.parametrize(
    "first,second,flush_between",
    [
        # within one buffer: a concrete type difference (int64 vs float64)
        (([1, 2], pa.int64(), True), ([3, 4], pa.float64(), True), False),
        # within one buffer: a nullability difference only
        (([1, 2], pa.int64(), False), ([3, 4], pa.int64(), True), False),
        # across an already-locked file: float widening is still rejected
        (([1], pa.float64(), True), ([2], pa.float32(), True), True),
        # across an already-locked file: a decimal precision difference is rejected
        (([1], pa.decimal128(38, 9), True), ([2], pa.decimal128(10, 2), True), True),
    ],
    ids=[
        "within-batch-type",
        "within-batch-nullability",
        "cross-file-float",
        "cross-file-decimal",
    ],
)
def test_none_rejects_any_schema_difference(
    first: Tuple[List[Any], "pa.DataType", bool],
    second: Tuple[List[Any], "pa.DataType", bool],
    flush_between: bool,
) -> None:
    """`none` requires identical schemas: any type or nullability difference - within a single
    buffer or across an already-locked file - fails with an actionable dlt error and no rotation.
    """
    c1 = {"val": new_column("val", "double")}

    def _table(values: List[Any], arrow_type: "pa.DataType", nullable: bool) -> "pa.Table":
        return pa.table(
            [_cross_batch_array(values, arrow_type)],
            schema=pa.schema([pa.field("val", arrow_type, nullable=nullable)]),
        )

    with pytest.raises(IncompatibleArrowSchema) as exc_info:
        with get_writer(
            ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote("none")
        ) as writer:
            writer.write_data_item(_table(*first), columns=c1)
            if flush_between:
                writer._flush_items()
            writer.write_data_item(_table(*second), columns=c1)

    assert "arrow_concat_promote_options" in str(exc_info.value)


@pytest.mark.parametrize(
    "promote,items,expected",
    [
        # default: a cross-family mismatch splits into one file per type
        (
            "default",
            [([1, 2], pa.int64()), ([3, 4], pa.float64())],
            [(pa.int64(), 2), (pa.float64(), 2)],
        ),
        # permissive: genuinely incompatible types still split rather than fail
        (
            "permissive",
            [([1, 2], pa.int64()), (["a", "b"], pa.string())],
            [(pa.int64(), 2), (pa.string(), 2)],
        ),
        # compatible items are grouped into the same file; only the odd one rotates
        (
            "default",
            [([1], pa.int64()), ([2], pa.int64()), ([3], pa.float64())],
            [(pa.int64(), 2), (pa.float64(), 1)],
        ),
        # permissive casts a later (wider) type into the first/locked schema, accepting loss
        (
            "permissive",
            [([1], pa.int64()), ([2.0], pa.float64())],
            [(pa.int64(), 2)],
        ),
        # less precise decimal coerces into the first (more precise) schema
        (
            "permissive",
            [([Decimal("1.1")], pa.decimal128(10, 1)), ([Decimal("1.2")], pa.decimal128(10, 2))],
            [(pa.decimal128(10, 1), 2)],
        ),
    ],
    ids=[
        "default-int-float",
        "permissive-int-string",
        "default-largest-run",
        "permissive-cast-into-first",
        "permissive-less-precise-decimal",
    ],
)
def test_within_batch_split(promote: str, items: Any, expected: Any) -> None:
    """default splits a buffer into one file per type; permissive casts everything into the first
    schema (rotating only when no unified schema exists)."""
    c1 = {"col1": new_column("col1", "bigint")}
    with get_writer(
        ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote(promote)
    ) as writer:
        for values, arrow_type in items:
            table = pa.Table.from_pydict({"col1": pa.array(values, type=arrow_type)})
            writer.write_data_item(table, columns=c1)

    assert len(writer.closed_files) == len(expected)
    for closed, (exp_type, exp_rows) in zip(writer.closed_files, expected):
        result = pq.read_table(closed.file_path)
        assert result.schema.field("col1").type == exp_type
        assert result.num_rows == exp_rows


def test_within_batch_split_over_locked_file() -> None:
    """A buffer whose first item also does not fit the already-locked file rotates per item,
    preserving every row and a valid `last_modified` on each produced file."""
    c1 = {"col1": new_column("col1", "bigint")}
    with get_writer(
        ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote("default")
    ) as writer:
        # lock the first file to int64
        writer.write_data_item(
            pa.Table.from_pydict({"col1": pa.array([1], pa.int64())}), columns=c1
        )
        writer._flush_items()
        # one buffer whose two items are incompatible with each other AND with the locked file
        writer.write_data_item(
            pa.Table.from_pydict({"col1": pa.array([2.0], pa.float64())}), columns=c1
        )
        writer.write_data_item(
            pa.Table.from_pydict({"col1": pa.array(["x"], pa.string())}), columns=c1
        )

    assert len(writer.closed_files) == 3
    types = [pq.read_table(f.file_path).schema.field("col1").type for f in writer.closed_files]
    assert types == [pa.int64(), pa.float64(), pa.string()]
    # regression guard: every produced file must have a non-None last_modified (metrics summing)
    assert all(f.last_modified is not None for f in writer.closed_files)
    assert sum(f.items_count for f in writer.closed_files) == 3


def test_within_batch_record_batch_groups_preserve_order_over_locked_file() -> None:
    """default: yield 1 locks a file; yield 2's three record batches split into two runs (the
    string batch is incompatible with the two double batches), and the double run also widens the
    locked int64 file - forcing two rotations. All records are preserved in original order."""
    c1 = {"v": new_column("v", "bigint")}
    with get_writer(
        ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote("default")
    ) as writer:
        # yield 1 -> file 0 locked to int64
        writer.write_data_item(pa.record_batch({"v": pa.array([1, 2], pa.int64())}), columns=c1)
        writer._flush_items()
        # yield 2: three record batches in one buffer. batch 3 (string) is incompatible with
        # batches 1 & 2 (double) -> two runs [double(3, 4, 5), string("a")]; the double run also
        # widens the locked int64 file, so writing it rotates first
        writer.write_data_item(
            [
                pa.record_batch({"v": pa.array([3.0, 4.0], pa.float64())}),
                pa.record_batch({"v": pa.array([5.0], pa.float64())}),
                pa.record_batch({"v": pa.array(["a"], pa.string())}),
            ],
            columns=c1,
        )

    files = [pq.read_table(f.file_path) for f in writer.closed_files]
    assert [f.schema.field("v").type for f in files] == [pa.int64(), pa.float64(), pa.string()]
    # each file holds exactly its run, and records keep their original order across files
    assert files[0].column("v").to_pylist() == [1, 2]
    assert files[1].column("v").to_pylist() == [3.0, 4.0, 5.0]
    assert files[2].column("v").to_pylist() == ["a"]


def test_empty_tables_get_flushed() -> None:
    c1 = {"col1": new_column("col1", "bigint")}
    single_elem_table = pa.Table.from_pylist([{"col1": 1}])
    empty_batch = pa.RecordBatch.from_pylist([], schema=single_elem_table.schema)

    with get_writer(ArrowToParquetWriter, file_max_bytes=2**8, buffer_max_items=2) as writer:
        writer.write_data_item(empty_batch, columns=c1)
        writer.write_data_item(empty_batch, columns=c1)
        # written
        assert len(writer._buffered_items) == 0
        writer.write_data_item(empty_batch, columns=c1)
        assert len(writer._buffered_items) == 1
        writer.write_data_item(single_elem_table, columns=c1)
        assert len(writer._buffered_items) == 0


def test_concat_batches_reconciles_nullability() -> None:
    """default must reconcile nullability across consecutive record batches - they must not be
    grouped via `Table.from_batches`, which rejects differing nullability."""
    from dlt.common.libs.pyarrow import concat_batches_and_tables_in_order

    s_notnull = pa.schema([pa.field("v", pa.int64(), nullable=False)])
    s_nullable = pa.schema([pa.field("v", pa.int64(), nullable=True)])
    rb_notnull = pa.record_batch([pa.array([1, 2], pa.int64())], schema=s_notnull)
    rb_nullable = pa.record_batch([pa.array([3, 4], pa.int64())], schema=s_nullable)

    out = concat_batches_and_tables_in_order([rb_notnull, rb_nullable], promote_options="default")
    assert out.column("v").to_pylist() == [1, 2, 3, 4]


def _cross_batch_array(values: List[Any], arrow_type: "pa.DataType") -> "pa.Array":
    if pa.types.is_decimal(arrow_type):
        values = [Decimal(str(v)) for v in values]
    return pa.array(values, type=arrow_type)


@pytest.mark.parametrize(
    "promote,writer_type,incoming_type,expected_types",
    [
        # permissive always casts the incoming batch into the first (locked) schema -> one file,
        # whether the cast widens (lossless) or narrows (lossy)
        ("permissive", pa.float64(), pa.float32(), [pa.float64()]),
        ("permissive", pa.int32(), pa.int8(), [pa.int32()]),
        ("permissive", pa.decimal128(38, 9), pa.decimal128(10, 2), [pa.decimal128(38, 9)]),
        ("permissive", pa.float32(), pa.float64(), [pa.float32()]),
        ("permissive", pa.int8(), pa.int64(), [pa.int8()]),
        ("permissive", pa.decimal128(10, 2), pa.decimal128(38, 9), [pa.decimal128(10, 2)]),
        ("permissive", pa.decimal128(38, 2), pa.decimal128(38, 9), [pa.decimal128(38, 2)]),
        # cross-family: an int batch is cast into a float file (default would reject)
        ("permissive", pa.float64(), pa.int64(), [pa.float64()]),
        # default rejects any concrete type difference -> always rotates to a new file
        ("default", pa.float32(), pa.float64(), [pa.float32(), pa.float64()]),
        (
            "default",
            pa.decimal128(10, 2),
            pa.decimal128(20, 2),
            [pa.decimal128(10, 2), pa.decimal128(20, 2)],
        ),
        (
            "default",
            pa.decimal128(38, 9),
            pa.decimal128(10, 2),
            [pa.decimal128(38, 9), pa.decimal128(10, 2)],
        ),
    ],
    ids=[
        "permissive-float-up",
        "permissive-int-up",
        "permissive-decimal-up",
        "permissive-float-down",
        "permissive-int-down",
        "permissive-decimal-down",
        "permissive-decimal-overflow-down",
        "permissive-int-into-float",
        "default-float-rotate",
        "default-decimal-rotate",
        "default-decimal-narrower-rotate",
    ],
)
def test_cross_batch_promotion(
    promote: str,
    writer_type: "pa.DataType",
    incoming_type: "pa.DataType",
    expected_types: List[Any],
) -> None:
    """permissive casts every later batch into the first/locked schema (one file). default rotates
    to a new file on any type difference. First schema always wins."""
    col_type: TDataType = "decimal" if pa.types.is_decimal(writer_type) else "double"
    c1 = {"val": new_column("val", col_type)}

    with get_writer(
        ArrowToParquetWriter,
        buffer_max_items=1,
        caps=_caps_with_promote(promote),
    ) as writer:
        t1 = pa.Table.from_pydict({"val": _cross_batch_array([1], writer_type)})
        writer.write_data_item(t1, columns=c1)
        writer._flush_items()

        t2 = pa.Table.from_pydict({"val": _cross_batch_array([2], incoming_type)})
        writer.write_data_item(t2, columns=c1)

    assert len(writer.closed_files) == len(expected_types)
    for closed, expected in zip(writer.closed_files, expected_types):
        assert pq.read_table(closed.file_path).schema.field("val").type == expected
    # all rows are preserved across the produced files, no nulls introduced
    rows = [
        v for f in writer.closed_files for v in pq.read_table(f.file_path).column("val").to_pylist()
    ]
    assert len(rows) == 2 and None not in rows


def test_cross_batch_schema_mixed_columns_cast_into_first() -> None:
    """permissive casts every column of a later batch into the first/locked schema - one file."""
    c1 = {
        "a": new_column("a", "double"),
        "b": new_column("b", "bigint"),
    }

    with get_writer(
        ArrowToParquetWriter,
        buffer_max_items=1,
        caps=_caps_with_promote("permissive"),
    ) as writer:
        t1 = pa.Table.from_pydict(
            {
                "a": pa.array([1.5], type=pa.float64()),
                "b": pa.array([1], type=pa.int8()),
            }
        )
        writer.write_data_item(t1, columns=c1)
        writer._flush_items()

        # a narrows (float32 -> float64, lossless) and b narrows (int16 -> int8, value fits);
        # both are cast into the first schema, so it stays one file with the first schema
        t2 = pa.Table.from_pydict(
            {
                "a": pa.array([2.5], type=pa.float32()),
                "b": pa.array([100], type=pa.int16()),
            }
        )
        writer.write_data_item(t2, columns=c1)

    assert len(writer.closed_files) == 1
    result = pq.read_table(writer.closed_files[0].file_path)
    assert result.schema.field("a").type == pa.float64()
    assert result.schema.field("b").type == pa.int8()
    assert result.column("a").to_pylist() == [1.5, 2.5]
    assert result.column("b").to_pylist() == [1, 100]


def test_cross_batch_schema_metadata_only_diff_no_rotation() -> None:
    c1 = {"val": new_column("val", "double")}

    with get_writer(
        ArrowToParquetWriter,
        buffer_max_items=1,
        caps=_caps_with_promote("permissive"),
    ) as writer:
        schema1 = pa.schema([pa.field("val", pa.float64(), metadata={b"source": b"file1"})])
        t1 = pa.Table.from_pydict({"val": pa.array([1.5], type=pa.float64())}, schema=schema1)
        writer.write_data_item(t1, columns=c1)
        writer._flush_items()

        schema2 = pa.schema([pa.field("val", pa.float64(), metadata={b"source": b"file2"})])
        t2 = pa.Table.from_pydict({"val": pa.array([2.5], type=pa.float64())}, schema=schema2)
        writer.write_data_item(t2, columns=c1)

    assert len(writer.closed_files) == 1
    table = pq.read_table(writer.closed_files[0].file_path)
    assert table.column("val").to_pylist() == [1.5, 2.5]


@pytest.mark.parametrize("promote_options", ["none", "default", "permissive"])
def test_first_schema_metadata_preserved(promote_options: str) -> None:
    """In every mode the first batch's schema metadata wins on the written file."""
    c1 = {"val": new_column("val", "double")}
    s1 = pa.schema([pa.field("val", pa.float64())], metadata={b"src": b"first"})
    s2 = pa.schema([pa.field("val", pa.float64())], metadata={b"src": b"second"})

    with get_writer(
        ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote(promote_options)
    ) as writer:
        writer.write_data_item(pa.table([pa.array([1.5], pa.float64())], schema=s1), columns=c1)
        writer._flush_items()
        writer.write_data_item(pa.table([pa.array([2.5], pa.float64())], schema=s2), columns=c1)

    assert len(writer.closed_files) == 1
    assert pq.read_table(writer.closed_files[0].file_path).schema.metadata == {b"src": b"first"}


def test_default_reconciles_nullability_into_one_file() -> None:
    """default merges batches that differ only in nullability into a single file (symmetric)."""
    c1 = {"val": new_column("val", "bigint")}
    s_notnull = pa.schema([pa.field("val", pa.int64(), nullable=False)])
    s_nullable = pa.schema([pa.field("val", pa.int64(), nullable=True)])

    with get_writer(
        ArrowToParquetWriter, buffer_max_items=10, caps=_caps_with_promote("default")
    ) as writer:
        writer.write_data_item(
            pa.table([pa.array([1, 2], pa.int64())], schema=s_notnull), columns=c1
        )
        writer.write_data_item(
            pa.table([pa.array([3, 4], pa.int64())], schema=s_nullable), columns=c1
        )

    assert len(writer.closed_files) == 1
    assert pq.read_table(writer.closed_files[0].file_path).column("val").to_pylist() == [1, 2, 3, 4]
