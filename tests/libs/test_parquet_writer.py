import os
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import datetime  # noqa: 251
import time

from dlt.common import pendulum, Decimal, json
from dlt.common.configuration import inject_section
from dlt.common.data_writers.writers import ArrowToParquetWriter, ParquetDataWriter
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.utils import new_column
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.libs.pyarrow import from_arrow_scalar

from tests.common.data_writers.utils import get_writer
from tests.cases import (
    TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
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

    # this modifies original `data`
    with get_writer(ParquetDataWriter) as writer:
        writer.write_data_item([dict(data)], TABLE_UPDATE_COLUMNS_SCHEMA)

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
        # what we have is pandas Timezone which is naive
        actual = table.column(key).to_pylist()[0]
        if isinstance(value, datetime.datetime):
            actual = ensure_pendulum_datetime(actual)
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

    assert len(writer.closed_files) == 25
    with open(writer.closed_files[4].file_path, "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == list(range(16, 20))


def test_parquet_writer_config() -> None:
    os.environ["NORMALIZE__DATA_WRITER__VERSION"] = "2.0"
    os.environ["NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE"] = str(1024 * 512)
    os.environ["NORMALIZE__DATA_WRITER__TIMESTAMP_TIMEZONE"] = "America/New York"

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
            assert writer._writer.parquet_version == "2.0"
            assert writer._writer.parquet_data_page_size == 1024 * 512
            assert writer._writer.timestamp_timezone == "America/New York"

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
