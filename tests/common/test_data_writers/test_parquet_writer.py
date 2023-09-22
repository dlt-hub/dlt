import os
import pyarrow as pa
import pyarrow.parquet as pq
import datetime  # noqa: 251

from dlt.common import pendulum, Decimal
from dlt.common.configuration import inject_section
from dlt.common.data_writers.buffered import BufferedDataWriter
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext
from dlt.common.schema.utils import new_column
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.time import ensure_pendulum_date, ensure_pendulum_datetime

from tests.cases import TABLE_UPDATE_COLUMNS_SCHEMA, TABLE_ROW_ALL_DATA_TYPES
from tests.utils import TEST_STORAGE_ROOT, write_version, autouse_test_storage, preserve_environ


def get_writer(
    _format: TLoaderFileFormat = "insert_values",
    buffer_max_items: int = 10,
    file_max_items: int = 10,
    file_max_bytes: int = None,
    _caps: DestinationCapabilitiesContext = None) -> BufferedDataWriter:
    caps = _caps or DestinationCapabilitiesContext.generic_capabilities()
    caps.preferred_loader_file_format = _format
    file_template = os.path.join(TEST_STORAGE_ROOT, f"{_format}.%s")
    return BufferedDataWriter(_format, file_template, buffer_max_items=buffer_max_items, _caps=caps, file_max_items=file_max_items, file_max_bytes=file_max_bytes)


def test_parquet_writer_schema_evolution_with_big_buffer() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_writer("parquet") as writer:
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}], {"col1": c1, "col2": c2, "col3": c3, "col4": c4})

    with open(writer.closed_files[0], "rb") as f:
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

    with get_writer("parquet", buffer_max_items=4, file_max_items=50) as writer:
        for _ in range(0, 20):
            writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3})
        for _ in range(0, 20):
            writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}], {"col1": c1, "col2": c2, "col3": c3, "col4": c4})

    assert len(writer.closed_files) == 2

    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        assert len(table.schema) == 3

    with open(writer.closed_files[1], "rb") as f:
        table = pq.read_table(f)
        assert len(table.schema) == 4


def test_parquet_writer_json_serialization() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "complex")

    with get_writer("parquet") as writer:
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": {"hello":"dave"}}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": {"hello":"marcin"}}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": {}}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": []}], {"col1": c1, "col2": c2, "col3": c3})

    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == [1, 1, 1, 1]
        assert table.column("col2").to_pylist() == [2, 2, 2, 2]
        assert table.column("col3").to_pylist() == ["""{"hello":"dave"}""","""{"hello":"marcin"}""","""{}""","""[]"""]


def test_parquet_writer_all_data_fields() -> None:

    data = dict(TABLE_ROW_ALL_DATA_TYPES)
    # fix dates to use pendulum
    data["col4"] = ensure_pendulum_datetime(data["col4"])
    data["col10"] = ensure_pendulum_date(data["col10"])
    data["col11"] = pendulum.Time.fromisoformat(data["col11"])
    data["col4_precision"] = ensure_pendulum_datetime(data["col4_precision"])
    data["col11_precision"] = pendulum.Time.fromisoformat(data["col11_precision"])

    with get_writer("parquet") as writer:
        writer.write_data_item([data], TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        for key, value in data.items():
            # what we have is pandas Timezone which is naive
            actual = table.column(key).to_pylist()[0]
            if isinstance(value, datetime.datetime):
                actual = ensure_pendulum_datetime(actual)
            assert actual == value


def test_parquet_writer_items_file_rotation() -> None:
    columns = {
        "col1": new_column("col1", "bigint"),
    }

    with get_writer("parquet", file_max_items=10) as writer:
        for i in range(0, 100):
            writer.write_data_item([{"col1": i}], columns)

    assert len(writer.closed_files) == 10
    with open(writer.closed_files[4], "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == list(range(40, 50))


def test_parquet_writer_size_file_rotation() -> None:
    columns = {
        "col1": new_column("col1", "bigint"),
    }

    with get_writer("parquet", file_max_bytes=2**8, buffer_max_items=2) as writer:
        for i in range(0, 100):
            writer.write_data_item([{"col1": i}], columns)

    assert len(writer.closed_files) == 25
    with open(writer.closed_files[4], "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == list(range(16, 20))


def test_parquet_writer_config() -> None:

    os.environ["NORMALIZE__DATA_WRITER__VERSION"] = "2.0"
    os.environ["NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE"] = str(1024 * 512)
    os.environ["NORMALIZE__DATA_WRITER__TIMESTAMP_TIMEZONE"] = "America/New York"

    with inject_section(ConfigSectionContext(pipeline_name=None, sections=("normalize", ))):
        with get_writer("parquet", file_max_bytes=2**8, buffer_max_items=2) as writer:
            for i in range(0, 5):
                writer.write_data_item([{"col1": i, "col2": pendulum.now()}], {"col1": new_column("col1", "bigint"), "col2": new_column("col2", "timestamp")})
            # force the parquet writer to be created
            writer._flush_items()

            # flavor can't be tested
            assert writer._writer.parquet_version == "2.0"
            assert writer._writer.parquet_data_page_size == 1024 * 512
            assert writer._writer.timestamp_timezone == "America/New York"

            # tz can
            column_type = writer._writer.schema.field("col2").type
            assert column_type.tz == "America/New York"


def test_parquet_writer_schema_from_caps() -> None:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.decimal_precision = (18, 9)
    caps.wei_precision = (156, 78)  # will be trimmed to dec256
    caps.timestamp_precision = 9  # nanoseconds

    with get_writer("parquet", file_max_bytes=2**8, buffer_max_items=2) as writer:
        for _ in range(0, 5):
            writer.write_data_item(
                [{"col1": Decimal("2617.27"), "col2": pendulum.now(), "col3": Decimal(2**250)}],
                {"col1": new_column("col1", "decimal"), "col2": new_column("col2", "timestamp"), "col3": new_column("col3", "wei")}
            )
        # force the parquet writer to be created
        writer._flush_items()

        column_type = writer._writer.schema.field("col2").type
        assert column_type.tz == "UTC"
        column_type = writer._writer.schema.field("col1").type
        assert isinstance(column_type, pa.Decimal128Type)
        assert column_type.precision == 38
        assert column_type.scale == 9
        column_type = writer._writer.schema.field("col3").type
        assert isinstance(column_type, pa.Decimal256Type)
        # got scaled down to maximum
        assert column_type.precision == 76
        assert column_type.scale == 0
