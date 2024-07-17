from datetime import datetime  # noqa: I251
import os

import pytest
import numpy as np
import pyarrow as pa
import pandas as pd
import base64

import dlt
from dlt.common import pendulum
from dlt.common.time import reduce_pendulum_datetime_precision
from dlt.common.utils import uniq_id

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, select_data
from tests.utils import (
    TestDataItemFormat,
    arrow_item_from_pandas,
    TPythonTableFormat,
)
from tests.cases import arrow_table_all_data_types

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_staging_configs=True, all_staging_configs=True
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_load_arrow_item(
    item_type: TestDataItemFormat,
    destination_config: DestinationTestConfiguration,
) -> None:
    # compression must be on for redshift
    # os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "True"
    include_time = destination_config.destination not in (
        "athena",
        "redshift",
        "databricks",
        "synapse",
        "clickhouse",
    )  # athena/redshift can't load TIME columns
    include_binary = not (
        destination_config.destination in ("redshift", "databricks")
        and destination_config.file_format == "jsonl"
    )

    include_decimal = not (
        destination_config.destination == "databricks" and destination_config.file_format == "jsonl"
    )
    include_date = not (
        destination_config.destination == "databricks" and destination_config.file_format == "jsonl"
    )

    item, records, _ = arrow_table_all_data_types(
        item_type,
        include_json=False,
        include_time=include_time,
        include_decimal=include_decimal,
        include_binary=include_binary,
        include_date=include_date,
    )

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    @dlt.resource
    def some_data():
        yield item

    # use csv for postgres to get native arrow processing
    file_format = (
        destination_config.file_format if destination_config.destination != "postgres" else "csv"
    )

    load_info = pipeline.run(some_data(), loader_file_format=file_format)
    assert_load_info(load_info)
    # assert the table types
    some_table_columns = pipeline.default_schema.get_table("some_data")["columns"]
    assert some_table_columns["string"]["data_type"] == "text"
    assert some_table_columns["float"]["data_type"] == "double"
    assert some_table_columns["int"]["data_type"] == "bigint"
    assert some_table_columns["datetime"]["data_type"] == "timestamp"
    assert some_table_columns["bool"]["data_type"] == "bool"
    if include_time:
        assert some_table_columns["time"]["data_type"] == "time"
    if include_binary:
        assert some_table_columns["binary"]["data_type"] == "binary"
    if include_decimal:
        assert some_table_columns["decimal"]["data_type"] == "decimal"
    if include_date:
        assert some_table_columns["date"]["data_type"] == "date"

    qual_name = pipeline.sql_client().make_qualified_table_name("some_data")
    rows = [list(row) for row in select_data(pipeline, f"SELECT * FROM {qual_name}")]

    for row in rows:
        for i in range(len(row)):
            # Postgres returns memoryview for binary columns
            if isinstance(row[i], memoryview):
                row[i] = row[i].tobytes()

    if destination_config.destination == "redshift":
        # Redshift needs hex string
        for record in records:
            if "binary" in record:
                record["binary"] = record["binary"].hex()

    if destination_config.destination == "clickhouse":
        for record in records:
            # Clickhouse needs base64 string for jsonl
            if "binary" in record and destination_config.file_format == "jsonl":
                record["binary"] = base64.b64encode(record["binary"]).decode("ascii")
            if "binary" in record and destination_config.file_format == "parquet":
                record["binary"] = record["binary"].decode("ascii")

    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] = pendulum.instance(row[i])
            # clickhouse produces rounding errors on double with jsonl, so we round the result coming from there
            if (
                destination_config.destination == "clickhouse"
                and destination_config.file_format == "jsonl"
                and isinstance(row[i], float)
            ):
                row[i] = round(row[i], 4)

    expected = sorted([list(r.values()) for r in records])

    for row in expected:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] = reduce_pendulum_datetime_precision(
                    row[i], pipeline.destination.capabilities().timestamp_precision
                )

    load_id = load_info.loads_ids[0]

    # Sort rows by all columns except _dlt_id/_dlt_load_id for deterministic comparison
    rows = sorted(rows, key=lambda row: row[:-2])
    expected = sorted(expected)

    for row, expected_row in zip(rows, expected):
        # Compare without _dlt_id/_dlt_load_id columns
        assert row[3] == expected_row[3]
        assert row[:-2] == expected_row
        # Load id and dlt_id are set
        assert row[-2] == load_id
        assert isinstance(row[-1], str)


@pytest.mark.no_load  # Skips drop_pipeline fixture since we don't do any loading
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        default_staging_configs=True,
        all_staging_configs=True,
        default_vector_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_parquet_column_names_are_normalized(
    item_type: TPythonTableFormat, destination_config: DestinationTestConfiguration
) -> None:
    """Test normalizing of parquet columns in all destinations"""
    # Create df with column names with inconsistent naming conventions
    df = pd.DataFrame(
        np.random.randint(0, 100, size=(10, 7)),
        columns=[
            "User ID",
            "fIRst-NamE",
            "last_name",
            "e-MAIL",
            " pHone Number",
            "ADDRESS",
            "CreatedAt",
        ],
    )
    tbl = arrow_item_from_pandas(df, item_type)

    @dlt.resource
    def some_data():
        yield tbl

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())
    pipeline.extract(some_data())

    # Find the extracted file
    norm_storage = pipeline._get_normalize_storage()
    extract_files = [
        fn for fn in norm_storage.list_files_to_normalize_sorted() if fn.endswith(".parquet")
    ]
    assert len(extract_files) == 1

    # Normalized column names according to schema naming convention
    expected_column_names = [
        pipeline.default_schema.naming.normalize_path(col) for col in df.columns
    ]
    new_table_name = pipeline.default_schema.naming.normalize_table_identifier("some_data")
    schema_columns = pipeline.default_schema.get_table_columns(new_table_name)

    # Schema columns are normalized
    assert [c["name"] for c in schema_columns.values()] == expected_column_names

    with norm_storage.extracted_packages.storage.open_file(extract_files[0], "rb") as f:
        result_tbl = pa.parquet.read_table(f)

        # Parquet schema is written with normalized column names
        assert result_tbl.schema.names == expected_column_names


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        default_staging_configs=True,
        all_staging_configs=True,
        default_vector_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_load_arrow_with_not_null_columns(
    item_type: TestDataItemFormat, destination_config: DestinationTestConfiguration
) -> None:
    """Resource schema contains non-nullable columns. Arrow schema should be written accordingly"""
    item, records, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=False)

    @dlt.resource(primary_key="string", columns=[{"name": "int", "nullable": False}])
    def some_data():
        yield item

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    pipeline.extract(some_data())

    norm_storage = pipeline._get_normalize_storage()
    extract_files = [
        fn for fn in norm_storage.list_files_to_normalize_sorted() if fn.endswith(".parquet")
    ]
    assert len(extract_files) == 1

    # Check the extracted parquet file. It should have the respective non-nullable column in schema
    with norm_storage.extracted_packages.storage.open_file(extract_files[0], "rb") as f:
        result_tbl = pa.parquet.read_table(f)
        assert result_tbl.schema.field("string").nullable is False
        assert result_tbl.schema.field("string").type == pa.string()
        assert result_tbl.schema.field("int").nullable is False
        assert result_tbl.schema.field("int").type == pa.int64()

    pipeline.normalize()
    # Load is succesful
    info = pipeline.load()
    assert_load_info(info)
