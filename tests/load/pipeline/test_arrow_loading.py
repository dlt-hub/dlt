from datetime import datetime  # noqa: I251
from typing import Any, Union, List, Dict, Tuple, Literal
import os

import pytest
import numpy as np
import pyarrow as pa
import pandas as pd

import dlt
from dlt.common import Decimal
from dlt.common import pendulum
from dlt.common.time import reduce_pendulum_datetime_precision
from dlt.common.utils import uniq_id
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.load.pipeline.utils import assert_table, assert_query_data, select_data
from tests.utils import preserve_environ
from tests.cases import arrow_table_all_data_types, TArrowFormat


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_staging_configs=True, all_staging_configs=True
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_load_item(
    item_type: Literal["pandas", "table", "record_batch"],
    destination_config: DestinationTestConfiguration,
) -> None:
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "True"
    include_time = destination_config.destination not in (
        "athena",
        "redshift",
    )  # athena/redshift can't load TIME columns from parquet
    item, records = arrow_table_all_data_types(
        item_type, include_json=False, include_time=include_time
    )

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    @dlt.resource
    def some_data():
        yield item

    load_info = pipeline.run(some_data())
    # assert the table types
    some_table_columns = pipeline.default_schema.get_table("some_data")["columns"]
    assert some_table_columns["string"]["data_type"] == "text"
    assert some_table_columns["float"]["data_type"] == "double"
    assert some_table_columns["int"]["data_type"] == "bigint"
    assert some_table_columns["datetime"]["data_type"] == "timestamp"
    assert some_table_columns["binary"]["data_type"] == "binary"
    assert some_table_columns["decimal"]["data_type"] == "decimal"
    assert some_table_columns["bool"]["data_type"] == "bool"
    if include_time:
        assert some_table_columns["time"]["data_type"] == "time"

    qual_name = pipeline.sql_client().make_qualified_table_name("some_data")
    rows = [list(row) for row in select_data(pipeline, f"SELECT * FROM {qual_name}")]

    for row in rows:
        for i in range(len(row)):
            # Postgres returns memoryview for binary columns
            if isinstance(row[i], memoryview):
                row[i] = row[i].tobytes()

    if destination_config.destination == "redshift":
        # Binary columns are hex formatted in results
        for record in records:
            if "binary" in record:
                record["binary"] = record["binary"].hex()

    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] = pendulum.instance(row[i])

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
@pytest.mark.parametrize("item_type", ["table", "pandas", "record_batch"])
def test_parquet_column_names_are_normalized(
    item_type: TArrowFormat, destination_config: DestinationTestConfiguration
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

    if item_type == "pandas":
        tbl = df
    elif item_type == "table":
        tbl = pa.Table.from_pandas(df)
    elif item_type == "record_batch":
        tbl = pa.RecordBatch.from_pandas(df)

    @dlt.resource
    def some_data():
        yield tbl

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination=destination_config.destination)
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

    with norm_storage.storage.open_file(extract_files[0], "rb") as f:
        result_tbl = pa.parquet.read_table(f)

        # Parquet schema is written with normalized column names
        assert result_tbl.schema.names == expected_column_names
