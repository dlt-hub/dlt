from typing import Optional
from datetime import datetime, timedelta, time as dt_time, date  # noqa: I251
import os

import pytest
from pytest_mock import MockerFixture

import numpy as np
import pyarrow as pa
import pandas as pd
import base64

import dlt
from dlt.common import logger
from dlt.common.libs.pyarrow import (
    cast_arrow_as_columns_schema,
    py_arrow_to_table_schema_columns,
    row_tuples_to_arrow,
)
from dlt.common.time import (
    ensure_pendulum_datetime_utc,
    reduce_pendulum_datetime_precision,
    ensure_pendulum_time,
    ensure_pendulum_date,
)
from dlt.common.utils import uniq_id

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    table_update_and_row_for_destination,
)
from tests.pipeline.utils import assert_load_info, select_data
from tests.utils import (
    TestDataItemFormat,
    arrow_item_from_pandas,
    TPythonTableFormat,
)
from tests.cases import (
    arrow_table_all_data_types,
    table_update_and_row,
)


# NOTE: this test runs on parquet + postgres needs adbc dependency group
destination_cases = destinations_configs(
    default_sql_configs=True,
    # default_staging_configs=True,
    # all_staging_configs=False,
    table_format_filesystem_configs=True,
)
# if postgres got selected, add postgres config with native parquet support via adbc
if "postgres" in [case.destination_type for case in destination_cases]:
    destination_cases.append(
        DestinationTestConfiguration(destination_type="postgres", file_format="parquet")
    )


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destination_cases,
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

    include_time = destination_config.destination_type not in (
        "athena",
        "redshift",
        "databricks",
        "synapse",
        "clickhouse",
    )  # athena/redshift can't load TIME columns
    include_binary = not (
        destination_config.destination_type in ("redshift", "databricks")
        and destination_config.file_format == "jsonl"
    )

    include_decimal = True
    if (
        destination_config.destination_type == "databricks"
        and destination_config.file_format == "jsonl"
    ) or (destination_config.destination_name == "sqlalchemy_sqlite"):
        include_decimal = False

    include_date = not (
        destination_config.destination_type == "databricks"
        and destination_config.file_format == "jsonl"
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
    destination_config.file_format = (
        "csv"
        if destination_config.destination_type == "postgres"
        and destination_config.file_format == "insert_values"
        else destination_config.file_format
    )

    load_info = pipeline.run(some_data(), **destination_config.run_kwargs)
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

    rows = [list(row) for row in select_data(pipeline, "SELECT * FROM some_data")]
    for row in rows:
        for i in range(len(row)):
            # Postgres returns memoryview for binary columns
            if isinstance(row[i], memoryview):
                row[i] = row[i].tobytes()

    if destination_config.destination_type == "redshift":
        # Redshift needs hex string
        for record in records:
            if "binary" in record:
                record["binary"] = record["binary"].hex()

    if destination_config.destination_type == "clickhouse":
        for record in records:
            # Clickhouse needs base64 string for jsonl
            if "binary" in record and destination_config.file_format == "jsonl":
                record["binary"] = base64.b64encode(record["binary"]).decode("ascii")
            if "binary" in record and destination_config.file_format == "parquet":
                record["binary"] = record["binary"].decode("ascii")

    expected = sorted([list(r.values()) for r in records])
    first_record = list(records[0].values())
    for row, expected_row in zip(rows, expected):
        for i in range(len(expected_row)):
            if isinstance(expected_row[i], datetime):
                # use UTC conversion here because timezone is not specified and Athena
                # returns naive datetimes
                row[i] = ensure_pendulum_datetime_utc(row[i])
            # clickhouse produces rounding errors on double with jsonl, so we round the result coming from there
            elif (
                destination_config.destination_type == "clickhouse"
                and destination_config.file_format == "jsonl"
                and isinstance(row[i], float)
            ):
                row[i] = round(row[i], 4)
            elif isinstance(first_record[i], dt_time):
                # Some drivers (mysqlclient) return TIME columns as timedelta as seconds since midnight
                # sqlite returns iso strings
                row[i] = ensure_pendulum_time(row[i])
            elif isinstance(expected_row[i], date):
                row[i] = ensure_pendulum_date(row[i])

    for row in expected:
        for i in range(len(row)):
            if isinstance(row[i], (datetime, dt_time)):
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


# TODO: also parametrize by native, parquet formats
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_all_types_tuples_to_arrow(
    destination_config: DestinationTestConfiguration,
) -> None:
    # run only if 'adbc-driver-postgresql' is not installed
    import importlib.util

    if importlib.util.find_spec("adbc_driver_postgresql") is not None:
        pytest.skip("runs only when 'adbc-driver-postgresql' is NOT installed")

    pipeline = destination_config.setup_pipeline("test_all_types_tuples_to_arrow", dev_mode=True)

    with pipeline._maybe_destination_capabilities() as caps:
        pass

    # force parquet
    destination_config.file_format = "parquet"

    columns_schema, row = table_update_and_row_for_destination(destination_config)
    if destination_config.destination_name == "sqlalchemy_sqlite":
        # sqlite stores timestamp as strings and this is precision 3 timestamps with
        # trailing zeros that arrow can not parse nor cast
        columns_schema.pop("col4_precision")
        row.pop("col4_precision")

    table = row_tuples_to_arrow(
        [list(row.values())] * 10,
        caps,
        columns=columns_schema,
        tz="UTC",
    )

    @dlt.resource(
        table_format=destination_config.table_format, file_format="parquet", columns=columns_schema
    )
    def arrow_items():
        yield table

    load_info = pipeline.run(arrow_items())
    assert_load_info(load_info)

    # NOTE: result table will not have exactly the same types as dlt schema in columns_schema in case
    # of native cursors that read arrow directly (ie. duckdb, bigquery), we just check a few columns
    # TODO: move that test to read interfaces and verify schema fully, also consider casting arrow
    # tables to match the schema exactly (IMO that should be optional)

    rel = pipeline.dataset().arrow_items
    rel_schema = rel.columns_schema
    assert list(rel_schema.keys()) == list(columns_schema.keys())
    result_table = rel.arrow()

    # here we cast result to the rel schema,
    result_table = cast_arrow_as_columns_schema(result_table, rel_schema, caps, "utc")

    # compare schemas
    result_columns_schema = py_arrow_to_table_schema_columns(result_table.schema)
    for name, column in result_columns_schema.items():
        orig_type = columns_schema[name]["data_type"]
        rel_type = rel_schema[name]["data_type"]
        if orig_type != rel_type:
            print(f"On {name} orig vs rel mismatch", columns_schema[name], rel_schema[name])
        if orig_type == "wei":
            # do not verify wei, it may be converted to text or decimal
            continue
        elif rel_type == "json":
            rel_type = "text"

        assert column["data_type"] == rel_type, f"Column type mismatch on {name}"
        # compare decimals
        if rel_type == "decimal":
            assert column.get("precision") == rel_schema[name].get("precision")
            assert column.get("scale") == rel_schema[name].get("scale")
        if rel_type == "timestamp":
            # we cast above so we have timezones/naive when we want them
            pass
            # verify tz awareness
            # timezone = rel_schema[name].get("timezone", True)
            # if timezone or not caps.supports_naive_datetime:
            #     assert (
            #         column.get("timezone", True) is True
            #     ), f"tz aware timestamp expected on {name}"
            # else:
            #     assert column["timezone"] is False, f"naive timestamp expected on {name}"


@pytest.mark.no_load  # Skips drop_pipeline fixture since we don't do any loading
@pytest.mark.parametrize(
    "destination_config",
    destination_cases,
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


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destination_cases,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_load_arrow_with_not_null_columns(
    item_type: TestDataItemFormat, destination_config: DestinationTestConfiguration
) -> None:
    """Resource schema contains non-nullable columns. Arrow schema should be written accordingly"""
    if (
        destination_config.destination_type in ("databricks", "redshift")
        and destination_config.file_format == "jsonl"
    ):
        pytest.skip(
            "databricks + redshift / json cannot load most of the types so we skip this test"
        )

    item, records, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=False)

    @dlt.resource(primary_key="string", columns=[{"name": "int", "nullable": False}])
    def some_data():
        yield item

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    pipeline.extract(
        some_data(),
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )

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
    # Load is successful
    info = pipeline.load()
    assert_load_info(info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, default_staging_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("is_none", [True, False])
def test_warning_from_arrow_normalizer_on_null_column(
    destination_config: DestinationTestConfiguration,
    mocker: MockerFixture,
    is_none: bool,
) -> None:
    """
    Test that the arrow normalizer emits a warning when a pyarrow table is yielded
    with a column (`col1`) that contains only null values.
    """

    @dlt.source()
    def my_source():
        @dlt.resource
        def my_resource():
            col1: list[Optional[str]] = [None, None] if is_none else ["a", "b"]

            table = pa.table(
                {
                    "id": pa.array([1, 2]),
                    "col1": pa.array(col1),
                }
            )

            yield table

        return [my_resource()]

    logger_spy = mocker.spy(logger, "warning")

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    pipeline.extract(my_source())
    pipeline.normalize()

    if is_none:
        logger_spy.assert_called_once()
        expected_warning = (
            "columns in table 'my_resource' did not receive any data during this load "
            "and therefore could not have their types inferred:\n"
            "  - col1"
        )
        assert expected_warning in logger_spy.call_args_list[0][0][0]
    else:
        logger_spy.assert_not_called()
