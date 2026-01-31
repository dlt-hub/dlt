import decimal
from copy import deepcopy
import os
import pytest
from typing import cast
from pytest_mock import MockerFixture

import dlt
from dlt.common import pendulum
from dlt.common.destination import TLoaderFileFormat
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.load.exceptions import LoadClientJobFailed
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.pipeline.utils import simple_nested_pipeline
from tests.load.snowflake.test_snowflake_client import QUERY_TAG

from tests.pipeline.utils import assert_load_info, assert_query_column
from tests.load.utils import (
    assert_all_data_types_row,
    destinations_configs,
    DestinationTestConfiguration,
)
from tests.cases import TABLE_ROW_ALL_DATA_TYPES_DATETIMES, table_update_and_row


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_case_sensitive_identifiers(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient

    snow_ = dlt.destinations.snowflake(naming_convention="sql_cs_v1")
    # we make sure that session was not tagged (lack of query tag in config)
    tag_query_spy = mocker.spy(SnowflakeSqlClient, "_tag_session")

    dataset_name = "CaseSensitive_Dataset_" + uniq_id()
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_case_sensitive_identifiers", dataset_name=dataset_name, destination=snow_
    )
    caps = pipeline.destination.capabilities()
    assert caps.naming_convention == "sql_cs_v1"

    destination_client = pipeline.destination_client()
    # assert snowflake caps to be in case sensitive mode
    assert destination_client.capabilities.casefold_identifier is str

    # load some case sensitive data
    info = pipeline.run(
        [{"Id": 1, "Capital": 0.0}], table_name="Expenses", **destination_config.run_kwargs
    )
    assert_load_info(info)
    tag_query_spy.assert_not_called()
    with pipeline.sql_client() as client:
        assert client.has_dataset()
        # use the same case sensitive dataset
        with client.with_alternative_dataset_name(dataset_name):
            assert client.has_dataset()
        # make it case insensitive (upper)
        with client.with_alternative_dataset_name(dataset_name.upper()):
            assert not client.has_dataset()
        # keep case sensitive but make lowercase
        with client.with_alternative_dataset_name(dataset_name.lower()):
            assert not client.has_dataset()

        # must use quoted identifiers
        rows = client.execute_sql('SELECT "Id", "Capital" FROM "Expenses"')
        print(rows)
        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql('SELECT "Id", "Capital" FROM Expenses')


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_query_tagging(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
):
    from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient

    os.environ["DESTINATION__SNOWFLAKE__QUERY_TAG"] = QUERY_TAG
    tag_query_spy = mocker.spy(SnowflakeSqlClient, "_tag_session")
    pipeline = destination_config.setup_pipeline("test_snowflake_case_sensitive_identifiers")
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert_load_info(info)
    assert tag_query_spy.call_count == 2


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_custom_stage(destination_config: DestinationTestConfiguration) -> None:
    """Using custom stage name instead of the table stage"""
    os.environ["DESTINATION__SNOWFLAKE__STAGE_NAME"] = "my_non_existing_stage"
    pipeline, data = simple_nested_pipeline(destination_config, f"custom_stage_{uniq_id()}", False)
    with pytest.raises(PipelineStepFailed) as f_jobs:
        pipeline.run(data(), **destination_config.run_kwargs)
    assert isinstance(f_jobs.value.__cause__, LoadClientJobFailed)
    assert "MY_NON_EXISTING_STAGE" in f_jobs.value.__cause__.failed_message

    # NOTE: this stage must be created in DLT_DATA database for this test to pass!
    # CREATE STAGE MY_CUSTOM_LOCAL_STAGE;
    # GRANT READ, WRITE ON STAGE DLT_DATA.PUBLIC.MY_CUSTOM_LOCAL_STAGE TO ROLE DLT_LOADER_ROLE;
    stage_name = "PUBLIC.MY_CUSTOM_LOCAL_STAGE"
    os.environ["DESTINATION__SNOWFLAKE__STAGE_NAME"] = stage_name
    pipeline, data = simple_nested_pipeline(destination_config, f"custom_stage_{uniq_id()}", False)
    info = pipeline.run(data(), **destination_config.run_kwargs)
    assert_load_info(info)

    load_id = info.loads_ids[0]

    # Get a list of the staged files and verify correct number of files in the "load_id" dir
    with pipeline.sql_client() as client:
        staged_files = client.execute_sql(f'LIST @{stage_name}/"{load_id}"')
        assert len(staged_files) == 3
        # check data of one table to ensure copy was done successfully
        tbl_name = client.make_qualified_table_name("lists")
        assert_query_column(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_delete_file_after_copy(destination_config: DestinationTestConfiguration) -> None:
    """Using keep_staged_files = false option to remove staged files after copy"""
    os.environ["DESTINATION__SNOWFLAKE__KEEP_STAGED_FILES"] = "FALSE"

    pipeline, data = simple_nested_pipeline(
        destination_config, f"delete_staged_files_{uniq_id()}", False
    )

    info = pipeline.run(data(), **destination_config.run_kwargs)
    assert_load_info(info)

    load_id = info.loads_ids[0]

    with pipeline.sql_client() as client:
        # no files are left in table stage
        stage_name = client.make_qualified_table_name("%lists")
        staged_files = client.execute_sql(f'LIST @{stage_name}/"{load_id}"')
        assert len(staged_files) == 0

        # ensure copy was done
        tbl_name = client.make_qualified_table_name("lists")
        assert_query_column(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as SqlCsV1NamingConvention


class ScandinavianNamingConvention(SqlCsV1NamingConvention):
    """A variant of sql_cs_v1 which replaces Scandinavian characters."""

    def normalize_identifier(self, identifier: str) -> str:
        replace_map = {"æ": "ae", "ø": "oe", "å": "aa", "ö": "oe", "ä": "ae"}
        new_identifier = "".join(replace_map.get(c, c) for c in identifier)
        return super().normalize_identifier(new_identifier)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_char_replacement_cs_naming_convention(
    destination_config: DestinationTestConfiguration,
) -> None:
    snow_ = dlt.destinations.snowflake(
        naming_convention=ScandinavianNamingConvention, replace_strategy="staging-optimized"
    )

    pipeline = destination_config.setup_pipeline(
        "test_char_replacement_naming_convention", dev_mode=True, destination=snow_
    )

    data = [{"AmlSistUtførtDato": pendulum.now().date()}]

    pipeline.run(
        data,
        table_name="AMLPerFornyelseø",
        write_disposition="replace",
        loader_file_format="parquet",
    )
    pipeline.run(
        data,
        table_name="AMLPerFornyelseø",
        write_disposition="replace",
        loader_file_format="parquet",
    )
    rel_ = pipeline.dataset()["AMLPerFornyelseoe"]
    results = rel_.fetchall()
    assert len(results) == 1
    assert "AmlSistUtfoertDato" in rel_.columns


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        all_staging_configs=True,
        with_file_format="parquet",
        subset=["snowflake"],
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "use_vectorized_scanner",
    ["TRUE", "FALSE"],
)
def test_snowflake_use_vectorized_scanner(
    destination_config, use_vectorized_scanner: str, mocker: MockerFixture
) -> None:
    """Tests whether the vectorized scanner option is correctly applied when loading Parquet files into Snowflake."""

    from dlt.destinations.impl.snowflake import snowflake

    os.environ["DESTINATION__SNOWFLAKE__USE_VECTORIZED_SCANNER"] = use_vectorized_scanner

    load_job_spy = mocker.spy(snowflake, "gen_copy_sql")

    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    columns_schema, _ = table_update_and_row()
    expected_rows = deepcopy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)

    @dlt.resource(table_name="data_types", write_disposition="merge", columns=columns_schema)
    def my_resource():
        nonlocal data_types
        yield [data_types] * 10

    pipeline = destination_config.setup_pipeline(
        f"vectorized_scanner_{use_vectorized_scanner}_{uniq_id()}",
        dataset_name="parquet_test_" + uniq_id(),
    )

    info = pipeline.run(my_resource(), **destination_config.run_kwargs)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"
    assert len(package_info.jobs["failed_jobs"]) == 0
    # 1 table + 1 state + 2 reference jobs if staging
    expected_completed_jobs = 2 + 2 if pipeline.staging else 2
    # add sql merge job
    if destination_config.supports_merge:
        expected_completed_jobs += 1
    assert len(package_info.jobs["completed_jobs"]) == expected_completed_jobs

    if use_vectorized_scanner == "FALSE":
        # no vectorized scanner in all copy jobs
        assert sum(
            [
                1
                for spy_return in load_job_spy.spy_return_list
                if "USE_VECTORIZED_SCANNER = TRUE" not in spy_return
            ]
        ) == len(load_job_spy.spy_return_list)
        assert sum(
            [
                1
                for spy_return in load_job_spy.spy_return_list
                if "ON_ERROR = ABORT_STATEMENT" not in spy_return
            ]
        ) == len(load_job_spy.spy_return_list)

    elif use_vectorized_scanner == "TRUE":
        # vectorized scanner in one copy job to data_types
        assert (
            sum(
                [
                    1
                    for spy_return in load_job_spy.spy_return_list
                    if "USE_VECTORIZED_SCANNER = TRUE" in spy_return
                ]
            )
            == 1
        )
        assert (
            sum(
                [
                    1
                    for spy_return in load_job_spy.spy_return_list
                    if "ON_ERROR = ABORT_STATEMENT" in spy_return
                ]
            )
            == 1
        )

        # the vectorized scanner shows NULL values in json outputs when enabled
        # as a result, when queried back, we receive a string "null" in json type
        expected_rows["col9_null"] = "null"

    with pipeline.sql_client() as sql_client:
        qual_name = sql_client.make_qualified_table_name
        db_rows = sql_client.execute_sql(f"SELECT * FROM {qual_name('data_types')}")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # "snowflake" does not parse JSON from parquet string so double parse
        assert_all_data_types_row(
            sql_client.capabilities,
            db_row,
            expected_row=expected_rows,
            schema=columns_schema,
            parse_json_strings=True,
        )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_cluster_hints(destination_config: DestinationTestConfiguration) -> None:
    from dlt.destinations.impl.snowflake.sql_client import SnowflakeSqlClient

    def get_cluster_key(sql_client: SnowflakeSqlClient, table_name: str) -> str:
        with sql_client:
            _catalog_name, schema_name, table_names = sql_client._get_information_schema_components(
                table_name
            )
            qry = f"""
                SELECT CLUSTERING_KEY FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema_name}'
                AND TABLE_NAME = '{table_names[0]}'
            """
            return sql_client.execute_sql(qry)[0][0]

    pipeline = destination_config.setup_pipeline("test_snowflake_cluster_hints", dev_mode=True)
    sql_client = cast(SnowflakeSqlClient, pipeline.sql_client())
    table_name = "test_snowflake_cluster_hints"

    @dlt.resource(table_name=table_name)
    def test_data():
        return [
            {"c1": 1, "c2": "a"},
            {"c1": 2, "c2": "b"},
        ]

    # create new table with clustering
    test_data.apply_hints(columns=[{"name": "c1", "cluster": True}])
    info = pipeline.run(test_data(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert get_cluster_key(sql_client, table_name) == 'LINEAR("C1")'

    # change cluster hints on existing table without adding new column
    test_data.apply_hints(columns=[{"name": "c2", "cluster": True}])
    info = pipeline.run(test_data(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert get_cluster_key(sql_client, table_name) == 'LINEAR("C1")'  # unchanged (no new column)

    # add new column to existing table with pending cluster hints from previous run
    test_data.apply_hints(columns=[{"name": "c3", "data_type": "bool"}])
    info = pipeline.run(test_data(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert get_cluster_key(sql_client, table_name) == 'LINEAR("C1","C2")'  # updated

    # remove clustering from existing table
    test_data.apply_hints(
        columns=[
            {"name": "c1", "cluster": False},
            {"name": "c2", "cluster": False},
            {"name": "c4", "data_type": "bool"},  # include new column to trigger alter
        ]
    )
    info = pipeline.run(test_data(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert get_cluster_key(sql_client, table_name) is None

    # add clustering to existing table (and add new column to trigger alter)
    test_data.apply_hints(
        columns=[
            {"name": "c1", "cluster": True},
            {"name": "c5", "data_type": "bool"},  # include new column to trigger alter
        ]
    )
    info = pipeline.run(test_data(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert get_cluster_key(sql_client, table_name) == 'LINEAR("C1")'


@pytest.mark.skip(reason="perf test for merge")
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["snowflake"],
    ),
    ids=lambda x: x.name,
)
def test_snowflake_merge_time(destination_config):
    import pyarrow as pa
    import numpy as np
    import time
    from datetime import date, timedelta

    # create a unique dataset name for this test
    dataset_name = f"merge_performance_{uniq_id()}"
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_merge_time", dataset_name=dataset_name
    )

    # define the number of rows and date range
    num_rows = 1_000_000
    base_date = date(2023, 1, 1)
    days = 5

    # generate data for 5 different days
    all_data = []

    # create column data
    user_ids = np.random.randint(1, 10000, num_rows)
    product_ids = np.random.randint(1, 1000, num_rows)
    values = np.random.random(num_rows) * 1000

    # create data for each day
    for day_offset in range(days):
        current_date = base_date + timedelta(days=day_offset)
        dates = np.array([current_date] * num_rows)

        table = pa.Table.from_arrays(
            [pa.array(user_ids), pa.array(product_ids), pa.array(dates), pa.array(values)],
            names=["user_id", "product_id", "event_date", "value"],
        )
        all_data.append(table)

    combined_table = pa.concat_tables(all_data)

    @dlt.resource(
        table_name="merge_test",
        primary_key=["user_id", "product_id"],
        merge_key="event_date",
        write_disposition="merge",
    )
    def initial_data():
        yield combined_table

    # Load initial data
    print(f"Loading {len(combined_table)} rows of initial data...")
    start_time = time.time()
    info = pipeline.run(initial_data(), **destination_config.run_kwargs)
    initial_load_time = time.time() - start_time
    print(f"Initial data load completed in {initial_load_time:.2f} seconds")
    assert_load_info(info)

    # generate overlap data (2 days overlapping with initial data)
    overlap_days = 2
    new_days = 3
    all_new_data = []

    # create data for overlapping days (modify some values)
    for day_offset in range(overlap_days):
        current_date = base_date + timedelta(days=day_offset)
        dates = np.array([current_date] * num_rows)

        # use same IDs but different values for the overlapping data
        new_values = np.random.random(num_rows) * 2000

        table = pa.Table.from_arrays(
            [pa.array(user_ids), pa.array(product_ids), pa.array(dates), pa.array(new_values)],
            names=["user_id", "product_id", "event_date", "value"],
        )
        all_new_data.append(table)

    # create data for new days
    for day_offset in range(days, days + new_days):
        current_date = base_date + timedelta(days=day_offset)
        dates = np.array([current_date] * num_rows)

        # different user IDs for completely new data
        new_user_ids = np.random.randint(10000, 20000, num_rows)
        new_values = np.random.random(num_rows) * 1500

        table = pa.Table.from_arrays(
            [pa.array(new_user_ids), pa.array(product_ids), pa.array(dates), pa.array(new_values)],
            names=["user_id", "product_id", "event_date", "value"],
        )
        all_new_data.append(table)

    new_combined_table = pa.concat_tables(all_new_data)

    # define merge resource
    @dlt.resource(
        table_name="merge_test",
        primary_key=["user_id", "product_id"],
        merge_key="event_date",
        write_disposition="merge",
    )
    def merge_data():
        yield new_combined_table

    print(f"Merging {len(new_combined_table)} rows of data with {overlap_days} days overlap...")
    start_time = time.time()
    merge_info = pipeline.run(merge_data(), **destination_config.run_kwargs)
    merge_time = time.time() - start_time
    print(f"Merge operation completed in {merge_time:.2f} seconds")
    assert_load_info(merge_info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("loader_file_format", ["jsonl", "csv", "parquet"])
def test_snowflake_decfloat_loading_and_schema(
    destination_config: DestinationTestConfiguration,
    loader_file_format: TLoaderFileFormat,
) -> None:
    """Load decimal data using DECFLOAT type and verify across file formats.

    Text-based formats (jsonl, csv) work correctly: INFORMATION_SCHEMA shows DECFLOAT
    and values round-trip through dataset().fetchall().

    Parquet does NOT work: parquet maps unbound decimals to DECIMAL(38,9) which has only
    29 integer digits. Values requiring DECFLOAT's full 36-digit range fail at normalize
    because they overflow the fixed parquet precision.
    """
    snow_ = dlt.destinations.snowflake(use_decfloat=True)
    pipeline = destination_config.setup_pipeline(
        "test_decfloat_loading",
        dataset_name="decfloat_test_" + uniq_id(),
        destination=snow_,
    )

    # Use values that exceed 128-bit integer range (2^127-1 ≈ 1.7e38) when unscaled.
    # "1e35" has 36 digits total and its significand exceeds 128-bit capacity, proving
    # DECFLOAT handles what a fixed-precision 128-bit decimal cannot.
    val_large = decimal.Decimal("123456789012345678901234567890123456")  # 36 integer digits
    val_small = decimal.Decimal("0.123456789012345678901234567890123456")  # 36 fractional digits

    @dlt.resource(
        table_name="decfloat_data",
        columns=[{"name": "amount", "data_type": "decimal"}],
    )
    def decimal_data():
        yield [
            {"amount": val_small},
            {"amount": val_large},
        ]

    if loader_file_format == "parquet":
        # Parquet uses fixed-precision DECIMAL(38,9) → 29 integer digits max.
        # Values exceeding 128-bit range can't be represented in parquet at all, so
        # the pipeline fails at normalize. Use jsonl or csv for DECFLOAT's full range.
        with pytest.raises(PipelineStepFailed):
            pipeline.run(decimal_data(), loader_file_format=loader_file_format)
        return

    info = pipeline.run(decimal_data(), loader_file_format=loader_file_format)
    assert_load_info(info)

    # verify the column type in Snowflake's INFORMATION_SCHEMA is DECFLOAT
    with pipeline.sql_client() as client:
        _, schema_name, table_names = client._get_information_schema_components("decfloat_data")
        rows = client.execute_sql(
            "SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS"
            f" WHERE table_schema = '{schema_name}'"
            f" AND table_name = '{table_names[0]}'"
            " AND column_name = 'AMOUNT'"
        )
        assert rows[0][0] == "DECFLOAT"

    # verify data via dataset() fetchall with increased precision context
    with decimal.localcontext(decimal.Context(prec=38)):
        rows = pipeline.dataset().decfloat_data.select("amount").order_by("amount").fetchall()
    assert len(rows) == 2
    assert rows[0][0] == val_small
    assert rows[1][0] == val_large


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_decfloat_arrow_reading_not_supported(
    destination_config: DestinationTestConfiguration,
) -> None:
    """The arrow/df path does not correctly handle DECFLOAT columns.
    The Snowflake connector logs 'unknown snowflake data type : DECFLOAT' and returns
    a raw dict instead of Decimal. The DB-API path (fetchall()) works correctly."""
    snow_ = dlt.destinations.snowflake(use_decfloat=True)
    pipeline = destination_config.setup_pipeline(
        "test_decfloat_arrow",
        dataset_name="decfloat_arrow_" + uniq_id(),
        destination=snow_,
    )

    @dlt.resource(
        table_name="decfloat_arrow",
        columns=[
            {"name": "amount", "data_type": "decimal"},
            {"name": "label", "data_type": "text"},
        ],
    )
    def decimal_data():
        yield [{"amount": decimal.Decimal("42.5"), "label": "test"}]

    info = pipeline.run(decimal_data(), loader_file_format="jsonl")
    assert_load_info(info)

    # DB-API path via dataset() works correctly
    rows = pipeline.dataset().decfloat_arrow.select("amount").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == decimal.Decimal("42.5")

    # arrow path: Snowflake connector doesn't recognize DECFLOAT and returns a raw
    # structured dict {'exponent': ..., 'significand': ...} instead of a proper Decimal.
    # Using .arrow() directly to surface the underlying issue without pandas wrapping.
    table = pipeline.dataset().decfloat_arrow.arrow()
    assert table is not None
    val = table.column("amount").to_pylist()[0]
    assert not isinstance(
        val, decimal.Decimal
    ), f"Expected raw dict from arrow path, got Decimal: {val}"
    assert isinstance(val, dict)
    assert "exponent" in val and "significand" in val


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_decfloat_precision_preservation(
    destination_config: DestinationTestConfiguration,
) -> None:
    """DECFLOAT stores up to 36 significant digits. Standard DECIMAL(38,9) has only 29 integer
    digits and 9 fractional, so it can't store a number with 36 significant digits without
    truncation. This test loads such numbers and verifies exact round-trip.

    Python's default decimal context has prec=28, but DECFLOAT supports 36 digits.
    We must increase Python's precision context BEFORE fetching so the Snowflake connector
    creates Decimal objects with full precision.
    """
    snow_ = dlt.destinations.snowflake(use_decfloat=True)
    pipeline = destination_config.setup_pipeline(
        "test_decfloat_precision",
        dataset_name="decfloat_prec_" + uniq_id(),
        destination=snow_,
    )

    # 36-digit significant figures: can't fit in DECIMAL(38,9) without precision loss
    # large number: 30 integer digits + 6 fractional = 36 significant digits
    large_val = decimal.Decimal("123456789012345678901234567890.123456")
    # small number: 36 fractional significant digits
    small_val = decimal.Decimal("0.123456789012345678901234567890123456")

    @dlt.resource(
        table_name="decfloat_precision",
        columns=[{"name": "val", "data_type": "decimal"}],
    )
    def precision_data():
        yield [
            {"val": large_val},
            {"val": small_val},
        ]

    info = pipeline.run(precision_data(), loader_file_format="jsonl")
    assert_load_info(info)

    # The Snowflake connector creates Decimal objects using the current thread-local decimal
    # context, so we MUST set extended precision BEFORE the fetch call.
    with decimal.localcontext() as ctx:
        ctx.prec = 38  # enough for DECFLOAT's 36 significant digits

        rows = pipeline.dataset().decfloat_precision.select("val").order_by("val").fetchall()
        assert len(rows) == 2

        retrieved_small = rows[0][0]
        retrieved_large = rows[1][0]

        # verify exact round-trip: the values should survive with full precision
        assert (
            retrieved_small == small_val
        ), f"Small value precision loss: {retrieved_small} != {small_val}"
        assert (
            retrieved_large == large_val
        ), f"Large value precision loss: {retrieved_large} != {large_val}"

        # verify addition with extended precision works correctly
        total = retrieved_small + retrieved_large
        expected_total = small_val + large_val
        assert total == expected_total


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_decfloat_python_default_precision_warning(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Demonstrate that Python's default decimal precision (28) is insufficient for DECFLOAT's
    36-digit range. The Snowflake connector creates Decimal objects using the current context
    during fetch, so fetching with prec=28 already truncates the value."""
    snow_ = dlt.destinations.snowflake(use_decfloat=True)
    pipeline = destination_config.setup_pipeline(
        "test_decfloat_default_prec",
        dataset_name="decfloat_defprec_" + uniq_id(),
        destination=snow_,
    )

    # 36 significant digits: exceeds Python's default prec=28
    val_36_digits = decimal.Decimal("123456789012345678901234567890.123456")

    @dlt.resource(
        table_name="decfloat_defprec",
        columns=[{"name": "val", "data_type": "decimal"}],
    )
    def precision_data():
        yield [{"val": val_36_digits}]

    info = pipeline.run(precision_data(), loader_file_format="jsonl")
    assert_load_info(info)

    # fetch with default Python precision (28): the connector truncates during fetch
    rows = pipeline.dataset().decfloat_defprec.select("val").fetchall()
    retrieved_default = rows[0][0]
    # 36-digit number is already truncated to 28 significant digits at fetch time
    assert retrieved_default != val_36_digits

    # fetch with extended precision: the connector preserves all 36 digits
    with decimal.localcontext() as ctx:
        ctx.prec = 38
        rows = pipeline.dataset().decfloat_defprec.select("val").fetchall()
        retrieved_extended = rows[0][0]
        assert retrieved_extended == val_36_digits
