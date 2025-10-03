from copy import deepcopy
import os
import pytest
from pytest_mock import MockerFixture

import dlt
from dlt.common import pendulum
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
