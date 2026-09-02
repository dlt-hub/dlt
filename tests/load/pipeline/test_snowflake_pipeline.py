import datetime
import decimal
import json
from copy import deepcopy
import os
import pytest
from typing import Any, cast
from pytest_mock import MockerFixture

import dlt
from dlt.common import pendulum
from dlt.common.data_writers.escape import escape_snowflake_literal
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.destination import TLoaderFileFormat
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.load.exceptions import LoadClientJobTerminalRetry
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.pipeline.utils import simple_nested_pipeline
from tests.load.snowflake.test_snowflake_client import QUERY_TAG

from tests.pipeline.utils import assert_load_info, assert_query_column
from tests.load.utils import (
    assert_all_data_types_row,
    destinations_configs,
    DestinationTestConfiguration,
    AWS_BUCKET,
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
    set_query_tags_spy = mocker.spy(SnowflakeSqlClient, "set_query_tags")
    pipeline = destination_config.setup_pipeline("test_snowflake_query_tagging")
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert_load_info(info)

    expected_load_id = info.loads_ids[0]
    expected_pipeline_name = pipeline.pipeline_name
    expected_source = pipeline.default_schema.name
    expected_resource = pipeline.default_schema.get_table("digits")["resource"]

    tag_calls = [call.args[1] for call in set_query_tags_spy.call_args_list]
    load_tags = [
        call for call in tag_calls if call["operation"] == "load" and call["table"] == "digits"
    ]
    assert load_tags
    assert load_tags[0] == {
        "operation": "load",
        "source": expected_source,
        "resource": expected_resource,
        "table": "digits",
        "load_id": expected_load_id,
        "pipeline_name": expected_pipeline_name,
    }

    complete_load_tags = [call for call in tag_calls if call["operation"] == "complete_load"]
    assert complete_load_tags
    assert complete_load_tags[0] == {
        "operation": "complete_load",
        "source": expected_source,
        "resource": "",
        "table": "",
        "load_id": expected_load_id,
        "pipeline_name": expected_pipeline_name,
    }

    operations = {call["operation"] for call in tag_calls}
    assert operations == {
        "complete_load",
        "get_stored_state",
        "load",
        "prepare_storage",
        "update_stored_schema",
    }

    set_query_tags_spy.reset_mock()
    pipeline._schema_storage.clear_storage()
    pipeline.sync_destination()

    sync_tag_calls = [call.args[1] for call in set_query_tags_spy.call_args_list]
    operations = {call["operation"] for call in sync_tag_calls}
    assert operations == {"get_stored_state", "get_stored_schema"}
    for operation in ("get_stored_state", "get_stored_schema"):
        operation_tags = [call for call in sync_tag_calls if call["operation"] == operation]
        assert operation_tags
        assert operation_tags[0] == {
            "operation": operation,
            "source": expected_source,
            "resource": "",
            "table": "",
            "load_id": "",
            "pipeline_name": expected_pipeline_name,
        }

    set_query_tags_spy.reset_mock()
    info = pipeline.run(
        [1, 2, 3], table_name="digits", refresh="drop_sources", **destination_config.run_kwargs
    )
    assert_load_info(info)
    refresh_load_id = info.loads_ids[0]
    refresh_tag_calls = [call.args[1] for call in set_query_tags_spy.call_args_list]
    operations = {call["operation"] for call in refresh_tag_calls}
    assert operations == {
        "complete_load",
        "drop_tables",
        "load",
        "prepare_storage",
        "update_stored_schema",
    }
    drop_table_tags = [call for call in refresh_tag_calls if call["operation"] == "drop_tables"]
    assert drop_table_tags
    assert drop_table_tags[0] == {
        "operation": "drop_tables",
        "source": expected_source,
        "resource": "",
        "table": "",
        "load_id": refresh_load_id,
        "pipeline_name": expected_pipeline_name,
    }


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
    assert isinstance(f_jobs.value.__cause__, LoadClientJobTerminalRetry)
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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("loader_file_format", ["jsonl", "parquet"])
@pytest.mark.parametrize(
    "keep_staged_files", [True, False], ids=["keep-staged-files", "remove-staged-files"]
)
def test_snowflake_local_load_table_name_with_spaces(
    destination_config: DestinationTestConfiguration,
    loader_file_format: TLoaderFileFormat,
    keep_staged_files: bool,
) -> None:
    """Local files load into table names requiring quoting: PUT, COPY and REMOVE must quote
    the stage reference and the file path."""
    os.environ["DESTINATION__SNOWFLAKE__KEEP_STAGED_FILES"] = str(keep_staged_files)
    snow_ = dlt.destinations.snowflake(naming_convention="duck_case")
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_local_load_table_name_with_spaces",
        dataset_name="space_table_" + uniq_id(),
        destination=snow_,
    )

    info = pipeline.run(
        [{"value": 1}], table_name="my table", loader_file_format=loader_file_format
    )
    assert_load_info(info)
    load_id = info.loads_ids[0]

    with pipeline.sql_client() as client:
        qualified_table_name = client.make_qualified_table_name("my table")
        value_column = client.escape_column_name("value")
        assert client.execute_sql(f"SELECT {value_column} FROM {qualified_table_name}") == [(1,)]

        stage_name = client.make_qualified_table_name("%my table")
        stage_location = f'@{stage_name}/"{load_id}"'
        staged_files = client.execute_sql(f"LIST {escape_snowflake_literal(stage_location)}")
        assert len(staged_files) == (1 if keep_staged_files else 0)


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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        all_staging_configs=True, subset=["snowflake"], with_file_format="parquet"
    ),
    ids=lambda x: x.name,
)
def test_snowflake_staging_with_default_chain_credentials(
    destination_config: DestinationTestConfiguration,
    mocker: Any,
) -> None:
    """Snowflake loads data via S3 staging using frozen credentials from botocore default chain."""
    fs_creds = dlt.secrets.get("destination.filesystem.credentials", AwsCredentials)
    if not fs_creds or not hasattr(fs_creds, "aws_access_key_id"):
        pytest.skip("S3 filesystem credentials not configured")

    sts_creds = fs_creds.to_sts_credentials()
    fs_creds.aws_access_key_id = sts_creds["aws_access_key_id"]
    fs_creds.aws_secret_access_key = sts_creds["aws_secret_access_key"]
    fs_creds.aws_session_token = sts_creds["aws_session_token"]
    boto_session = fs_creds._to_botocore_session()
    assert boto_session.get_credentials().token == fs_creds.aws_session_token

    spy = mocker.spy(AwsCredentials, "to_session_credentials")

    staging_destination = dlt.destinations.filesystem(AWS_BUCKET, credentials=boto_session)
    pipeline = destination_config.setup_pipeline(
        "snowflake_staging_" + uniq_id(), dev_mode=True, staging=staging_destination
    )
    pipeline.run(
        [{"id": i, "value": f"row_{i}"} for i in range(5)],
        table_name="default_chain_test",
        loader_file_format="parquet",
    )

    with pipeline.sql_client() as c:
        rows = c.execute_sql("SELECT count(*) FROM default_chain_test")
        assert rows[0][0] == 5

    # verify to_session_credentials was called and returned frozen STS credentials with token
    assert spy.call_count > 0
    for call in spy.spy_return_list:
        assert call["aws_session_token"] is not None
        assert call["aws_session_token"] == fs_creds.aws_session_token


ALL_TYPES_STRUCT_DDL = (
    "OBJECT(s VARCHAR(16777216), i NUMBER(19,0), f FLOAT, b BOOLEAN, ts TIMESTAMP_LTZ(6), d DATE,"
    " t TIME(6), dec NUMBER(38,9), bin BINARY(8388608), arr ARRAY(NUMBER(19,0)),"
    " nested OBJECT(x VARCHAR(16777216), y NUMBER(19,0)), mp MAP(VARCHAR(16777216), NUMBER(19,0)),"
    " with space VARCHAR(16777216), 日本語 NUMBER(19,0))"
)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_nested_types_parquet(destination_config: DestinationTestConfiguration) -> None:
    """Native-nested arrow via parquet: new table (struct with all data types), new nested column,
    and in-place data type evolution (struct gains a field)."""
    from dlt.common.libs.pyarrow import pyarrow as pa

    snow = dlt.destinations.snowflake(use_nested_types=True)
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_nested_types_parquet", dev_mode=True, destination=snow
    )

    def run_arrow(schema: Any, data: Any) -> Any:
        @dlt.resource(name="items", primary_key="pk", write_disposition="append")
        def items(tbl: Any) -> Any:
            yield tbl

        return pipeline.run(
            items(pa.Table.from_pylist(data, schema=schema)), loader_file_format="parquet"
        )

    # a struct covering every supported data type plus nested list / struct / map
    all_types = pa.struct(
        [
            ("s", pa.string()),
            ("i", pa.int64()),
            ("f", pa.float64()),
            ("b", pa.bool_()),
            ("ts", pa.timestamp("us", tz="UTC")),
            ("d", pa.date32()),
            ("t", pa.time64("us")),
            ("dec", pa.decimal128(38, 9)),
            ("bin", pa.binary()),
            ("arr", pa.list_(pa.int64())),
            ("nested", pa.struct([("x", pa.string()), ("y", pa.int64())])),
            ("mp", pa.map_(pa.string(), pa.int64())),
            # field names that are not normalized and need quoting/escaping
            ("with space", pa.string()),
            ("日本語", pa.int64()),
        ]
    )
    value = {
        "s": "hello",
        "i": 42,
        "f": 1.5,
        "b": True,
        "ts": datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "d": datetime.date(2024, 1, 2),
        "t": datetime.time(3, 4, 5),
        "dec": decimal.Decimal("123.456"),
        "bin": b"\x01\x02\x03",
        "arr": [1, 2, 3],
        "nested": {"x": "q", "y": 7},
        "mp": [("k", 9)],
        "with space": "spaced",
        "日本語": 99,
    }

    def qual(client: Any) -> str:
        return client.make_qualified_table_name("items")

    # new table: struct with all data types
    schema1 = pa.schema(
        [pa.field("pk", pa.int64(), nullable=False), pa.field("payload", all_types)]
    )
    assert_load_info(run_arrow(schema1, [{"pk": 1, "payload": value}]))
    with pipeline.sql_client() as client:
        typeof = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(payload) FROM {qual(client)} WHERE pk = 1"
        )[0][0]
        payload = json.loads(
            client.execute_sql(f"SELECT payload FROM {qual(client)} WHERE pk = 1")[0][0]
        )
    assert typeof == ALL_TYPES_STRUCT_DDL + "[LOB]"
    assert payload["s"] == "hello"
    assert payload["i"] == 42
    assert payload["f"] == 1.5
    assert payload["b"] is True
    assert payload["d"] == "2024-01-02"
    assert payload["t"] == "03:04:05"
    assert payload["ts"] is not None
    assert payload["dec"] == 123.456
    assert payload["bin"] == "010203"
    assert payload["arr"] == [1, 2, 3]
    assert payload["nested"] == {"x": "q", "y": 7}
    assert payload["mp"] == {"k": 9}
    assert payload["with space"] == "spaced"
    assert payload["日本語"] == 99

    # query nested fields, array elements and map values (incl. weird field names) through dataset()
    spaced, uni, arr0, map_v, nested_x = pipeline.dataset()(
        "SELECT payload['with space'], payload['日本語'], payload['arr'][0],"
        " payload['mp']['k'], payload['nested']['x'] FROM items WHERE pk = 1"
    ).fetchall()[0]
    assert spaced == "spaced"
    assert uni == 99
    assert arr0 == 1
    assert map_v == 9
    assert nested_x == "q"

    # new column: add a whole new nested column
    schema2 = pa.schema(
        [
            pa.field("pk", pa.int64(), nullable=False),
            pa.field("payload", all_types),
            pa.field("more", pa.list_(pa.string())),
        ]
    )
    assert_load_info(run_arrow(schema2, [{"pk": 2, "payload": value, "more": ["a", "b"]}]))
    with pipeline.sql_client() as client:
        more_type = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(more) FROM {qual(client)} WHERE pk = 2"
        )[0][0]
        more = {r[0]: r[1] for r in client.execute_sql(f"SELECT pk, more FROM {qual(client)}")}
    assert more_type.startswith("ARRAY(")
    assert more[1] is None  # existing row null-filled for the new column
    assert json.loads(more[2]) == ["a", "b"]

    # data type evolution: the payload struct gains a field
    all_types_v2 = pa.struct([*list(all_types), pa.field("added", pa.string())])
    schema3 = pa.schema(
        [
            pa.field("pk", pa.int64(), nullable=False),
            pa.field("payload", all_types_v2),
            pa.field("more", pa.list_(pa.string())),
        ]
    )
    assert_load_info(
        run_arrow(schema3, [{"pk": 3, "payload": {**value, "added": "new"}, "more": ["c"]}])
    )
    with pipeline.sql_client() as client:
        typeof = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(payload) FROM {qual(client)} WHERE pk = 3"
        )[0][0]
        payloads = {
            r[0]: json.loads(r[1])
            for r in client.execute_sql(f"SELECT pk, payload FROM {qual(client)}")
        }
    assert "added VARCHAR" in typeof
    assert payloads[1]["added"] is None  # row loaded before the field existed null-fills it
    assert payloads[3]["added"] == "new"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_nested_types_jsonl(destination_config: DestinationTestConfiguration) -> None:
    """jsonl with only the nested columns defined via `columns` hints (scalars inferred from data):
    new table, new nested column, and in-place data type evolution."""
    from dlt.common.libs.pyarrow import pyarrow as pa

    snow = dlt.destinations.snowflake(use_nested_types=True)
    pipeline = destination_config.setup_pipeline(
        "test_snowflake_nested_types_jsonl", dev_mode=True, destination=snow
    )

    def nested_hints(**fields: Any) -> Any:
        # only the nested columns are declared (as an arrow schema); the rest is inferred from data
        return pa.schema([pa.field(n, t) for n, t in fields.items()])

    def run(columns: Any, data: Any) -> Any:
        @dlt.resource(name="items", primary_key="pk", columns=columns, write_disposition="append")
        def items(rows: Any) -> Any:
            yield rows

        return pipeline.run(items(data), loader_file_format="jsonl")

    payload_t = pa.struct(
        [
            ("a", pa.int64()),
            # field names that are not normalized and need quoting/escaping
            ("with space", pa.string()),
            ("日本語", pa.int64()),
            ("tags", pa.list_(pa.int64())),
            ("attrs", pa.map_(pa.string(), pa.string())),
            ("nested", pa.struct([("x", pa.string()), ("y", pa.bool_())])),
        ]
    )

    def qual(client: Any) -> str:
        return client.make_qualified_table_name("items")

    # new table: only `payload` is hinted; `pk` and `note` are inferred from the data
    assert_load_info(
        run(
            nested_hints(payload=payload_t),
            [
                {
                    "pk": 1,
                    "note": "first",
                    "payload": {
                        "a": 10,
                        "with space": "hello world",
                        "日本語": 7,
                        "tags": [1, 2],
                        "attrs": {"weird key": "v"},
                        "nested": {"x": "hi", "y": True},
                    },
                }
            ],
        )
    )
    tcols = pipeline.default_schema.get_table_columns("items")
    assert tcols["pk"]["data_type"] == "bigint"
    assert tcols["note"]["data_type"] == "text"
    assert tcols["payload"]["data_type"] == "json"
    with pipeline.sql_client() as client:
        typeof = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(payload) FROM {qual(client)} WHERE pk = 1"
        )[0][0]
        payload = json.loads(
            client.execute_sql(f"SELECT payload FROM {qual(client)} WHERE pk = 1")[0][0]
        )
    assert typeof.startswith("OBJECT(")
    assert payload == {
        "a": 10,
        "with space": "hello world",
        "日本語": 7,
        "tags": [1, 2],
        "attrs": {"weird key": "v"},
        "nested": {"x": "hi", "y": True},
    }

    # query nested fields, array elements and map values (incl. weird field/key names) through dataset()
    spaced, uni, tag0, attr_v, nested_x = pipeline.dataset()(
        "SELECT payload['with space'], payload['日本語'], payload['tags'][0],"
        " payload['attrs']['weird key'], payload['nested']['x'] FROM items WHERE pk = 1"
    ).fetchall()[0]
    assert spaced == "hello world"
    assert uni == 7
    assert tag0 == 1
    assert attr_v == "v"
    assert nested_x == "hi"

    # new column: add a new nested column (`extra`) and an inferred scalar (`amount`)
    assert_load_info(
        run(
            nested_hints(payload=payload_t, extra=pa.list_(pa.int64())),
            [
                {
                    "pk": 2,
                    "note": "second",
                    "amount": 1.5,
                    "payload": {
                        "a": 20,
                        "with space": "",
                        "日本語": 0,
                        "tags": [],
                        "attrs": {},
                        "nested": {"x": "z", "y": False},
                    },
                    "extra": [7, 8, 9],
                }
            ],
        )
    )
    assert pipeline.default_schema.get_table_columns("items")["amount"]["data_type"] == "double"
    with pipeline.sql_client() as client:
        extra_type = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(extra) FROM {qual(client)} WHERE pk = 2"
        )[0][0]
        extra = {r[0]: r[1] for r in client.execute_sql(f"SELECT pk, extra FROM {qual(client)}")}
    assert extra_type.startswith("ARRAY(")
    assert extra[1] is None  # existing row null-filled for the new column
    assert json.loads(extra[2]) == [7, 8, 9]

    # data type evolution: `payload` struct gains a field
    payload_v2 = pa.struct([*list(payload_t), pa.field("b", pa.string())])
    assert_load_info(
        run(
            nested_hints(payload=payload_v2, extra=pa.list_(pa.int64())),
            [
                {
                    "pk": 3,
                    "note": "third",
                    "amount": 2.5,
                    "payload": {
                        "a": 30,
                        "with space": "w",
                        "日本語": 3,
                        "tags": [3],
                        "attrs": {"m": "n"},
                        "nested": {"x": "q", "y": True},
                        "b": "added",
                    },
                    "extra": [1],
                }
            ],
        )
    )
    with pipeline.sql_client() as client:
        typeof = client.execute_sql(
            f"SELECT SYSTEM$TYPEOF(payload) FROM {qual(client)} WHERE pk = 3"
        )[0][0]
        payloads = {
            r[0]: json.loads(r[1])
            for r in client.execute_sql(f"SELECT pk, payload FROM {qual(client)}")
        }
    assert "b VARCHAR" in typeof
    assert payloads[1]["b"] is None  # row loaded before the field existed null-fills it
    assert payloads[3]["b"] == "added"

    # filter on nested data via WHERE: struct field and array membership
    ds = pipeline.dataset()
    assert [r[0] for r in ds("SELECT pk FROM items WHERE payload['a'] = 30").fetchall()] == [3]
    # a structured ARRAY must be cast to a semi-structured array for ARRAY_CONTAINS
    assert [
        r[0]
        for r in ds(
            "SELECT pk FROM items WHERE ARRAY_CONTAINS(2::VARIANT, payload['tags']::ARRAY)"
        ).fetchall()
    ] == [1]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_json_columns_stay_variant(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Backward compatibility: without use_nested_types a json column stays VARIANT, and adding a
    second json column later still evolves to VARIANT (no structured typing)."""

    # default snowflake destination - use_nested_types is NOT set
    pipeline = destination_config.setup_pipeline("test_snowflake_json_variant", dev_mode=True)

    def run(columns: Any, data: Any) -> Any:
        @dlt.resource(name="items", columns=columns, write_disposition="append")
        def items(rows: Any) -> Any:
            yield rows

        return pipeline.run(items(data), loader_file_format="jsonl")

    def col_types(client: Any) -> Any:
        rows = client.execute_sql(f"DESCRIBE TABLE {client.make_qualified_table_name('items')}")
        return {r[0]: r[1] for r in rows}

    # a json column is stored as VARIANT
    assert_load_info(
        run({"payload": {"data_type": "json"}}, [{"id": 1, "payload": {"a": 1, "b": [1, 2]}}])
    )
    with pipeline.sql_client() as client:
        assert col_types(client)["PAYLOAD"] == "VARIANT"

    # adding a second json column evolves normally and is also VARIANT
    assert_load_info(
        run(
            {"payload": {"data_type": "json"}, "extra": {"data_type": "json"}},
            [{"id": 2, "payload": {"a": 2}, "extra": {"c": 3}}],
        )
    )
    with pipeline.sql_client() as client:
        types = col_types(client)
        assert types["PAYLOAD"] == "VARIANT"
        assert types["EXTRA"] == "VARIANT"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("loader_file_format", ["jsonl", "parquet"])
@pytest.mark.parametrize(
    "context_timezone,expected_offset_hours",
    [("UTC", 0), ("Europe/Berlin", 1)],
    ids=["context-utc", "context-berlin"],
)
def test_snowflake_timestamp_tz_keeps_written_offset(
    destination_config: DestinationTestConfiguration,
    context_timezone: str,
    expected_offset_hours: int,
    loader_file_format: TLoaderFileFormat,
) -> None:
    """`use_timestamp_tz` freezes the offset stored with each value, so every session returns the
    same offset. `TIMESTAMP_LTZ` renders the instant in the session timezone instead.

    The stored offset is the one `dlt` wrote, so it follows the timezone `dlt` stores values in.
    January keeps `Europe/Berlin` on standard time, so the offset is not a DST guess.

    Both file formats are loaded because they carry the offset differently: `jsonl` writes it into
    the value text, while `parquet` writes the same epoch whatever the timezone and carries the
    zone as a column label only.
    """
    instant = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    if expected_offset_hours == 1 and loader_file_format == "parquet":
        # parquet ignore timezone label in parquet metadata
        expected_offset_hours = 0
    expected_offset = datetime.timedelta(hours=expected_offset_hours)
    run_kwargs = {**destination_config.run_kwargs, "loader_file_format": loader_file_format}

    pipeline = destination_config.setup_pipeline(
        "test_snowflake_timestamp_tz_" + uniq_id(),
        dev_mode=True,
        destination=destination_config.destination_factory(use_timestamp_tz=True),
    )
    with Container().injectable_context(TimezoneContext(context_timezone)):
        assert_load_info(
            pipeline.run([{"id": 1, "ts": instant}], table_name="events", **run_kwargs)
        )

    with pipeline.sql_client() as client:
        column_type = client.execute_sql(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = %s AND"
            " table_name = 'EVENTS' AND column_name = 'TS'",
            client.fully_qualified_dataset_name(quote=False),
        )[0][0]
        assert column_type == "TIMESTAMP_TZ"

        qualified_name = client.make_qualified_table_name("events")
        for session_timezone in ("America/New_York", "Asia/Tokyo"):
            client.execute_sql(f"ALTER SESSION SET TIMEZONE = '{session_timezone}'")
            value = client.execute_sql(f"SELECT ts FROM {qualified_name}")[0][0]
            assert value.utcoffset() == expected_offset
            # whatever offset is stored, the instant must survive
            assert value == instant
