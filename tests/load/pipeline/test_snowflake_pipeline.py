import os
import pytest
from pytest_mock import MockerFixture

import dlt
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt.load.exceptions import LoadClientJobFailed
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.common.configuration.exceptions import ConfigurationValueError

from tests.load.pipeline.test_pipelines import simple_nested_pipeline
from tests.load.snowflake.test_snowflake_client import QUERY_TAG
from tests.pipeline.utils import assert_load_info, assert_query_data
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    drop_active_pipeline_data,
)

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

    drop_active_pipeline_data()

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
        assert_query_data(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


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
        assert_query_data(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


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
    assert "AmlSistUtfoertDato" in rel_.columns_schema


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_staging_configs=True, with_file_format="parquet", subset=["snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "on_error_parquet",
    ["ABORT_STATEMENT", "SKIP_FILE", "CONTINUE"],
)
def test_snowflake_use_vectorized_scanner(
    destination_config: DestinationTestConfiguration,
    on_error_parquet: str
) -> None:
    """Using use_vectorized_scanner = true option to use vectorized scanner for parquet files"""
    os.environ["DESTINATION__SNOWFLAKE__USE_VECTORIZED_SCANNER"] = "TRUE"
    os.environ["DESTINATION__SNOWFLAKE__ON_ERROR_PARQUET"] = on_error_parquet

    pipeline, data = simple_nested_pipeline(
        destination_config, f"vectorized_scanner_{on_error_parquet}_{uniq_id()}", False
    )

    if on_error_parquet not in ["ABORT_STATEMENT", "SKIP_FILE"]:
        with pytest.raises(PipelineStepFailed) as step_ex:
            pipeline.run(data, **destination_config.run_kwargs)
        assert step_ex.value.__cause__, ConfigurationValueError
    else:
        info = pipeline.run(data(), **destination_config.run_kwargs)
        assert_load_info(info)

    # test data that causes an 'Max LOB size (134217728) exceeded' error in Snowflake
    @dlt.resource
    def large_data():
        yield {"id": 1, "value": "A" * 214748364}

    if on_error_parquet == "ABORT_STATEMENT":
        with pytest.raises(LoadClientJobFailed) as step_ex:
            pipeline.run(large_data(), **destination_config.run_kwargs)

    if on_error_parquet == "SKIP_FILE":
        info = pipeline.run(data(), **destination_config.run_kwargs)
        assert_load_info(info)
