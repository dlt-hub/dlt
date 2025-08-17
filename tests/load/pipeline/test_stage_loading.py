import pytest
from typing import List

import dlt, os
from dlt.common import json
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TDataType
from dlt.destinations.path_utils import get_file_format_and_compression

from tests.load.pipeline.test_merge_disposition import github
from tests.pipeline.utils import load_table_counts, assert_load_info
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    assert_all_data_types_row,
    table_update_and_row_for_destination,
)
from tests.cases import table_update_and_row


@dlt.resource(
    table_name="issues", write_disposition="merge", primary_key="id", merge_key=("node_id", "url")
)
def load_modified_issues():
    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
    ) as f:
        issues = json.load(f)

        # change 2 issues
        issue = next(filter(lambda i: i["id"] == 1232152492, issues))
        issue["number"] = 105

        issue = next(filter(lambda i: i["id"] == 1142699354, issues))
        issue["number"] = 300

        yield from issues


@dlt.resource(table_name="events", write_disposition="append", primary_key="timestamp")
def event_many_load_2():
    with open("tests/normalize/cases/event.event.many_load_2.json", "r", encoding="utf-8") as f:
        events = json.load(f)
        yield from events


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config", destinations_configs(all_staging_configs=True), ids=lambda x: x.name
)
def test_staging_load(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_stage_loading_5", dataset_name="test_staging_load" + uniq_id()
    )

    info = pipeline.run(github(), **destination_config.run_kwargs)
    assert_load_info(info)
    # checks if remote_url is set correctly on copy jobs
    metrics = info.metrics[info.loads_ids[0]][0]
    for job_metrics in metrics["job_metrics"].values():
        remote_url = job_metrics.remote_url
        job_ext, is_compressed = get_file_format_and_compression(job_metrics.job_id)
        if job_ext not in ("reference", "sql"):
            assert remote_url.endswith(job_ext if not is_compressed else ".".join([job_ext, "gz"]))
            bucket_uri = destination_config.bucket_url
            if FilesystemConfiguration.is_local_path(bucket_uri):
                bucket_uri = FilesystemConfiguration.make_file_url(bucket_uri)
            assert remote_url.startswith(bucket_uri)

    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    assert len(package_info.jobs["failed_jobs"]) == 0
    # we have 4 parquet and 4 reference jobs plus one merge job
    num_jobs = 4 + 4
    num_sql_jobs = 0
    if destination_config.supports_merge:
        num_sql_jobs += 1
        # sql job is used to copy parquet to Athena Iceberg table (_dlt_pipeline_state)
        # if destination_config.destination == "athena":
        #     num_sql_jobs += 1
    assert len(package_info.jobs["completed_jobs"]) == num_jobs + num_sql_jobs
    assert (
        len(
            [
                x
                for x in package_info.jobs["completed_jobs"]
                if x.job_file_info.file_format == "reference"
            ]
        )
        == 4
    )
    # pipeline state is loaded with preferred format, so allows (possibly) for two job formats
    caps = pipeline.destination.capabilities()
    # NOTE: preferred_staging_file_format goes first because here we test staged loading and
    # default caps will be modified so preferred_staging_file_format is used as main
    preferred_format = caps.preferred_staging_file_format or caps.preferred_loader_file_format
    assert (
        len(
            [
                x
                for x in package_info.jobs["completed_jobs"]
                if x.job_file_info.file_format in (destination_config.file_format, preferred_format)
            ]
        )
        == 4
    )
    if destination_config.supports_merge:
        assert (
            len(
                [
                    x
                    for x in package_info.jobs["completed_jobs"]
                    if x.job_file_info.file_format == "sql"
                ]
            )
            == num_sql_jobs
        )

    initial_counts = load_table_counts(pipeline)
    assert initial_counts["issues"] == 100

    # check item of first row in db
    with pipeline.sql_client() as sql_client:
        qual_name = sql_client.make_qualified_table_name
        if destination_config.destination_type in ["mssql", "synapse"]:
            rows = sql_client.execute_sql(
                f"SELECT TOP 1 url FROM {qual_name('issues')} WHERE id = 388089021"
            )
        else:
            rows = sql_client.execute_sql(
                f"SELECT url FROM {qual_name('issues')} WHERE id = 388089021 LIMIT 1"
            )
        assert rows[0][0] == "https://api.github.com/repos/duckdb/duckdb/issues/71"

    if destination_config.supports_merge:
        # test merging in some changed values
        info = pipeline.run(load_modified_issues, **destination_config.run_kwargs)
        assert_load_info(info)
        assert pipeline.default_schema.tables["issues"]["write_disposition"] == "merge"
        merge_counts = load_table_counts(pipeline)
        assert merge_counts == initial_counts

        # check changes where merged in
        with pipeline.sql_client() as sql_client:
            if destination_config.destination_type in ["mssql", "synapse"]:
                qual_name = sql_client.make_qualified_table_name
                rows_1 = sql_client.execute_sql(
                    f"SELECT TOP 1 number FROM {qual_name('issues')} WHERE id = 1232152492"
                )
                rows_2 = sql_client.execute_sql(
                    f"SELECT TOP 1 number FROM {qual_name('issues')} WHERE id = 1142699354"
                )
            else:
                rows_1 = sql_client.execute_sql(
                    f"SELECT number FROM {qual_name('issues')} WHERE id = 1232152492 LIMIT 1"
                )
                rows_2 = sql_client.execute_sql(
                    f"SELECT number FROM {qual_name('issues')} WHERE id = 1142699354 LIMIT 1"
                )
            assert rows_1[0][0] == 105
            assert rows_2[0][0] == 300

    # test append
    info = pipeline.run(
        github().load_issues,
        write_disposition="append",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    append_counts = load_table_counts(pipeline)
    assert {k: v * 2 for k, v in initial_counts.items()} == append_counts

    # test replace
    info = pipeline.run(
        github().load_issues,
        write_disposition="replace",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert pipeline.default_schema.tables["issues"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    replace_counts = load_table_counts(pipeline)
    assert replace_counts == initial_counts


@pytest.mark.parametrize(
    "destination_config", destinations_configs(all_staging_configs=True), ids=lambda x: x.name
)
def test_truncate_staging_dataset(destination_config: DestinationTestConfiguration) -> None:
    """This test checks if tables truncation on staging destination done according to the configuration.

    Test loads data to the destination three times:
    * with truncation
    * without truncation (after this 2 staging files should be left)
    * with truncation (after this 1 staging file should be left)
    """
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_stage_loading", dataset_name="test_staging_load" + uniq_id()
    )
    # pipeline.destination.config_params["table_location_layout"] = "{dataset_name}/{table_name}_{location_tag}"
    resource = event_many_load_2()
    table_name: str = resource.table_name  # type: ignore[assignment]

    # load the data, files stay on the stage after the load
    info = pipeline.run(
        resource,
        **destination_config.run_kwargs,
    )
    assert_load_info(info)

    # load the data without truncating of the staging, should see two files on staging
    pipeline.destination.config_params["truncate_tables_on_staging_destination_before_load"] = False
    info = pipeline.run(
        resource,
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # check there are two staging files
    _, staging_client = pipeline._get_destination_clients(pipeline.default_schema)
    with staging_client:
        # except Athena + Iceberg which stores Iceberg data and metadata
        if (
            destination_config.destination_type == "athena"
            and destination_config.table_format == "iceberg"
        ):
            table_count = 9
            # but keeps them in staging dataset on staging destination - but only the last one
            with staging_client.with_staging_dataset():  # type: ignore[attr-defined]
                assert len(staging_client.list_table_files(table_name)) == 1  # type: ignore[attr-defined]
        else:
            table_count = 2
        assert len(staging_client.list_table_files(table_name)) == table_count  # type: ignore[attr-defined]

    # load the data with truncating, so only new file is on the staging
    pipeline.destination.config_params["truncate_tables_on_staging_destination_before_load"] = True
    info = pipeline.run(
        resource,
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # check that table exists in the destination
    with pipeline.sql_client() as sql_client:
        qual_name = sql_client.make_qualified_table_name
        assert len(sql_client.execute_sql(f"SELECT * from {qual_name(table_name)}")) > 4
    # check there is only one staging file
    _, staging_client = pipeline._get_destination_clients(pipeline.default_schema)
    with staging_client:
        # except for Athena which does not delete staging destination tables
        if destination_config.destination_type == "athena":
            if destination_config.table_format == "iceberg":
                # even more iceberg metadata
                table_count = 13
            else:
                table_count = 3
        else:
            table_count = 1
        assert len(staging_client.list_table_files(table_name)) == table_count  # type: ignore[attr-defined]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_staging_configs=True), ids=lambda x: x.name
)
def test_all_data_types(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "test_stage_loading", dataset_name="test_all_data_types" + uniq_id()
    )

    column_schemas, data_types = table_update_and_row_for_destination(destination_config)

    # apply the exact columns definitions so we process nested and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="merge", columns=column_schemas)
    def my_resource():
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source():
        return my_resource

    info = pipeline.run(
        my_source(),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)

    with pipeline.sql_client() as sql_client:
        qual_name = sql_client.make_qualified_table_name
        db_rows = sql_client.execute_sql(f"SELECT * FROM {qual_name('data_types')}")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # parquet is not really good at inserting json, best we get are strings in JSON columns
        parse_json_strings = (
            destination_config.file_format == "parquet"
            and destination_config.destination_type in ["redshift", "bigquery", "snowflake"]
        )
        allow_base64_binary = (
            destination_config.file_format == "jsonl"
            and destination_config.destination_type in ["redshift", "clickhouse"]
        )
        allow_string_binary = (
            destination_config.file_format == "parquet"
            and destination_config.destination_type in ["clickhouse"]
        )
        # content must equal
        assert_all_data_types_row(
            sql_client.capabilities,
            db_row[:-2],
            parse_json_strings=parse_json_strings,
            allow_base64_binary=allow_base64_binary,
            allow_string_binary=allow_string_binary,
            schema=column_schemas,
        )
