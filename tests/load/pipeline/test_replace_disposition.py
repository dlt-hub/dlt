from typing import Any, Dict
import dlt, os, pytest
from dlt.common.utils import uniq_id
from pytest_mock import MockerFixture

from dlt.common.schema.typing import REPLACE_STRATEGIES, TLoaderReplaceStrategy
from dlt.destinations.sql_jobs import SqlStagingReplaceFollowupJob
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.pipeline.utils import (
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
    assert_empty_tables,
)
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)
from tests.load.pipeline.utils import skip_if_unsupported_replace_strategy


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(local_filesystem_configs=True, default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_disposition(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
) -> None:
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)

    # only allow 40 items per file
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "40"
    # use staging tables for replace
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy
    # share the same database across many pipelines in this test
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = "duckdb:///test_replace_disposition.duckdb"
    # os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__CATALOG"] = (
    #     "sqlite:///test_replace_disposition.db"
    # )

    # filesystem does not have child tables, prepend defaults
    # def norm_table_counts(counts: Dict[str, int], *child_tables: str) -> Dict[str, int]:
    #     return {**{t: 0 for t in child_tables}, **counts}

    dataset_name = "test_replace_strategies_ds" + uniq_id()
    pipeline = destination_config.setup_pipeline(
        "test_replace_strategies", dataset_name=dataset_name
    )

    offset = 1000

    # keep merge key with unknown column to test replace SQL generator
    @dlt.resource(
        name="items",
        write_disposition="replace",
        primary_key="id",
        table_format=destination_config.table_format,
    )
    def load_items():
        # will produce 3 jobs for the main table with 40 items each
        # 6 jobs for the sub_items
        # 3 jobs for the sub_sub_items
        for _, index in enumerate(range(offset, offset + 120), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "sub_items": [
                    {"id": index + 1000, "name": f"sub item {index + 1000}"},
                    {
                        "id": index + 2000,
                        "name": f"sub item {index + 2000}",
                        "sub_sub_items": [
                            {
                                "id": index + 3000,
                                "name": f"sub item {index + 3000}",
                            }
                        ],
                    },
                ],
            }

    # append resource to see if we do not drop any tables
    @dlt.resource(write_disposition="append", table_format=destination_config.table_format)
    def append_items():
        for _, index in enumerate(range(offset, offset + 12), 1):
            yield {
                "id": index,
                "name": f"item {index}",
            }

    # first run with offset 0
    info = pipeline.run([load_items, append_items], **destination_config.run_kwargs)
    assert_load_info(info)

    # get dlt table names
    state_table_name = pipeline.default_schema.state_table_name
    loads_table_name = pipeline.default_schema.loads_table_name
    version_table_name = pipeline.default_schema.version_table_name

    # function to count state load jobs
    increase_state_loads = lambda info: len(
        [
            job
            for job in info.load_packages[0].jobs["completed_jobs"]
            if job.job_file_info.table_name == state_table_name
            and job.job_file_info.file_format not in ["sql", "reference"]
        ]
    )

    # count state records that got extracted
    state_records = increase_state_loads(info)
    dlt_loads: int = 1
    dlt_versions: int = 1

    # second run with higher offset so we can check the results
    offset = 1000
    info = pipeline.run([load_items, append_items], **destination_config.run_kwargs)
    assert_load_info(info)
    state_records += increase_state_loads(info)
    dlt_loads += 1

    # we should have all items loaded
    table_counts = load_table_counts(pipeline, *pipeline.default_schema.tables.keys())
    assert table_counts == {
        "append_items": 24,  # loaded twice
        "items": 120,
        "items__sub_items": 240,
        "items__sub_items__sub_sub_items": 120,
        state_table_name: state_records,
        loads_table_name: dlt_loads,
        version_table_name: dlt_versions,
    }

    # check trace
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "append_items": 12,
        "items": 120,
        "items__sub_items": 240,
        "items__sub_items__sub_sub_items": 120,
    }

    # check we really have the replaced data in our destination
    table_dicts = load_tables_to_dicts(pipeline)
    assert {x for i, x in enumerate(range(1000, 1120), 1)} == {
        int(x["id"]) for x in table_dicts["items"]
    }
    assert {x for i, x in enumerate(range(2000, 2000 + 120), 1)}.union(
        {x for i, x in enumerate(range(3000, 3000 + 120), 1)}
    ) == {int(x["id"]) for x in table_dicts["items__sub_items"]}
    assert {x for i, x in enumerate(range(4000, 4120), 1)} == {
        int(x["id"]) for x in table_dicts["items__sub_items__sub_sub_items"]
    }

    # we need to test that destination tables including child tables are cleared if we do not yield anything
    @dlt.resource(name="items", write_disposition="replace", primary_key="id")
    def load_items_none():
        # do not yield even once
        if False:
            yield

    info = pipeline.run([load_items_none, append_items], **destination_config.run_kwargs)
    assert_load_info(info)
    state_records += increase_state_loads(info)
    dlt_loads += 1

    # table and child tables should be cleared
    table_counts = load_table_counts(pipeline, "append_items")
    assert table_counts == {
        "append_items": 36,
    }
    assert_empty_tables(pipeline, "items", "items__sub_items", "items__sub_items__sub_sub_items")
    # check trace
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "append_items": 12,
        "items": 0,
    }

    # create a pipeline with different name but loading to the same dataset as above - this is to provoke truncating non existing tables
    pipeline_2 = destination_config.setup_pipeline(
        "test_replace_strategies_2", dataset_name=dataset_name
    )
    info = pipeline_2.run(load_items, table_name="items_copy", **destination_config.run_kwargs)
    assert_load_info(info)
    new_state_records = increase_state_loads(info)
    assert new_state_records == 1
    dlt_loads += 1
    dlt_versions += 1
    # check trace
    assert pipeline_2.last_trace.last_normalize_info.row_counts == {
        "items_copy": 120,
        "items_copy__sub_items": 240,
        "items_copy__sub_items__sub_sub_items": 120,
        state_table_name: 1,
    }

    info = pipeline_2.run(append_items, **destination_config.run_kwargs)
    assert_load_info(info)
    new_state_records = increase_state_loads(info)
    assert new_state_records == 0
    dlt_loads += 1

    # new pipeline
    table_counts = load_table_counts(pipeline_2, *pipeline_2.default_schema.tables.keys())
    assert table_counts == {
        "append_items": 48,
        "items_copy": 120,
        "items_copy__sub_items": 240,
        "items_copy__sub_items__sub_sub_items": 120,
        state_table_name: state_records + 1,
        loads_table_name: dlt_loads,
        version_table_name: dlt_versions + 1,
    }
    # check trace
    assert pipeline_2.last_trace.last_normalize_info.row_counts == {
        "append_items": 12,
    }

    # old pipeline -> shares completed loads and versions table
    table_counts = load_table_counts(pipeline, "append_items")
    assert table_counts == {
        "append_items": 48,
    }
    assert_empty_tables(pipeline, "items", "items__sub_items", "items__sub_items__sub_sub_items")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(local_filesystem_configs=True, default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_table_clearing(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
) -> None:
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)

    # use staging tables for replace
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy

    pipeline = destination_config.setup_pipeline(
        "test_replace_table_clearing", dataset_name="test_replace_table_clearing", dev_mode=True
    )

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_with_subitems():
        data = {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_without_subitems():
        data = [{"id": 1, "name": "item", "sub_items": []}]
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def items_with_subitems_yield_none():
        yield None
        yield None
        data = [
            {
                "id": 1,
                "name": "item",
                "sub_items": [
                    {"id": 101, "name": "sub item 101"},
                    {"id": 101, "name": "sub item 102"},
                ],
            }
        ]
        yield dlt.mark.with_table_name(data, "items")
        yield dlt.mark.with_table_name(data, "other_items")
        yield None

    # this resource only gets loaded once, and should remain populated regardless of the loads to the other tables
    @dlt.resource(name="static_items", write_disposition="replace", primary_key="id")
    def static_items():
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def yield_none():
        yield

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def no_yield():
        # this will not yield even once
        if False:
            yield

    @dlt.resource(name="main_resource", write_disposition="replace", primary_key="id")
    def yield_empty_list():
        yield []

    # regular call
    pipeline.run([items_with_subitems, static_items], **destination_config.run_kwargs)
    assert load_table_counts(pipeline) == {
        "items": 1,
        "items__sub_items": 2,
        "other_items": 1,
        "other_items__sub_items": 2,
        "static_items": 1,
        "static_items__sub_items": 2,
    }

    # check trace
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "items": 1,
        "items__sub_items": 2,
        "other_items": 1,
        "other_items__sub_items": 2,
        "static_items": 1,
        "static_items__sub_items": 2,
        pipeline.default_schema.state_table_name: 1,
    }

    # see if child table gets cleared
    pipeline.run(items_without_subitems, **destination_config.run_kwargs)
    assert load_table_counts(
        pipeline, "items", "other_items", "static_items", "static_items__sub_items"
    ) == {
        "items": 1,
        "other_items": 1,
        "static_items": 1,
        "static_items__sub_items": 2,
    }
    assert_empty_tables(pipeline, "items__sub_items", "other_items__sub_items")
    # check trace
    assert pipeline.last_trace.last_normalize_info.row_counts == {"items": 1, "other_items": 1}

    # see if yield none clears everything
    for empty_resource in [yield_none, no_yield, yield_empty_list]:
        pipeline.run(items_with_subitems, **destination_config.run_kwargs)
        pipeline.run(empty_resource, **destination_config.run_kwargs)
        assert load_table_counts(pipeline, "static_items", "static_items__sub_items") == {
            "static_items": 1,
            "static_items__sub_items": 2,
        }
        assert_empty_tables(pipeline, "items", "other_items", "other_items__sub_items")
        # check trace
        assert pipeline.last_trace.last_normalize_info.row_counts == {"items": 0, "other_items": 0}

    # see if yielding something next to other none entries still goes into db
    pipeline.run(items_with_subitems_yield_none, **destination_config.run_kwargs)
    assert load_table_counts(pipeline) == {
        "items": 1,
        "items__sub_items": 2,
        "other_items": 1,
        "other_items__sub_items": 2,
        "static_items": 1,
        "static_items__sub_items": 2,
    }
    # check trace
    assert pipeline.last_trace.last_normalize_info.row_counts == {
        "items": 1,
        "items__sub_items": 2,
        "other_items": 1,
        "other_items__sub_items": 2,
    }


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        default_staging_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_sql_queries(
    destination_config: DestinationTestConfiguration,
    replace_strategy: TLoaderReplaceStrategy,
    mocker: MockerFixture,
) -> None:
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)

    from dlt.destinations.sql_jobs import SqlStagingFollowupJob, SqlStagingReplaceFollowupJob

    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy

    clone_sql_generator_spy = mocker.spy(SqlStagingReplaceFollowupJob, "_generate_clone_sql")
    insert_sql_generator_spy = mocker.spy(SqlStagingFollowupJob, "_generate_insert_sql")

    dest_type = destination_config.destination_type
    destination_spy = None

    if dest_type == "sqlalchemy":
        from dlt.destinations.impl.sqlalchemy.load_jobs import SqlalchemyReplaceJob

        destination_spy = mocker.spy(SqlalchemyReplaceJob, "generate_sql")

    elif dest_type == "postgres":
        from dlt.destinations.impl.postgres.postgres import PostgresStagingReplaceJob

        destination_spy = mocker.spy(PostgresStagingReplaceJob, "generate_sql")

    elif dest_type == "mssql":
        from dlt.destinations.impl.mssql.mssql import MsSqlStagingReplaceJob

        destination_spy = mocker.spy(MsSqlStagingReplaceJob, "generate_sql")

    elif dest_type == "clickhouse":
        from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseStagingReplaceJob

        destination_spy = mocker.spy(ClickHouseStagingReplaceJob, "generate_sql")

    pipeline = destination_config.setup_pipeline(
        f"insert_from_staging_test_{uniq_id()}", dev_mode=True
    )
    load_info = pipeline.run(
        [{"id": 1}],
        table_name="my_table",
        write_disposition="replace",
        **destination_config.run_kwargs,
    )
    assert_load_info(load_info)

    # make sure data got loaded
    assert len(pipeline.dataset().my_table.fetchall()) == 1

    if replace_strategy == "truncate-and-insert":
        if dest_type == "sqlalchemy":
            assert destination_spy.call_count == 0
        else:
            assert clone_sql_generator_spy.call_count == 0
            assert insert_sql_generator_spy.call_count == 0

    elif replace_strategy == "insert-from-staging":
        if dest_type == "sqlalchemy":
            assert destination_spy.call_count == 1
        else:
            assert clone_sql_generator_spy.call_count == 0
            assert insert_sql_generator_spy.call_count == (
                2 if destination_config.uses_table_format_for_state_table else 1
            )

    elif replace_strategy == "staging-optimized":
        if dest_type in ["postgres", "mssql", "clickhouse"]:
            assert destination_spy.call_count == 1
        else:
            assert clone_sql_generator_spy.call_count == 1
            assert insert_sql_generator_spy.call_count == 0


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["snowflake"],
    ),
    ids=lambda x: x.name,
)
def test_snowflake_atomic_swap_replace(
    destination_config: DestinationTestConfiguration,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test Snowflake atomic swap with sequential loads, nested tables, and empty resource."""
    from dlt.destinations.sql_jobs import SqlStagingFollowupJob, SqlStagingReplaceFollowupJob
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeStagingReplaceJob

    monkeypatch.setenv("DESTINATION__REPLACE_STRATEGY", "staging-optimized")
    monkeypatch.setenv("DESTINATION__SNOWFLAKE__ENABLE_ATOMIC_SWAP", "true")

    clone_spy = mocker.spy(SqlStagingReplaceFollowupJob, "_generate_clone_sql")
    insert_spy = mocker.spy(SqlStagingFollowupJob, "_generate_insert_sql")
    swap_spy = mocker.spy(SnowflakeStagingReplaceJob, "generate_sql")

    pipeline = destination_config.setup_pipeline("snowflake_atomic_swap_test", dev_mode=True)

    @dlt.resource(name="items", write_disposition="replace", primary_key="id")
    def load_items(offset):
        for i in range(offset, offset + 3):
            yield {
                "id": i,
                "name": f"item {i}",
                "sub_items": [
                    {"id": i * 100 + 1, "name": f"sub {i * 100 + 1}"},
                    {"id": i * 100 + 2, "name": f"sub {i * 100 + 2}"},
                ],
            }

    # first load: nested data via swap
    info = pipeline.run(load_items(0), **destination_config.run_kwargs)
    assert_load_info(info)

    assert swap_spy.call_count == 1
    assert clone_spy.call_count == 0
    assert insert_spy.call_count == 0
    for sql_stmt in swap_spy.return_value:
        assert "SWAP WITH" in sql_stmt

    assert load_table_counts(pipeline, "items", "items__sub_items") == {
        "items": 3,
        "items__sub_items": 6,
    }

    # second load: different data, verifies full replacement after prior swap
    swap_spy.reset_mock()
    info = pipeline.run(load_items(100), **destination_config.run_kwargs)
    assert_load_info(info)

    assert swap_spy.call_count == 1
    assert load_table_counts(pipeline, "items", "items__sub_items") == {
        "items": 3,
        "items__sub_items": 6,
    }
    table_dicts = load_tables_to_dicts(pipeline, "items")
    assert {int(r["id"]) for r in table_dicts["items"]} == {100, 101, 102}

    # third load: empty resource clears all tables
    @dlt.resource(name="items", write_disposition="replace", primary_key="id")
    def load_items_empty():
        if False:
            yield

    swap_spy.reset_mock()
    info = pipeline.run(load_items_empty(), **destination_config.run_kwargs)
    assert_load_info(info)
    assert_empty_tables(pipeline, "items", "items__sub_items")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        subset=["duckdb", "filesystem"],
    ),
    ids=lambda x: x.name,
)
def test_replace_chain_jobless_nested_tables(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Nested tables of a replace resource that receive no data in a run get no load job and
    must still be emptied."""
    pipeline = destination_config.setup_pipeline("replace_jobless", dev_mode=True)

    @dlt.resource(name="items", write_disposition="replace")
    def items(children: bool):
        if children:
            yield {"id": 1, "children": [{"cid": 1}, {"cid": 2}]}
        else:
            yield {"id": 2}

    info = pipeline.run(items(True), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(pipeline, "items", "items__children") == {
        "items": 1,
        "items__children": 2,
    }

    info = pipeline.run(items(False), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(pipeline, "items") == {"items": 1}
    assert_empty_tables(pipeline, "items__children")
    if destination_config.table_format == "delta":
        from dlt.common.libs.deltalake import get_delta_tables

        # delta truncation is transactional: the nested table is kept and empty
        dt = get_delta_tables(pipeline, "items__children")["items__children"]
        assert dt.to_pyarrow_table().num_rows == 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(table_format_local_configs=True, with_table_format="delta"),
    ids=lambda x: x.name,
)
def test_replace_chain_truncate_consistent_with_package_jobs(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    """Locks the contract the truncation skip in `initialize_storage` relies on: `verify_schema`
    receives exactly the new jobs stored in the load package, so tables with data jobs and replace
    disposition are left to their atomic overwrite while jobless chain tables are truncated.
    """
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

    verify_spy = mocker.spy(FilesystemClient, "verify_schema")
    truncate_spy = mocker.spy(FilesystemClient, "truncate_tables")

    pipeline = destination_config.setup_pipeline("replace_chain_contract", dev_mode=True)

    @dlt.resource(name="items", write_disposition="replace")
    def items(children: bool):
        if children:
            yield {"id": 1, "children": [{"cid": 1}, {"cid": 2}]}
        else:
            yield {"id": 2}

    info = pipeline.run(items(True), **destination_config.run_kwargs)
    assert_load_info(info)

    verify_spy.reset_mock()
    truncate_spy.reset_mock()
    info = pipeline.run(items(False), **destination_config.run_kwargs)
    assert_load_info(info)

    # verify_schema got the very same new jobs that the load step executed from the package
    assert verify_spy.call_count == 1
    spied_jobs = {job.job_id() for job in verify_spy.call_args.kwargs["new_jobs"]}
    package_jobs = {
        job.job_file_info.job_id()
        for jobs in pipeline.get_load_package_info(info.loads_ids[0]).jobs.values()
        for job in jobs
        # reference jobs are followups created while the package runs
        if job.job_file_info.file_format != "reference"
    }
    assert spied_jobs == package_jobs

    # the root replace table has a data job and is overwritten atomically, only the jobless
    # nested table is truncated
    assert truncate_spy.call_count == 1
    assert truncate_spy.call_args.args[1] == ["items__children"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb", "snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", ["insert-from-staging", "staging-optimized"])
def test_replace_strategy_switch_creates_staging_tables(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
) -> None:
    """switching to a staging replace strategy on an existing dataset must create
    the now-required staging tables even though the schema version hash is unchanged.
    """
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)

    pipeline = destination_config.setup_pipeline("replace_strategy_switch", dev_mode=True)

    @dlt.resource(write_disposition="replace")
    def items():
        yield [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

    # the merge table makes the first run create the staging dataset without the replace table
    @dlt.resource(write_disposition="merge", primary_key="id")
    def merge_items():
        yield [{"id": 1, "val": "m"}]

    # nested merge table under a replace root makes a partial staging chain: the nested table
    # loads to the staging dataset while the root does not until the strategy switch
    @dlt.resource(
        write_disposition="replace",
        primary_key="id",
        nested_hints={"list": dlt.mark.make_nested_hints(write_disposition="merge")},
    )
    def nested_items():
        yield [{"id": 1, "list": [1, 2, 3]}]

    os.environ["DESTINATION__REPLACE_STRATEGY"] = "truncate-and-insert"
    info = pipeline.run([items(), merge_items(), nested_items()], **destination_config.run_kwargs)
    assert_load_info(info)

    # same schema, replace tables now go through the staging dataset
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy
    info = pipeline.run([items(), merge_items(), nested_items()], **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(pipeline, "items", "merge_items", "nested_items") == {
        "items": 2,
        "merge_items": 1,
        "nested_items": 1,
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_replace_staging_empty_resource_aborted_package_keeps_data(
    destination_config: DestinationTestConfiguration, mocker: MockerFixture
) -> None:
    """Makes sure that insert-from-staging does not truncate upfront when no data on resource"""
    os.environ["DESTINATION__REPLACE_STRATEGY"] = "insert-from-staging"
    pipeline = destination_config.setup_pipeline("replace_empty_abort", dev_mode=True)

    @dlt.resource(write_disposition="replace")
    def items(rows: Any) -> Any:
        yield from rows

    info = pipeline.run(items([{"id": 1}, {"id": 2}]), **destination_config.run_kwargs)
    assert_load_info(info)

    # fail the replace followup so the package aborts after the 0-row job loaded to staging
    mocker.patch.object(
        SqlStagingReplaceFollowupJob, "generate_sql", side_effect=Exception("compute failed")
    )
    with pytest.raises(PipelineStepFailed):
        pipeline.run(items([]), **destination_config.run_kwargs)
    assert load_table_counts(pipeline, "items") == {"items": 2}

    # the pending package completes once the fault is gone and the table is replaced with no rows
    mocker.stopall()
    info = pipeline.run(items([]), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(pipeline, "items") == {"items": 0}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb", "snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_replace_refreshed_disposition_no_data(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
) -> None:
    """A table whose write disposition is refreshed to replace on a run without data for it is
    truncated under every replace strategy."""
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy

    pipeline = destination_config.setup_pipeline("replace_refresh_empty", dev_mode=True)

    @dlt.resource(name="events", table_name=lambda e: e["kind"], primary_key="id")
    def events(kinds: Any) -> Any:
        for idx, kind in enumerate(kinds):
            yield {"id": idx, "kind": kind}

    pipeline.run(events(["a", "b"]), write_disposition="merge", **destination_config.run_kwargs)
    assert load_table_counts(pipeline, "a", "b") == {"a": 1, "b": 1}
    assert pipeline.default_schema.tables["b"]["write_disposition"] == "merge"

    # switch to replace with data only for "a": "b" is refreshed to replace and truncated
    pipeline.run(events(["a"]), write_disposition="replace", **destination_config.run_kwargs)
    assert pipeline.default_schema.tables["b"]["write_disposition"] == "replace"
    assert load_table_counts(pipeline, "a", "b") == {"a": 1, "b": 0}
