from typing import Dict
import dlt, os, pytest
from dlt.common.utils import uniq_id
from pytest_mock import MockerFixture

from dlt.common.schema.typing import REPLACE_STRATEGIES, TLoaderReplaceStrategy

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

    increase_state_loads = lambda info: len(
        [
            job
            for job in info.load_packages[0].jobs["completed_jobs"]
            if job.job_file_info.table_name == "_dlt_pipeline_state"
            and job.job_file_info.file_format not in ["sql", "reference"]
        ]
    )

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
        nonlocal offset
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
        nonlocal offset
        for _, index in enumerate(range(offset, offset + 12), 1):
            yield {
                "id": index,
                "name": f"item {index}",
            }

    # first run with offset 0
    info = pipeline.run([load_items, append_items], **destination_config.run_kwargs)
    assert_load_info(info)
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
        "_dlt_pipeline_state": state_records,
        "_dlt_loads": dlt_loads,
        "_dlt_version": dlt_versions,
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
        "_dlt_pipeline_state": 1,
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
        "_dlt_pipeline_state": state_records + 1,
        "_dlt_loads": dlt_loads,
        "_dlt_version": dlt_versions + 1,
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
        "_dlt_pipeline_state": 1,
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

    pipeline = destination_config.setup_pipeline("insert_from_staging_test", dev_mode=True)
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
            assert insert_sql_generator_spy.call_count == 1

    elif replace_strategy == "staging-optimized":
        if dest_type in ["postgres", "mssql"]:
            assert destination_spy.call_count == 1
        else:
            assert clone_sql_generator_spy.call_count == 1
            assert insert_sql_generator_spy.call_count == 0
