import os
from typing import Iterator, Dict, Any, List
from unittest import mock
from itertools import chain

import pytest

import dlt
from dlt.common.destination.client import JobClientBase
from dlt.extract import DltResource
from dlt.common.utils import uniq_id
from dlt.pipeline import helpers, state_sync, Pipeline
from dlt.load import Load
from dlt.pipeline.exceptions import (
    PipelineHasPendingDataException,
    PipelineNeverRan,
    PipelineStepFailed,
)
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.load.utils import FILE_BUCKET, destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_table_counts

from dlt.destinations.exceptions import DatabaseUndefinedRelation


def _attach(pipeline: Pipeline) -> Pipeline:
    return dlt.attach(pipeline.pipeline_name, pipelines_dir=pipeline.pipelines_dir)


@dlt.source(section="droppable", name="droppable")
def droppable_source(drop_columns: bool = False) -> List[DltResource]:
    @dlt.resource
    def droppable_a(
        a: dlt.sources.incremental[int] = dlt.sources.incremental("a", 0, range_start="open")
    ) -> Iterator[Dict[str, Any]]:
        if drop_columns:
            yield dict(a=1, b=2)
            yield dict(a=4, b=23)
        else:
            yield dict(a=1, b=2, c=3)
            yield dict(a=4, b=23, c=24)

    @dlt.resource
    def droppable_b(
        asd: dlt.sources.incremental[int] = dlt.sources.incremental("asd", 0)
    ) -> Iterator[Dict[str, Any]]:
        # Child table
        yield dict(asd=2323, qe=555, items=[dict(m=1, n=2), dict(m=3, n=4)])

    @dlt.resource
    def droppable_c(
        qe: dlt.sources.incremental[int] = dlt.sources.incremental("qe"),
    ) -> Iterator[Dict[str, Any]]:
        # Grandchild table
        if drop_columns:
            # dropped asdasd, items[r], items.labels.value
            yield dict(qe=111, items=[dict(k=2, labels=[dict(name="abc"), dict(name="www")])])
        else:
            yield dict(
                asdasd=2424,
                qe=111,
                items=[
                    dict(k=2, r=2, labels=[dict(name="abc", value=1), dict(name="www", value=2)])
                ],
            )

    @dlt.resource
    def droppable_d(
        o: dlt.sources.incremental[int] = dlt.sources.incremental("o"),
    ) -> Iterator[List[Dict[str, Any]]]:
        dlt.current.source_state()["data_from_d"] = {"foo1": {"bar": 1}, "foo2": {"bar": 2}}
        yield [dict(o=55), dict(o=22)]

    @dlt.resource(selected=True)
    def droppable_no_state():
        yield [1, 2, 3]

    return [droppable_a(), droppable_b(), droppable_c(), droppable_d(), droppable_no_state]


RESOURCE_TABLES = dict(
    droppable_a=["droppable_a"],
    droppable_b=["droppable_b", "droppable_b__items"],
    droppable_c=["droppable_c", "droppable_c__items", "droppable_c__items__labels"],
    droppable_d=["droppable_d"],
    droppable_no_state=["droppable_no_state"],
)

NO_STATE_RESOURCES = {"droppable_no_state"}


def assert_dropped_resources(pipeline: Pipeline, resources: List[str]) -> None:
    assert_dropped_resource_tables(pipeline, resources)
    assert_dropped_resource_states(pipeline, resources)


def assert_dropped_resource_tables(pipeline: Pipeline, resources: List[str]) -> None:
    # Verify only requested resource tables are removed from pipeline schema
    all_tables = set(chain.from_iterable(RESOURCE_TABLES.values()))
    dropped_tables = set(chain.from_iterable(RESOURCE_TABLES[r] for r in resources))
    expected_tables = all_tables - dropped_tables
    result_tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert result_tables == expected_tables

    # Verify requested tables are dropped from destination
    client: SqlJobClientBase
    with pipeline.destination_client(pipeline.default_schema_name) as client:  # type: ignore[assignment]
        assert client.is_storage_initialized()
        # Check all tables supposed to be dropped are not in dataset
        storage_tables = list(client.get_storage_tables(dropped_tables))
        # no columns in all tables
        assert all(len(table[1]) == 0 for table in storage_tables)

        # Check tables not from dropped resources still exist
        storage_tables = list(client.get_storage_tables(expected_tables))
        # all tables have columns
        assert all(len(table[1]) > 0 for table in storage_tables)


def assert_dropped_resource_states(pipeline: Pipeline, resources: List[str]) -> None:
    # Verify only requested resource keys are removed from state
    all_resources = set(RESOURCE_TABLES.keys()) - NO_STATE_RESOURCES
    expected_keys = all_resources - set(resources)
    sources_state = pipeline.state["sources"]
    result_keys = set(sources_state["droppable"]["resources"].keys())
    assert result_keys == expected_keys


def assert_destination_state_loaded(pipeline: Pipeline) -> None:
    """Verify stored destination state matches the local pipeline state"""
    client: SqlJobClientBase
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        destination_state = state_sync.load_pipeline_state_from_destination(
            pipeline.pipeline_name, client
        )
        # current pipeline schema available in the destination
        client.get_stored_schema_by_hash(pipeline.default_schema.version_hash)
    pipeline_state = dict(pipeline.state)
    del pipeline_state["_local"]
    assert pipeline_state == destination_state


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("in_source", (True, False))
def test_drop_command_resources_and_state(
    destination_config: DestinationTestConfiguration, in_source: bool
) -> None:
    """Test the drop command with resource and state path options and
    verify correct data is deleted from destination and locally"""
    if destination_config.destination_type == "filesystem" and destination_config.table_format:
        pytest.skip(
            "Cannot run this test on filesystem with open tables enabled, dlt tables are not open"
            " tables"
        )
    source: Any = droppable_source()
    if not in_source:
        source = list(source.selected_resources.values())

    pipeline = destination_config.setup_pipeline("droppable", dev_mode=True)
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)

    all_tables = list(pipeline.default_schema.tables.keys())
    assert load_table_counts(pipeline, *all_tables) == {
        "_dlt_version": 1,
        "_dlt_loads": 1,
        "droppable_a": 2,
        "droppable_b": 1,
        "droppable_c": 1,
        "droppable_d": 2,
        "droppable_no_state": 3,
        "_dlt_pipeline_state": 1,
        "droppable_b__items": 2,
        "droppable_c__items": 1,
        "droppable_c__items__labels": 2,
    }

    attached = _attach(pipeline)
    helpers.drop(
        attached,
        resources=["droppable_c", "droppable_d", "droppable_no_state"],
        state_paths="data_from_d.*.bar",
    )

    attached = _attach(pipeline)

    assert_dropped_resources(attached, ["droppable_c", "droppable_d", "droppable_no_state"])

    # Verify extra json paths are removed from state
    sources_state = pipeline.state["sources"]
    assert sources_state["droppable"]["data_from_d"] == {"foo1": {}, "foo2": {}}

    assert_destination_state_loaded(pipeline)

    # now run the same droppable_source to see if tables are recreated and they contain right number of items
    source = droppable_source(drop_columns=True)
    if not in_source:
        source = list(source.selected_resources.values())
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)
    # 2 versions (one dropped and replaced with schema with dropped tables, then we added missing tables)
    # 3 loads (one for drop)
    # droppable_no_state correctly replaced
    # all other resources stay at the same count (they are incremental so they got loaded again or not loaded at all ie droppable_a)
    all_tables = list(pipeline.default_schema.tables.keys())
    assert load_table_counts(pipeline, *all_tables) == {
        "_dlt_version": 2,
        "_dlt_loads": 3,
        "droppable_a": 2,
        "droppable_b": 1,
        "_dlt_pipeline_state": 3,
        "droppable_b__items": 2,
        "droppable_c": 1,
        "droppable_d": 2,
        "droppable_no_state": 3,
        "droppable_c__items": 1,
        "droppable_c__items__labels": 2,
    }
    # check if columns got correctly dropped
    droppable_a_schema = pipeline.default_schema.get_table("droppable_a")
    # this table was not dropped so column still exists
    assert "c" in droppable_a_schema["columns"]
    # dropped asdasd, items[r], items.labels.value
    droppable_c_schema = pipeline.default_schema.get_table("droppable_c")
    assert "asdasd" not in droppable_c_schema["columns"]
    assert "qe" in droppable_c_schema["columns"]
    droppable_c_i_schema = pipeline.default_schema.get_table("droppable_c__items")
    assert "r" not in droppable_c_i_schema["columns"]
    assert "k" in droppable_c_i_schema["columns"]
    droppable_c_l_schema = pipeline.default_schema.get_table("droppable_c__items__labels")
    assert "value" not in droppable_c_l_schema["columns"]
    assert "name" in droppable_c_l_schema["columns"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_command_only_state(destination_config: DestinationTestConfiguration) -> None:
    """Test drop command that deletes part of the state and syncs with destination"""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline(
        "drop_test_" + uniq_id(),
    )
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)
    helpers.drop(attached, state_paths="data_from_d.*.bar", state_only=True)

    attached = _attach(pipeline)

    assert_dropped_resources(attached, [])

    # Verify extra json paths are removed from state
    sources_state = pipeline.state["sources"]
    assert sources_state["droppable"]["data_from_d"] == {"foo1": {}, "foo2": {}}

    assert_destination_state_loaded(pipeline)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_command_only_tables(destination_config: DestinationTestConfiguration) -> None:
    """Test drop only tables and makes sure that schema and state are synced"""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)
    sources_state = pipeline.state["sources"]

    attached = _attach(pipeline)
    helpers.drop(attached, resources=["droppable_no_state"])

    attached = _attach(pipeline)

    assert_dropped_resources(attached, ["droppable_no_state"])
    # source state didn't change
    assert pipeline.state["sources"] == sources_state

    assert_destination_state_loaded(pipeline)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_destination_tables_fails(destination_config: DestinationTestConfiguration) -> None:
    """Fail on DROP TABLES in destination init. Command runs again."""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)

    with mock.patch.object(
        pipeline.destination.client_class,
        "drop_tables",
        autospec=True,
        side_effect=RuntimeError("Oh no!"),
    ):
        with pytest.raises(PipelineStepFailed) as einfo:
            helpers.drop(attached, resources=("droppable_a", "droppable_b"))
        assert isinstance(einfo.value.exception, RuntimeError)
        assert "Oh no!" in str(einfo.value.exception)

    helpers.drop(attached, resources=("droppable_a", "droppable_b"))

    assert_dropped_resources(attached, ["droppable_a", "droppable_b"])
    assert_destination_state_loaded(attached)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_fail_after_drop_tables(destination_config: DestinationTestConfiguration) -> None:
    """Fail directly after drop tables. Command runs again ignoring destination tables missing."""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)

    # Fail on client update_stored_schema
    with mock.patch.object(
        pipeline.destination.client_class,
        "update_stored_schema",
        autospec=True,
        side_effect=RuntimeError("Oh no!"),
    ):
        with pytest.raises(PipelineStepFailed) as einfo:
            helpers.drop(attached, resources=("droppable_a", "droppable_b"))

        assert isinstance(einfo.value.exception, RuntimeError)
        assert "Oh no!" in str(einfo.value.exception)

    attached = _attach(pipeline)
    helpers.drop(attached, resources=("droppable_a", "droppable_b"))

    assert_dropped_resources(attached, ["droppable_a", "droppable_b"])
    assert_destination_state_loaded(attached)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_load_step_fails(destination_config: DestinationTestConfiguration) -> None:
    """Test idempotence. pipeline.load() fails. Command can be run again successfully"""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)

    with mock.patch.object(Load, "run", side_effect=RuntimeError("Something went wrong")):
        with pytest.raises(PipelineStepFailed) as e:
            helpers.drop(attached, resources=("droppable_a", "droppable_b"))
        assert isinstance(e.value.exception, RuntimeError)

    attached = _attach(pipeline)
    helpers.drop(attached, resources=("droppable_a", "droppable_b"))

    assert_dropped_resources(attached, ["droppable_a", "droppable_b"])
    assert_destination_state_loaded(attached)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_resource_regex(destination_config: DestinationTestConfiguration) -> None:
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)

    helpers.drop(attached, resources=["re:.+_b", "re:.+_a"])

    attached = _attach(pipeline)

    assert_dropped_resources(attached, ["droppable_a", "droppable_b"])
    assert_destination_state_loaded(attached)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_nothing(destination_config: DestinationTestConfiguration) -> None:
    """No resources, no state keys. Nothing is changed."""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)

    attached = _attach(pipeline)
    previous_state = dict(attached.state)

    helpers.drop(attached)

    assert_dropped_resources(attached, [])
    assert previous_state == attached.state


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, table_format_local_configs=True),
    ids=lambda x: x.name,
)
def test_drop_all_flag(destination_config: DestinationTestConfiguration) -> None:
    """Using drop_all flag. Destination dataset and all local state is deleted"""
    source = droppable_source()
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(source, **destination_config.run_kwargs)
    dlt_tables = [
        t["name"] for t in pipeline.default_schema.dlt_tables()
    ]  # Original _dlt tables to check for

    attached = _attach(pipeline)

    helpers.drop(attached, drop_all=True)

    attached = _attach(pipeline)

    assert_dropped_resources(attached, list(RESOURCE_TABLES))

    # Verify original _dlt tables were not deleted
    with attached._get_destination_clients(attached.default_schema)[0] as client:
        storage_tables = list(client.get_storage_tables(dlt_tables))  # type: ignore[attr-defined]
        assert all(len(table[1]) > 0 for table in storage_tables)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_run_pipeline_after_partial_drop(destination_config: DestinationTestConfiguration) -> None:
    """Pipeline can be run again after dropping some resources"""
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(droppable_source(), **destination_config.run_kwargs)

    attached = _attach(pipeline)

    helpers.drop(attached, resources="droppable_a")

    attached = _attach(pipeline)

    attached.extract(droppable_source())  # TODO: individual steps cause pipeline.run() never raises
    attached.normalize()
    attached.load()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_state_only(destination_config: DestinationTestConfiguration) -> None:
    """Pipeline can be run again after dropping some resources"""
    pipeline = destination_config.setup_pipeline("drop_test_" + uniq_id(), dev_mode=True)
    pipeline.run(droppable_source(), **destination_config.run_kwargs)

    attached = _attach(pipeline)

    helpers.drop(attached, resources=("droppable_a", "droppable_b"), state_only=True)

    attached = _attach(pipeline)

    assert_dropped_resource_tables(attached, [])  # No tables dropped
    assert_dropped_resource_states(attached, ["droppable_a", "droppable_b"])
    assert_destination_state_loaded(attached)


def test_drop_first_run_and_pending_packages() -> None:
    """Attempts to drop before pipeline runs and when partial loads happen"""
    pipeline = dlt.pipeline("drop_test_" + uniq_id(), destination="duckdb")
    with pytest.raises(PipelineNeverRan):
        helpers.drop(pipeline, "droppable_a")
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline.run(droppable_source().with_resources("droppable_a"))
    pipeline.extract(droppable_source().with_resources("droppable_b"))
    with pytest.raises(PipelineHasPendingDataException):
        helpers.drop(pipeline, "droppable_a")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_staging_tables(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"drop_staging_tables_{uniq_id()}", dev_mode=False)

    @dlt.resource(
        columns={"value": {"data_type": "bool"}}, primary_key="id", write_disposition="merge"
    )
    def some_data():
        yield {"id": 1, "value": True, "children": [{"id": 1}, {"id": 2}]}

    pipeline.run(some_data, **destination_config.run_kwargs)

    attached = _attach(pipeline)
    helpers.drop(attached, resources=["some_data"])

    # Make sure the "some_data" table doesn't exist anymore
    with attached.sql_client() as client:
        qual_table_name, qual_staging_table_name = client.get_qualified_table_names("some_data")
        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql(f"SELECT * FROM {qual_table_name}")

        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql(f"SELECT * FROM {qual_staging_table_name}")

        # Child table "some_data__children" should also be non-existent
        qual_table_name, qual_staging_table_name = client.get_qualified_table_names(
            "some_data__children"
        )
        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql(f"SELECT * FROM {qual_table_name}")

        with pytest.raises(DatabaseUndefinedRelation):
            client.execute_sql(f"SELECT * FROM {qual_staging_table_name}")

    # Change the schema and try to load to the old table
    # NOTE: we test this because previously staging tables weren't dropped and
    # resulted in schema mismatch errors
    @dlt.resource(
        table_name="some_data",
        columns={"value": {"data_type": "text"}},
        primary_key="id",
        write_disposition="merge",
    )
    def some_data_redefined():
        yield {"id": 1, "value": "random", "children": []}

    attached = _attach(pipeline)

    load_info = attached.run(some_data_redefined, **destination_config.run_kwargs)
    assert_load_info(load_info)
