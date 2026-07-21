from typing import Any, List
import os
import pytest
import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.utils import uniq_id
from dlt.common.typing import DictStrAny
from dlt.common.schema.utils import is_nested_table
from dlt.common.pipeline import pipeline_state as current_pipeline_state, TRefreshMode

from dlt.destinations.sql_client import DBApiCursor
from dlt.extract.source import DltSource
from dlt.extract.resource import DltResource
from dlt.extract.state import resource_state
from dlt.pipeline.state_sync import load_pipeline_state_from_destination

from tests.pipeline.utils import (
    assert_load_info,
    assert_empty_tables,
    load_table_counts,
    load_tables_to_dicts,
    assert_only_table_columns,
    table_exists,
)
from tests.load.utils import destinations_configs, DestinationTestConfiguration


# destinations that exercise the full truncate/drop table-chain logic (sql + filesystem + athena)
refresh_chain_destinations = pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        subset=["duckdb", "filesystem", "athena"],
    ),
    ids=lambda x: x.name,
)


def assert_source_state_is_wiped(state: DictStrAny) -> None:
    # Keys contains only "resources" or is empty
    assert list(state.keys()) == ["resources"] or not state
    for value in state["resources"].values():
        assert not value


def column_values(cursor: DBApiCursor, column_name: str) -> List[Any]:
    """Return all values in a column from a cursor"""
    idx = [c[0] for c in cursor.native_cursor.description].index(column_name)
    return [row[idx] for row in cursor.fetchall()]


@dlt.source
def refresh_source(first_run: bool = True, drop_sources: bool = False):
    @dlt.resource(table_name="SomeDataOne")
    def some_data_one():
        if first_run:
            # Set some source and resource state
            dlt.current.source_state()["source_key_1"] = "source_value_1"
            resource_state("some_data_one")["run1_1"] = "value1_1"
            resource_state("some_data_one")["run1_2"] = "value1_2"
            yield {"ItemId": 1, "FullName": "John"}
            yield {"ItemId": 2, "FullName": "Jane"}
        else:
            # Check state is cleared for this resource
            assert not resource_state("some_data_one")
            if drop_sources:
                assert_source_state_is_wiped(dlt.current.source_state())
            # Second dataset without name column to test tables are re-created
            yield {"ItemId": 3}
            yield {"ItemId": 4}

    @dlt.resource(table_name="SomeDataTwo")
    def some_data_two():
        if first_run:
            dlt.current.source_state()["source_key_2"] = "source_value_2"
            resource_state("some_data_two")["run1_1"] = "value1_1"
            resource_state("some_data_two")["run1_2"] = "value1_2"
            yield {"ItemId": 5, "FullName": "Joe"}
            yield {"ItemId": 6, "FullName": "Jill"}
        else:
            assert not resource_state("some_data_two")
            if drop_sources:
                assert_source_state_is_wiped(dlt.current.source_state())
            yield {"ItemId": 7}
            yield {"ItemId": 8}

    @dlt.resource(table_name="SomeDataThree", primary_key="ItemId", write_disposition="merge")
    def some_data_three():
        if first_run:
            dlt.current.source_state()["source_key_3"] = "source_value_3"
            resource_state("some_data_three")["run1_1"] = "value1_1"
            yield {"ItemId": 9, "FullName": "Jack"}
            yield {"ItemId": 10, "FullName": "Jill"}
        else:
            assert not resource_state("some_data_three")
            if drop_sources:
                assert_source_state_is_wiped(dlt.current.source_state())
            yield {"ItemId": 11}
            yield {"ItemId": 12}

    @dlt.resource(table_name="SomeDataFour")
    def some_data_four():
        yield []

    yield some_data_one
    yield some_data_two
    yield some_data_three
    yield some_data_four


@refresh_chain_destinations
@pytest.mark.parametrize("in_source", (True, False))
@pytest.mark.parametrize("with_wipe", (True, False))
def test_refresh_drop_sources(
    destination_config: DestinationTestConfiguration, in_source: bool, with_wipe: bool
):
    pipeline_name = "refresh_source"
    dataset_name = pipeline_name + uniq_id()
    pipeline = destination_config.setup_pipeline(pipeline_name, dataset_name=dataset_name)

    data: Any = refresh_source(first_run=True, drop_sources=True)
    if not in_source:
        data = list(data.selected_resources.values())

    # first run pipeline so destination so tables are created
    info = pipeline.run(data, refresh="drop_sources", **destination_config.run_kwargs)
    assert_load_info(info)
    assert table_exists(pipeline, "some_data_three")

    # second run of pipeline with only selected resources
    if with_wipe:
        pipeline._wipe_working_folder()
        pipeline = destination_config.setup_pipeline(pipeline_name, dataset_name=dataset_name)

    data = refresh_source(first_run=False, drop_sources=True).with_resources(
        "some_data_one", "some_data_two"
    )
    if not in_source:
        data = list(data.selected_resources.values())

    info = pipeline.run(
        data,
        refresh="drop_sources",
        **destination_config.run_kwargs,
    )
    # the load package exposes the refresh mode and the tables dropped at the destination
    package = info.load_packages[0]
    assert package.refresh == "drop_sources"
    assert {"some_data_one", "some_data_two", "some_data_three"} <= set(package.dropped_tables)

    assert set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)) == {
        "some_data_one",
        "some_data_two",
    }

    # no "name" column should exist as table was dropped and re-created without it
    assert_only_table_columns(pipeline, "some_data_one", ["item_id"])
    data = load_tables_to_dicts(pipeline, "some_data_one")["some_data_one"]
    result = sorted([row["item_id"] for row in data])
    # only rows from second run should exist
    assert result == [3, 4]

    # confirm resource tables not selected on second run got dropped
    assert not table_exists(pipeline, "some_data_three")
    # loaded state is wiped
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    assert_source_state_is_wiped(destination_state["sources"]["refresh_source"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        subset=["duckdb", "filesystem"],
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_existing_schema_hash(destination_config: DestinationTestConfiguration):
    """Test when new schema is identical to a previously stored schema after dropping and re-creating tables.
    The change should be detected regardless and tables are created again in destination db
    """
    pipeline = destination_config.setup_pipeline(
        "refresh_full_test", refresh="drop_sources", dev_mode=True
    )

    info = pipeline.run(
        refresh_source(first_run=True, drop_sources=True), **destination_config.run_kwargs
    )
    assert_load_info(info)
    first_schema_hash = pipeline.default_schema.version_hash

    # Second run with all tables dropped and only some tables re-created
    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=True).with_resources(
            "some_data_one", "some_data_two"
        ),
        **destination_config.run_kwargs,
    )

    # Just check the local schema
    new_table_names = set(
        t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)
    )
    assert new_table_names == {"some_data_one", "some_data_two"}

    # Run again with all tables to ensure they are re-created
    # The new schema in this case should match the schema of the first run exactly
    info = pipeline.run(
        refresh_source(first_run=True, drop_sources=True), **destination_config.run_kwargs
    )
    # Check table 3 was re-created
    data = load_tables_to_dicts(pipeline, "some_data_three")["some_data_three"]
    result = sorted([(row["item_id"], row["full_name"]) for row in data])
    assert result == [(9, "Jack"), (10, "Jill")]

    # Schema is identical to first schema
    new_schema_hash = pipeline.default_schema.version_hash
    assert new_schema_hash == first_schema_hash


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        subset=["duckdb", "filesystem", "iceberg", "athena"],
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("in_source", (True, False))
@pytest.mark.parametrize("with_wipe", (True, False))
def test_refresh_drop_resources(
    destination_config: DestinationTestConfiguration, in_source: bool, with_wipe: bool
):
    if destination_config not in ["duckdb", "filesystem", "iceberg"] and in_source and with_wipe:
        pytest.skip("not needed")

    # First run pipeline with load to destination so tables are created
    pipeline_name = "refresh_source"
    dataset_name = pipeline_name + uniq_id()
    pipeline = destination_config.setup_pipeline(pipeline_name, dataset_name=dataset_name)

    data: Any = refresh_source(first_run=True)
    if not in_source:
        data = list(data.selected_resources.values())

    info = pipeline.run(data, **destination_config.run_kwargs)
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    if with_wipe:
        pipeline._wipe_working_folder()
        pipeline = destination_config.setup_pipeline(pipeline_name, dataset_name=dataset_name)

    data = refresh_source(first_run=False).with_resources("some_data_one", "some_data_two")
    if not in_source:
        data = list(data.selected_resources.values())

    info = pipeline.run(
        data,
        refresh="drop_resources",
        **destination_config.run_kwargs,
    )

    # Confirm resource tables not selected on second run are untouched
    data = load_tables_to_dicts(pipeline, "some_data_three")["some_data_three"]
    result = sorted([(row["item_id"], row["full_name"]) for row in data])
    assert result == [(9, "Jack"), (10, "Jill")]

    # Check the columns to ensure the name column was dropped
    assert_only_table_columns(pipeline, "some_data_one", ["item_id"])
    data = load_tables_to_dicts(pipeline, "some_data_one")["some_data_one"]
    # Only second run data
    result = sorted([row["item_id"] for row in data])
    assert result == [3, 4]

    # Loaded state contains only keys created in second run
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )

    source_state = destination_state["sources"]["refresh_source"]
    # Source level state is kept
    assert source_state["source_key_1"] == "source_value_1"
    assert source_state["source_key_2"] == "source_value_2"
    assert source_state["source_key_3"] == "source_value_3"
    # Only resource excluded in second run remains
    assert source_state["resources"]["some_data_three"] == {"run1_1": "value1_1"}
    assert not source_state["resources"]["some_data_two"]
    assert not source_state["resources"]["some_data_one"]


@refresh_chain_destinations
def test_refresh_drop_data_only(destination_config: DestinationTestConfiguration):
    """Refresh drop_data should truncate all selected tables before load"""
    # First run pipeline with load to destination so tables are created
    pipeline = destination_config.setup_pipeline(
        "refresh_full_test", refresh="drop_data", dev_mode=True
    )

    info = pipeline.run(
        refresh_source(first_run=True), write_disposition="append", **destination_config.run_kwargs
    )
    assert_load_info(info)

    first_schema_hash = pipeline.default_schema.version_hash

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_one", "some_data_two"),
        write_disposition="append",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # the load package exposes the refresh mode and the tables truncated at the destination
    package = info.load_packages[0]
    assert package.refresh == "drop_data"
    assert set(package.truncated_tables) == {"some_data_one", "some_data_two"}
    assert not package.dropped_tables

    # Schema should not be mutated
    assert pipeline.default_schema.version_hash == first_schema_hash

    # Tables selected in second run are truncated and should only have data from second run
    data = load_tables_to_dicts(pipeline, "some_data_one", "some_data_two", "some_data_three")
    # name column still remains when table was truncated instead of dropped
    # (except on filesystem where truncate and drop are the same)
    if destination_config.destination_type == "filesystem":
        result = sorted([row["item_id"] for row in data["some_data_one"]])
        assert result == [3, 4]

        result = sorted([row["item_id"] for row in data["some_data_two"]])
        assert result == [7, 8]
    else:
        result = sorted([(row["item_id"], row["full_name"]) for row in data["some_data_one"]])
        assert result == [(3, None), (4, None)]

        result = sorted([(row["item_id"], row["full_name"]) for row in data["some_data_two"]])
        assert result == [(7, None), (8, None)]

    # Other tables still have data from first run
    result = sorted([(row["item_id"], row["full_name"]) for row in data["some_data_three"]])
    assert result == [(9, "Jack"), (10, "Jill")]

    # State of selected resources is wiped, source level state is kept
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )

    source_state = destination_state["sources"]["refresh_source"]
    assert source_state["source_key_1"] == "source_value_1"
    assert source_state["source_key_2"] == "source_value_2"
    assert source_state["source_key_3"] == "source_value_3"
    assert not source_state["resources"]["some_data_one"]
    assert not source_state["resources"]["some_data_two"]
    assert source_state["resources"]["some_data_three"] == {"run1_1": "value1_1"}


@refresh_chain_destinations
def test_refresh_drop_sources_multiple_sources(destination_config: DestinationTestConfiguration):
    """
    Ensure only state and tables for currently selected source is dropped
    """

    @dlt.source
    def refresh_source_2(first_run=True):
        @dlt.resource
        def source_2_data_1():
            pipeline_state, _ = current_pipeline_state(pipeline._container)
            if first_run:
                dlt.current.source_state()["source_2_key_1"] = "source_2_value_1"
                resource_state("source_2_data_1")["run1_1"] = "value1_1"
                yield {"product": "apple", "price": 1}
                yield {"product": "banana", "price": 2}
            else:
                # First source should not have state wiped
                assert (
                    pipeline_state["sources"]["refresh_source"]["source_key_1"] == "source_value_1"
                )
                assert pipeline_state["sources"]["refresh_source"]["resources"][
                    "some_data_one"
                ] == {
                    "run1_1": "value1_1",
                    "run1_2": "value1_2",
                }
                # Source state is wiped
                assert_source_state_is_wiped(dlt.current.source_state())
                yield {"product": "orange"}
                yield {"product": "pear"}

        @dlt.resource
        def source_2_data_2():
            if first_run:
                dlt.current.source_state()["source_2_key_2"] = "source_2_value_2"
                resource_state("source_2_data_2")["run1_1"] = "value1_1"
                yield {"product": "carrot", "price": 3}
                yield {"product": "potato", "price": 4}
            else:
                assert_source_state_is_wiped(dlt.current.source_state())
                yield {"product": "cabbage"}
                yield {"product": "lettuce"}

        yield source_2_data_1
        yield source_2_data_2

    pipeline = destination_config.setup_pipeline(
        "refresh_full_test", refresh="drop_sources", dev_mode=True
    )

    # Run both sources
    info = pipeline.run(
        [refresh_source(first_run=True, drop_sources=True), refresh_source_2(first_run=True)],
        **destination_config.run_kwargs,
    )
    assert_load_info(info, 2)
    info = pipeline.run(
        refresh_source_2(first_run=False).with_resources("source_2_data_1"),
        **destination_config.run_kwargs,
    )
    assert_load_info(info, 2)

    # Check source 1 schema still has all tables
    table_names = set(
        t["name"] for t in pipeline.schemas["refresh_source"].data_tables(include_incomplete=True)
    )
    assert table_names == {"some_data_one", "some_data_two", "some_data_three", "some_data_four"}

    # Source 2 has only the selected tables
    table_names = set(
        t["name"] for t in pipeline.schemas["refresh_source_2"].data_tables(include_incomplete=True)
    )
    assert table_names == {"source_2_data_1"}

    # Destination still has tables from source 1
    data = load_tables_to_dicts(pipeline, "some_data_one")
    result = sorted([(row["item_id"], row["full_name"]) for row in data["some_data_one"]])
    assert result == [(1, "John"), (2, "Jane")]

    # First table from source2 exists, with only first column
    data = load_tables_to_dicts(pipeline, "source_2_data_1", schema_name="refresh_source_2")
    assert_only_table_columns(
        pipeline, "source_2_data_1", ["product"], schema_name="refresh_source_2"
    )
    result = sorted([row["product"] for row in data["source_2_data_1"]])
    assert result == ["orange", "pear"]

    # Second table from source 2 is gone
    assert not table_exists(pipeline, "source_2_data_2", schema_name="refresh_source_2")


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
def test_refresh_argument_to_run(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test", dev_mode=True)

    info = pipeline.run(refresh_source(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)

    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_three"),
        **destination_config.run_kwargs,
        refresh="drop_sources",
    )
    assert_load_info(info)

    # Check local schema to confirm refresh was at all applied
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_three"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_two"),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)

    # Nothing is dropped
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_two", "some_data_three"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_refresh_argument_to_extract(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test", dev_mode=True)

    info = pipeline.run(refresh_source(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)

    pipeline.extract(
        refresh_source(first_run=False).with_resources("some_data_three"),
        table_format=destination_config.table_format,
        refresh="drop_sources",
    )

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    # All other data tables removed
    assert tables == {"some_data_three"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    pipeline.extract(
        refresh_source(first_run=False).with_resources("some_data_two"),
        table_format=destination_config.table_format,
    )

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    assert tables == {"some_data_two", "some_data_three"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_staging_configs=True, all_buckets_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
def test_refresh_staging_dataset(destination_config: DestinationTestConfiguration):
    data = [
        {"id": 1, "pop": 1},
        {"id": 2, "pop": 3},
        {"id": 2, "pop": 4},  # duplicate
    ]

    pipeline = destination_config.setup_pipeline("test_refresh_staging_dataset" + uniq_id())
    # this is Athena iceberg setting: use random table location
    pipeline.destination.config_params["table_location_layout"] = (
        "{dataset_name}/{table_name}_{location_tag}"
    )

    source = DltSource(
        dlt.Schema("data_x"),
        "data_section",
        [
            dlt.resource(data, name="data_1", primary_key="id", write_disposition="merge"),
            dlt.resource(data, name="data_2", primary_key="id", write_disposition="append"),
        ],
    )
    # create two tables so two tables need to be dropped
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)

    # make data so inserting on mangled tables is not possible
    data_i = [
        {"id": "A", "pop": 0.1},
        {"id": "B", "pop": 0.3},
        {"id": "A", "pop": 0.4},
    ]
    source_i = DltSource(
        dlt.Schema("data_x"),
        "data_section",
        [
            dlt.resource(data_i, name="data_1", primary_key="id", write_disposition="merge"),
            dlt.resource(data_i, name="data_2", primary_key="id", write_disposition="append"),
        ],
    )
    info = pipeline.run(source_i, refresh="drop_resources", **destination_config.run_kwargs)
    assert_load_info(info)

    # now replace the whole source and load different tables
    source_i = DltSource(
        dlt.Schema("data_x"),
        "data_section",
        [
            dlt.resource(data_i, name="data_1_v2", primary_key="id", write_disposition="merge"),
            dlt.resource(data_i, name="data_2_v2", primary_key="id", write_disposition="append"),
        ],
    )
    info = pipeline.run(source_i, refresh="drop_sources", **destination_config.run_kwargs)
    assert_load_info(info)

    # tables got dropped
    with pytest.raises(DestinationUndefinedEntity):
        load_table_counts(pipeline, "data_1", "data_2")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("refresh", ["drop_source", "drop_resource", "drop_data"])
def test_changing_write_disposition_with_refresh(
    destination_config: DestinationTestConfiguration, refresh: str
):
    """NOTE: this test simply tests whether truncating of tables and deleting schema versions will produce"""
    """errors on a non-existing dataset (it should not)"""
    pipeline = destination_config.setup_pipeline("test", dev_mode=True, refresh=refresh)
    pipeline.run(
        [1, 2, 3], table_name="items", write_disposition="append", **destination_config.run_kwargs
    )
    # `primary_key` is required by the `upsert` merge strategy
    pipeline.run(
        [1, 2, 3],
        table_name="items",
        write_disposition="merge",
        primary_key="value",
        **destination_config.run_kwargs,
    )


@dlt.source
def refresh_additional_cases(first_run: bool = True):
    """Resources spanning append/replace/merge dispositions that produce nested, dynamic
    (event-dispatch) and `with_table_name`-marked tables. On the refresh (second) run each resource
    yields data for only some of its tables, so the others receive no data."""

    @dlt.resource(
        name="parent",
        write_disposition="append",
        nested_hints={
            # a pseudo-root child (broken out by a primary key) whose write disposition differs
            # from the append root
            "tags": dlt.mark.make_nested_hints(primary_key="tid", write_disposition="replace")
        },
    )
    def parent():
        if first_run:
            dlt.current.source_state()["src_key"] = "src_value"
            resource_state("parent")["run1"] = "value1"
            yield {
                "id": 1,
                "name": "p1",
                "children": [{"cid": 11}, {"cid": 12}],
                "tags": [{"tid": 1}],
            }
        else:
            # root receives data, the child and pseudo-root tables receive none
            yield {"id": 2, "name": "p2", "children": [], "tags": []}

    @dlt.resource(
        name="events",
        table_name=lambda e: "event_" + e["kind"],
        write_disposition="replace",
        primary_key="id",
    )
    def events():
        if first_run:
            resource_state("events")["run1"] = "value1"
            yield from [{"id": 1, "kind": "a"}, {"id": 2, "kind": "b"}]
        else:
            # only event_a is dispatched, event_b receives no data
            yield {"id": 3, "kind": "a"}

    @dlt.resource(name="marked", write_disposition="merge", primary_key="id")
    def marked():
        if first_run:
            resource_state("marked")["run1"] = "value1"
            # non-normalized dispatch names exercise identifier normalization end to end through the
            # refresh path (they land in the schema as mark_x / mark_y / mark_variant)
            yield dlt.mark.with_table_name({"id": 1}, "MarkX")
            yield dlt.mark.with_table_name({"id": 2}, "MarkY")
            # a table variant whose write disposition differs from the merge root
            yield dlt.mark.with_hints(
                {"id": 4},
                dlt.mark.make_hints(table_name="MarkVariant", write_disposition="replace"),
                create_table_variant=True,
            )
        else:
            # only mark_x receives data; mark_y and mark_variant receive none
            yield dlt.mark.with_table_name({"id": 3}, "MarkX")

    yield parent
    yield events
    yield marked


@refresh_chain_destinations
@pytest.mark.parametrize("refresh", ["drop_data", "drop_resources"])
@pytest.mark.parametrize("pre_drop", [False, True], ids=["no_pre_drop", "pre_drop"])
def test_refresh_truncates_or_drops_additional_cases(
    destination_config: DestinationTestConfiguration, refresh: TRefreshMode, pre_drop: bool
):
    """Makes sure that all cases in `refresh_additional_cases` are fully dropped or truncated.
    `pre_drop` removes one of the table to check if refresh survives ie. tables removed by the user
    """
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    all_tables = [
        "parent",
        "parent__children",
        "parent__tags",
        "event_a",
        "event_b",
        "mark_x",
        "mark_y",
        "mark_variant",
    ]
    pipeline = destination_config.setup_pipeline("refresh_chain" + uniq_id(), dev_mode=True)

    info = pipeline.run(refresh_additional_cases(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(pipeline, *all_tables) == {
        "parent": 1,
        "parent__children": 2,
        "parent__tags": 1,
        "event_a": 1,
        "event_b": 1,
        "mark_x": 1,
        "mark_y": 1,
        "mark_variant": 1,
    }
    # a pseudo-root and a variant whose write disposition differs from their root
    tables = pipeline.default_schema.tables
    assert is_nested_table(tables["parent__tags"]) is False
    assert tables["parent"]["write_disposition"] == "append"
    assert tables["parent__tags"]["write_disposition"] == "replace"
    assert tables["mark_variant"]["write_disposition"] == "replace"

    if pre_drop:
        # drop tables out of band so refresh must survive truncating/dropping a missing table:
        # one that gets data on refresh (parent) and two that get none (root event_b, pseudo-root parent__tags)
        pre_dropped = ["parent", "event_b", "parent__tags"]
        with pipeline.destination_client() as client:
            client.drop_tables(*pre_dropped, delete_schema=False)  # type: ignore[attr-defined]
        for dropped in pre_dropped:
            assert not table_exists(pipeline, dropped)

    info = pipeline.run(
        refresh_additional_cases(first_run=False), refresh=refresh, **destination_config.run_kwargs
    )
    assert_load_info(info)

    # tables that received data on the refresh run hold ONLY the refresh-run rows
    assert load_table_counts(pipeline, "parent", "event_a", "mark_x") == {
        "parent": 1,
        "event_a": 1,
        "mark_x": 1,
    }
    assert sorted(row["id"] for row in load_tables_to_dicts(pipeline, "parent")["parent"]) == [2]

    # tables that received NO data are emptied (drop_data) or dropped (drop_resources) - including
    # the pseudo-root and the variant whose write disposition differs from their root
    no_data_tables = ["parent__children", "parent__tags", "event_b", "mark_y", "mark_variant"]
    assert_empty_tables(pipeline, *no_data_tables)
    # drop_data truncates (table kept), drop_resources drops it (filesystem cannot distinguish)
    if destination_config.destination_type != "filesystem":
        for table in no_data_tables:
            assert table_exists(pipeline, table) is (refresh == "drop_data")

    # resource state is wiped, source-level state is kept (same for both modes)
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    source_state = destination_state["sources"]["refresh_additional_cases"]
    assert source_state["src_key"] == "src_value"
    # resource state is wiped (the key may be removed entirely or reset to empty)
    assert not source_state["resources"].get("parent")
    assert not source_state["resources"].get("events")
    assert not source_state["resources"].get("marked")


@refresh_chain_destinations
@pytest.mark.parametrize("refresh", ["drop_data", "drop_resources"])
@pytest.mark.parametrize("restore_state", [True, False], ids=["sync_state", "no_sync_state"])
def test_refresh_truncates_or_drops_when_no_data(
    destination_config: DestinationTestConfiguration, refresh: TRefreshMode, restore_state: bool
):
    """When a refreshed resource yields no data at all, its tables (root and nested) must still be
    emptied (drop_data) or dropped (drop_resources)
    """
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    @dlt.resource(name="items", write_disposition="append")
    def items(emit: bool):
        if emit:
            yield {"id": 1, "children": [{"cid": 1}]}

    pipeline = destination_config.setup_pipeline("refresh_no_data" + uniq_id(), dev_mode=True)
    pipeline.config.restore_from_destination = restore_state
    pipeline.run(items(True), **destination_config.run_kwargs)
    assert load_table_counts(pipeline, "items", "items__children") == {
        "items": 1,
        "items__children": 1,
    }

    info = pipeline.run(items(False), refresh=refresh, **destination_config.run_kwargs)
    # a load package must be generated even though the resource yields no data
    assert_load_info(info)
    assert_empty_tables(pipeline, "items", "items__children")
    # drop_data truncates (table kept, emptied), drop_resources drops it (filesystem cannot
    # distinguish the two - both remove the data files)
    if destination_config.destination_type != "filesystem":
        for table in ("items", "items__children"):
            assert table_exists(pipeline, table) is (refresh == "drop_data")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_drop_resources_incremental_empty_package_drops_all(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Makes sure that emitting empty package (no jobs) drops all resources"""
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    def items_resource(initial_value: int, end_value: int) -> DltResource:
        @dlt.resource(name="items", write_disposition="replace", primary_key="id")
        def items(
            updated: Any = dlt.sources.incremental(
                "ts", initial_value=initial_value, end_value=end_value
            )
        ) -> Any:
            yield [
                {"id": 1, "ts": 1, "children": [{"cid": 1}]},
                {"id": 2, "ts": 2, "children": [{"cid": 2}]},
            ]

        return items()

    pipeline = destination_config.setup_pipeline("refresh_inc_empty" + uniq_id(), dev_mode=True)

    # first refresh loads all data (ts within the [0, 10) window)
    info = pipeline.run(
        items_resource(0, 10), refresh="drop_resources", **destination_config.run_kwargs
    )
    assert_load_info(info)
    assert load_table_counts(pipeline, "items", "items__children") == {
        "items": 2,
        "items__children": 2,
    }
    # the hint-only primary key was typed from the data
    assert "primary_key" in pipeline.default_schema.tables["items"]["columns"]["id"]

    # second refresh: the [100, 200) window selects nothing -> fully empty package
    info = pipeline.run(
        items_resource(100, 200), refresh="drop_resources", **destination_config.run_kwargs
    )
    assert_load_info(info)
    # the empty package still reports the refresh mode and the dropped table chain
    package = info.load_packages[0]
    assert package.refresh == "drop_resources"
    assert set(package.dropped_tables) == {"items", "items__children"}
    # all tables are dropped and not recreated by the empty/replace run
    assert table_exists(pipeline, "items") is False
    assert table_exists(pipeline, "items__children") is False
