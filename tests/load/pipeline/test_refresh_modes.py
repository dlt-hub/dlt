from typing import Any, List

import pytest
import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.pipeline import resource_state
from dlt.common.utils import uniq_id
from dlt.common.typing import DictStrAny
from dlt.common.pipeline import pipeline_state as current_pipeline_state

from dlt.destinations.sql_client import DBApiCursor
from dlt.extract.source import DltSource
from dlt.pipeline.state_sync import load_pipeline_state_from_destination

from tests.utils import clean_test_storage
from tests.pipeline.utils import (
    _is_filesystem,
    assert_load_info,
    load_table_counts,
    load_tables_to_dicts,
    assert_only_table_columns,
    table_exists,
)
from tests.load.utils import destinations_configs, DestinationTestConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


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
    @dlt.resource
    def some_data_1():
        if first_run:
            # Set some source and resource state
            dlt.state()["source_key_1"] = "source_value_1"
            resource_state("some_data_1")["run1_1"] = "value1_1"
            resource_state("some_data_1")["run1_2"] = "value1_2"
            yield {"id": 1, "name": "John"}
            yield {"id": 2, "name": "Jane"}
        else:
            # Check state is cleared for this resource
            assert not resource_state("some_data_1")
            if drop_sources:
                assert_source_state_is_wiped(dlt.state())
            # Second dataset without name column to test tables are re-created
            yield {"id": 3}
            yield {"id": 4}

    @dlt.resource
    def some_data_2():
        if first_run:
            dlt.state()["source_key_2"] = "source_value_2"
            resource_state("some_data_2")["run1_1"] = "value1_1"
            resource_state("some_data_2")["run1_2"] = "value1_2"
            yield {"id": 5, "name": "Joe"}
            yield {"id": 6, "name": "Jill"}
        else:
            assert not resource_state("some_data_2")
            if drop_sources:
                assert_source_state_is_wiped(dlt.state())
            yield {"id": 7}
            yield {"id": 8}

    @dlt.resource(primary_key="id", write_disposition="merge")
    def some_data_3():
        if first_run:
            dlt.state()["source_key_3"] = "source_value_3"
            resource_state("some_data_3")["run1_1"] = "value1_1"
            yield {"id": 9, "name": "Jack"}
            yield {"id": 10, "name": "Jill"}
        else:
            assert not resource_state("some_data_3")
            if drop_sources:
                assert_source_state_is_wiped(dlt.state())
            yield {"id": 11}
            yield {"id": 12}

    @dlt.resource
    def some_data_4():
        yield []

    yield some_data_1
    yield some_data_2
    yield some_data_3
    yield some_data_4


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, subset=["duckdb", "filesystem"], local_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
def test_refresh_drop_sources(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_sources")

    # First run pipeline so destination so tables are created
    info = pipeline.run(
        refresh_source(first_run=True, drop_sources=True), **destination_config.run_kwargs
    )
    assert_load_info(info)
    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=True).with_resources(
            "some_data_1", "some_data_2"
        ),
        **destination_config.run_kwargs,
    )

    assert set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)) == {
        "some_data_1",
        "some_data_2",
    }

    # No "name" column should exist as table was dropped and re-created without it
    assert_only_table_columns(pipeline, "some_data_1", ["id"])
    data = load_tables_to_dicts(pipeline, "some_data_1")["some_data_1"]
    result = sorted([row["id"] for row in data])
    # Only rows from second run should exist
    assert result == [3, 4]

    # Confirm resource tables not selected on second run got dropped
    assert not table_exists(pipeline, "some_data_3")
    # Loaded state is wiped
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    assert_source_state_is_wiped(destination_state["sources"]["refresh_source"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_existing_schema_hash(destination_config: DestinationTestConfiguration):
    """Test when new schema is identical to a previously stored schema after dropping and re-creating tables.
    The change should be detected regardless and tables are created again in destination db
    """
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_sources")

    info = pipeline.run(
        refresh_source(first_run=True, drop_sources=True), **destination_config.run_kwargs
    )
    assert_load_info(info)
    first_schema_hash = pipeline.default_schema.version_hash

    # Second run with all tables dropped and only some tables re-created
    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=True).with_resources(
            "some_data_1", "some_data_2"
        ),
        **destination_config.run_kwargs,
    )

    # Just check the local schema
    new_table_names = set(
        t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)
    )
    assert new_table_names == {"some_data_1", "some_data_2"}

    # Run again with all tables to ensure they are re-created
    # The new schema in this case should match the schema of the first run exactly
    info = pipeline.run(
        refresh_source(first_run=True, drop_sources=True), **destination_config.run_kwargs
    )
    # Check table 3 was re-created
    data = load_tables_to_dicts(pipeline, "some_data_3")["some_data_3"]
    result = sorted([(row["id"], row["name"]) for row in data])
    assert result == [(9, "Jack"), (10, "Jill")]

    # Schema is identical to first schema
    new_schema_hash = pipeline.default_schema.version_hash
    assert new_schema_hash == first_schema_hash


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_refresh_drop_resources(destination_config: DestinationTestConfiguration):
    # First run pipeline with load to destination so tables are created
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_tables")

    info = pipeline.run(refresh_source(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_1", "some_data_2"),
        **destination_config.run_kwargs,
    )

    # Confirm resource tables not selected on second run are untouched
    data = load_tables_to_dicts(pipeline, "some_data_3")["some_data_3"]
    result = sorted([(row["id"], row["name"]) for row in data])
    assert result == [(9, "Jack"), (10, "Jill")]

    # Check the columns to ensure the name column was dropped
    assert_only_table_columns(pipeline, "some_data_1", ["id"])
    data = load_tables_to_dicts(pipeline, "some_data_1")["some_data_1"]
    # Only second run data
    result = sorted([row["id"] for row in data])
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
    assert source_state["resources"]["some_data_3"] == {"run1_1": "value1_1"}
    assert not source_state["resources"]["some_data_2"]
    assert not source_state["resources"]["some_data_1"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_refresh_drop_data_only(destination_config: DestinationTestConfiguration):
    """Refresh drop_data should truncate all selected tables before load"""
    # First run pipeline with load to destination so tables are created
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_data")

    info = pipeline.run(
        refresh_source(first_run=True), write_disposition="append", **destination_config.run_kwargs
    )
    assert_load_info(info)

    first_schema_hash = pipeline.default_schema.version_hash

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_1", "some_data_2"),
        write_disposition="append",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)

    # Schema should not be mutated
    assert pipeline.default_schema.version_hash == first_schema_hash

    # Tables selected in second run are truncated and should only have data from second run
    data = load_tables_to_dicts(pipeline, "some_data_1", "some_data_2", "some_data_3")
    # name column still remains when table was truncated instead of dropped
    # (except on filesystem where truncate and drop are the same)
    if destination_config.destination_type == "filesystem":
        result = sorted([row["id"] for row in data["some_data_1"]])
        assert result == [3, 4]

        result = sorted([row["id"] for row in data["some_data_2"]])
        assert result == [7, 8]
    else:
        result = sorted([(row["id"], row["name"]) for row in data["some_data_1"]])
        assert result == [(3, None), (4, None)]

        result = sorted([(row["id"], row["name"]) for row in data["some_data_2"]])
        assert result == [(7, None), (8, None)]

    # Other tables still have data from first run
    result = sorted([(row["id"], row["name"]) for row in data["some_data_3"]])
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
    assert not source_state["resources"]["some_data_1"]
    assert not source_state["resources"]["some_data_2"]
    assert source_state["resources"]["some_data_3"] == {"run1_1": "value1_1"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
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
                dlt.state()["source_2_key_1"] = "source_2_value_1"
                resource_state("source_2_data_1")["run1_1"] = "value1_1"
                yield {"product": "apple", "price": 1}
                yield {"product": "banana", "price": 2}
            else:
                # First source should not have state wiped
                assert (
                    pipeline_state["sources"]["refresh_source"]["source_key_1"] == "source_value_1"
                )
                assert pipeline_state["sources"]["refresh_source"]["resources"]["some_data_1"] == {
                    "run1_1": "value1_1",
                    "run1_2": "value1_2",
                }
                # Source state is wiped
                assert_source_state_is_wiped(dlt.state())
                yield {"product": "orange"}
                yield {"product": "pear"}

        @dlt.resource
        def source_2_data_2():
            if first_run:
                dlt.state()["source_2_key_2"] = "source_2_value_2"
                resource_state("source_2_data_2")["run1_1"] = "value1_1"
                yield {"product": "carrot", "price": 3}
                yield {"product": "potato", "price": 4}
            else:
                assert_source_state_is_wiped(dlt.state())
                yield {"product": "cabbage"}
                yield {"product": "lettuce"}

        yield source_2_data_1
        yield source_2_data_2

    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_sources")

    # Run both sources
    info = pipeline.run(
        [refresh_source(first_run=True, drop_sources=True), refresh_source_2(first_run=True)],
        **destination_config.run_kwargs,
    )
    assert_load_info(info, 2)
    # breakpoint()
    info = pipeline.run(
        refresh_source_2(first_run=False).with_resources("source_2_data_1"),
        **destination_config.run_kwargs,
    )
    assert_load_info(info, 2)

    # Check source 1 schema still has all tables
    table_names = set(
        t["name"] for t in pipeline.schemas["refresh_source"].data_tables(include_incomplete=True)
    )
    assert table_names == {"some_data_1", "some_data_2", "some_data_3", "some_data_4"}

    # Source 2 has only the selected tables
    table_names = set(
        t["name"] for t in pipeline.schemas["refresh_source_2"].data_tables(include_incomplete=True)
    )
    assert table_names == {"source_2_data_1"}

    # Destination still has tables from source 1
    data = load_tables_to_dicts(pipeline, "some_data_1")
    result = sorted([(row["id"], row["name"]) for row in data["some_data_1"]])
    assert result == [(1, "John"), (2, "Jane")]

    # # First table from source2 exists, with only first column
    data = load_tables_to_dicts(pipeline, "source_2_data_1", schema_name="refresh_source_2")
    assert_only_table_columns(
        pipeline, "source_2_data_1", ["product"], schema_name="refresh_source_2"
    )
    result = sorted([row["product"] for row in data["source_2_data_1"]])
    assert result == ["orange", "pear"]

    # # Second table from source 2 is gone
    assert not table_exists(pipeline, "source_2_data_2", schema_name="refresh_source_2")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_refresh_argument_to_run(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test")

    info = pipeline.run(refresh_source(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)

    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_3"),
        **destination_config.run_kwargs,
        refresh="drop_sources",
    )
    assert_load_info(info)

    # Check local schema to confirm refresh was at all applied
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_3"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_2"),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)

    # Nothing is dropped
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_2", "some_data_3"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, local_filesystem_configs=True, subset=["duckdb", "filesystem"]
    ),
    ids=lambda x: x.name,
)
def test_refresh_argument_to_extract(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test")

    info = pipeline.run(refresh_source(first_run=True), **destination_config.run_kwargs)
    assert_load_info(info)

    pipeline.extract(
        refresh_source(first_run=False).with_resources("some_data_3"),
        table_format=destination_config.table_format,
        refresh="drop_sources",
    )

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    # All other data tables removed
    assert tables == {"some_data_3"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    pipeline.extract(
        refresh_source(first_run=False).with_resources("some_data_2"),
        table_format=destination_config.table_format,
    )

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    assert tables == {"some_data_2", "some_data_3"}


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
    if _is_filesystem(pipeline):
        assert load_table_counts(pipeline, "data_1", "data_2") == {}
    else:
        with pytest.raises(DestinationUndefinedEntity):
            load_table_counts(pipeline, "data_1", "data_2")
    load_table_counts(pipeline, "data_1_v2", "data_1_v2")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_staging_configs=True, all_buckets_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("refresh", ["drop_source", "drop_resource", "drop_data"])
def test_changing_write_disposition_with_refresh(
    destination_config: DestinationTestConfiguration, refresh: str
):
    """NOTE: this test simply tests wether truncating of tables and deleting schema versions will produce"""
    """errors on a non-existing dataset (it should not)"""
    pipeline = destination_config.setup_pipeline("test", dev_mode=True, refresh=refresh)
    pipeline.run([1, 2, 3], table_name="items", write_disposition="append")
    pipeline.run([1, 2, 3], table_name="items", write_disposition="merge")
