from unittest import mock
from typing import Sequence, Any, List

import pytest
import dlt
from dlt.common.pipeline import resource_state
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import DBApiCursor
from dlt.pipeline.state_sync import load_pipeline_state_from_destination
from dlt.common.typing import DictStrAny
from dlt.common.pipeline import pipeline_state as current_pipeline_state

from tests.utils import clean_test_storage, preserve_environ
from tests.pipeline.utils import assert_load_info
from tests.load.utils import destinations_configs, DestinationTestConfiguration


def assert_source_state_is_wiped(state: DictStrAny) -> None:
    # Keys contains only "resources" or is empty
    assert list(state.keys()) == ["resources"] or not state
    for value in state["resources"].values():
        assert not value


def assert_only_table_columns(cursor: DBApiCursor, expected_columns: Sequence[str]) -> None:
    """Table has all and only the expected columns (excluding _dlt columns)"""
    # Ignore _dlt columns
    columns = [c[0] for c in cursor.native_cursor.description if not c[0].startswith("_")]
    assert set(columns) == set(expected_columns)


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

    @dlt.resource
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
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_drop_sources(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_sources")

    # First run pipeline so destination so tables are created
    info = pipeline.run(refresh_source(first_run=True, drop_sources=True))
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=True).with_resources(
            "some_data_1", "some_data_2"
        )
    )

    assert set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)) == {
        "some_data_1",
        "some_data_2",
        # Table has never seen data and is not dropped
        "some_data_4",
    }

    # Confirm resource tables not selected on second run got dropped
    with pytest.raises(DatabaseUndefinedRelation):
        with pipeline.sql_client() as client:
            result = client.execute_sql("SELECT * FROM some_data_3")

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM some_data_1 ORDER BY id") as cursor:
            # No "name" column should exist as table was dropped and re-created without it
            assert_only_table_columns(cursor, ["id"])
            result = column_values(cursor, "id")

            # Only rows from second run should exist
            assert result == [3, 4]

    # Loaded state is wiped
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    assert_source_state_is_wiped(destination_state["sources"]["refresh_source"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_existing_schema_hash(destination_config: DestinationTestConfiguration):
    """Test when new schema is identical to a previously stored schema after dropping and re-creating tables.
    The change should be detected regardless and tables are created again in destination db
    """
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_sources")

    info = pipeline.run(refresh_source(first_run=True, drop_sources=True))
    assert_load_info(info)
    first_schema_hash = pipeline.default_schema.version_hash

    # Second run with all tables dropped and only some tables re-created
    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=True).with_resources(
            "some_data_1", "some_data_2"
        )
    )

    # Just check the local schema
    new_table_names = set(
        t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True)
    )
    assert new_table_names == {"some_data_1", "some_data_2", "some_data_4"}

    # Run again with all tables to ensure they are re-created
    # The new schema in this case should match the schema of the first run exactly
    info = pipeline.run(refresh_source(first_run=True, drop_sources=True))
    # Check table 3 was re-created
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_3 ORDER BY id")
        assert result == [(9, "Jack"), (10, "Jill")]

    # Schema is identical to first schema
    new_schema_hash = pipeline.default_schema.version_hash
    assert new_schema_hash == first_schema_hash


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_drop_tables(destination_config: DestinationTestConfiguration):
    # First run pipeline with load to destination so tables are created
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_tables")

    info = pipeline.run(refresh_source(first_run=True))
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_1", "some_data_2")
    )

    # Confirm resource tables not selected on second run are untouched
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_3 ORDER BY id")
    assert result == [(9,), (10,)]

    with pipeline.sql_client() as client:
        # Check the columns to ensure the name column was dropped
        with client.execute_query("SELECT * FROM some_data_1 ORDER BY id") as cursor:
            columns = [c[0] for c in cursor.native_cursor.description]
            assert "id" in columns
            # Second run data contains no "name" column. Table was dropped and re-created so it should not exist
            assert "name" not in columns
            id_idx = columns.index("id")
            result = [row[id_idx] for row in cursor.fetchall()]

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
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_drop_data_only(destination_config: DestinationTestConfiguration):
    """Refresh drop_data should truncate all selected tables before load"""
    # First run pipeline with load to destination so tables are created
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_data")

    info = pipeline.run(refresh_source(first_run=True), write_disposition="append")
    assert_load_info(info)

    first_schema_hash = pipeline.default_schema.version_hash

    # Second run of pipeline with only selected resources
    # Mock wrap sql client to capture all queries executed
    from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

    with mock.patch.object(
        DuckDbSqlClient, "execute_query", side_effect=DuckDbSqlClient.execute_query, autospec=True
    ) as mock_execute_query:
        info = pipeline.run(
            refresh_source(first_run=False).with_resources("some_data_1", "some_data_2"),
            write_disposition="append",
        )

    assert_load_info(info)

    # Schema should not be mutated
    assert pipeline.default_schema.version_hash == first_schema_hash

    all_queries = [k[0][1] for k in mock_execute_query.call_args_list]
    assert all_queries
    for q in all_queries:
        assert "drop table" not in q.lower()  # Tables are only truncated, never dropped

    # Tables selected in second run are truncated and should only have data from second run
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_2 ORDER BY id")
        # name column still remains when table was truncated instead of dropped
        assert result == [(7, None), (8, None)]

    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_1 ORDER BY id")
        assert result == [(3, None), (4, None)]

    # Other tables still have data from first run
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_3 ORDER BY id")
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
        [refresh_source(first_run=True, drop_sources=True), refresh_source_2(first_run=True)]
    )
    assert_load_info(info, 2)
    # breakpoint()
    info = pipeline.run(refresh_source_2(first_run=False).with_resources("source_2_data_1"))
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
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_1 ORDER BY id")
        assert result == [(1, "John"), (2, "Jane")]

    # First table from source1 exists, with only first column
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM source_2_data_1 ORDER BY product") as cursor:
            assert_only_table_columns(cursor, ["product"])
            result = column_values(cursor, "product")
            assert result == ["orange", "pear"]

    # Second table from source 2 is gone
    with pytest.raises(DatabaseUndefinedRelation):
        with pipeline.sql_client() as client:
            result = client.execute_sql("SELECT * FROM source_2_data_2")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_argument_to_run(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test")

    info = pipeline.run(refresh_source(first_run=True))
    assert_load_info(info)

    info = pipeline.run(
        refresh_source(first_run=False).with_resources("some_data_3"),
        refresh="drop_sources",
    )
    assert_load_info(info)

    # Check local schema to confirm refresh was at all applied
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_3"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    info = pipeline.run(refresh_source(first_run=False).with_resources("some_data_2"))
    assert_load_info(info)

    # Nothing is dropped
    tables = set(t["name"] for t in pipeline.default_schema.data_tables())
    assert tables == {"some_data_2", "some_data_3"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_refresh_argument_to_extract(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test")

    info = pipeline.run(refresh_source(first_run=True))
    assert_load_info(info)

    pipeline.extract(
        refresh_source(first_run=False).with_resources("some_data_3"),
        refresh="drop_sources",
    )

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    # All other data tables removed
    assert tables == {"some_data_3", "some_data_4"}

    # Run again without refresh to confirm refresh option doesn't persist on pipeline
    pipeline.extract(refresh_source(first_run=False).with_resources("some_data_2"))

    tables = set(t["name"] for t in pipeline.default_schema.data_tables(include_incomplete=True))
    assert tables == {"some_data_2", "some_data_3", "some_data_4"}


@pytest.mark.parametrize(
    "destination_config", destinations_configs(local_filesystem_configs=True), ids=lambda x: x.name
)
def test_refresh_drop_sources_local_filesystem(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("refresh_full_test", refresh="drop_data")

    info = pipeline.run(refresh_source(first_run=True, drop_sources=False))
    assert_load_info(info)
    load_1_id = info.loads_ids[0]

    info = pipeline.run(
        refresh_source(first_run=False, drop_sources=False).with_resources(
            "some_data_1", "some_data_2"
        )
    )
    assert_load_info(info)
    load_2_id = info.loads_ids[0]

    client = pipeline._fs_client()

    # Only contains files from load 2
    file_names = client.list_table_files("some_data_1")
    assert len(file_names) == 1
    assert load_2_id in file_names[0]

    # Only contains files from load 2
    file_names = client.list_table_files("some_data_2")
    assert len(file_names) == 1
    assert load_2_id in file_names[0]

    # Nothing dropped, only file from load 1
    file_names = client.list_table_files("some_data_3")
    assert len(file_names) == 1
    assert load_1_id in file_names[0]
