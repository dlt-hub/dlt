from unittest import mock
from typing import Sequence, Any, List

import pytest
import dlt
from dlt.common.pipeline import resource_state
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import DBApiCursor
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.pipeline.state_sync import load_pipeline_state_from_destination
from dlt.common.typing import DictStrAny
from dlt.common.pipeline import pipeline_state as current_pipeline_state

from tests.utils import clean_test_storage, preserve_environ
from tests.pipeline.utils import assert_load_info


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
def refresh_source(first_run: bool = True, drop_dataset: bool = False):
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
            if drop_dataset:
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
            if drop_dataset:
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
            if drop_dataset:
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


def test_refresh_drop_dataset():

    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_dataset",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(refresh_source(first_run=True, drop_dataset=True))
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    info = pipeline.run(
        refresh_source(first_run=False, drop_dataset=True).with_resources(
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


def test_existing_schema_hash():
    """Test when new schema is identical to a previously stored schema after dropping and re-creating tables.
    The change should be detected regardless and tables are created again in destination db
    """
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_dataset",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(refresh_source(first_run=True, drop_dataset=True))
    assert_load_info(info)
    first_schema_hash = pipeline.default_schema.version_hash

    # Second run with all tables dropped and only some tables re-created
    info = pipeline.run(
        refresh_source(first_run=False, drop_dataset=True).with_resources(
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
    info = pipeline.run(refresh_source(first_run=True, drop_dataset=True))
    # Check table 3 was re-created
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id, name FROM some_data_3 ORDER BY id")
        assert result == [(9, "Jack"), (10, "Jill")]

    # Schema is identical to first schema
    new_schema_hash = pipeline.default_schema.version_hash
    assert new_schema_hash == first_schema_hash


def test_refresh_drop_tables():
    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_tables",
        dataset_name="refresh_full_test",
    )

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


def test_refresh_drop_data_only():
    """Refresh drop_data should truncate all selected tables before load"""
    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_data",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(refresh_source(first_run=True), write_disposition="append")
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    # Mock wrap sql client to capture all queries executed
    with mock.patch.object(
        DuckDbSqlClient, "execute_query", side_effect=DuckDbSqlClient.execute_query, autospec=True
    ) as mock_execute_query:
        info = pipeline.run(
            refresh_source(first_run=False).with_resources("some_data_1", "some_data_2"),
            write_disposition="append",
        )

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


def test_refresh_drop_dataset_multiple_sources():
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

    pipeline = dlt.pipeline(
        "refresh_full_test_2",
        destination="duckdb",
        refresh="drop_dataset",
        dataset_name="refresh_full_test",
    )

    # Run both sources
    info = pipeline.run(
        [refresh_source(first_run=True, drop_dataset=True), refresh_source_2(first_run=True)]
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
