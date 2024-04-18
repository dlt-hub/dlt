from unittest import mock

import pytest
import dlt
from dlt.common.pipeline import resource_state
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.pipeline.state_sync import load_pipeline_state_from_destination

from tests.utils import clean_test_storage, preserve_environ
from tests.pipeline.utils import assert_load_info


def test_refresh_drop_dataset():
    first_run = True

    @dlt.source
    def my_source():
        @dlt.resource
        def some_data_1():
            # Set some source and resource state
            if first_run:
                dlt.state()["source_key_1"] = "source_value_1"
                resource_state("some_data_1")["resource_key_1"] = "resource_value_1"
                resource_state("some_data_1")["resource_key_2"] = "resource_value_2"
            else:
                # State is cleared for all resources on second run
                assert not dlt.state()["resources"]
                assert "source_key_1" not in dlt.state()
                assert "source_key_2" not in dlt.state()
                assert "source_key_3" not in dlt.state()
                resource_state("some_data_1")["resource_key_3"] = "resource_value_3"
            yield {"id": 1, "name": "John"}
            yield {"id": 2, "name": "Jane"}

        @dlt.resource
        def some_data_2():
            if first_run:
                dlt.state()["source_key_2"] = "source_value_2"
                resource_state("some_data_2")["resource_key_3"] = "resource_value_3"
                resource_state("some_data_2")["resource_key_4"] = "resource_value_4"
            yield {"id": 3, "name": "Joe"}
            yield {"id": 4, "name": "Jill"}

        @dlt.resource
        def some_data_3():
            if first_run:
                dlt.state()["source_key_3"] = "source_value_3"
                resource_state("some_data_3")["resource_key_5"] = "resource_value_5"
            yield {"id": 5, "name": "Jack"}
            yield {"id": 6, "name": "Jill"}

        return [some_data_1, some_data_2, some_data_3]

    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_dataset",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(my_source())
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    first_run = False
    info = pipeline.run(my_source().with_resources("some_data_1", "some_data_2"))
    # pipeline.extract(my_source().with_resources("some_data_1", "some_data_2"))
    # pipeline.normalize()
    # pipeline.load()

    # Confirm resource tables not selected on second run got wiped
    with pytest.raises(DatabaseUndefinedRelation):
        with pipeline.sql_client() as client:
            result = client.execute_sql("SELECT * FROM some_data_3")

    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_1 ORDER BY id")
    assert result == [(1,), (2,)]

    # Loaded state contains only keys created in second run
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    assert destination_state["sources"]["my_source"]["resources"] == {
        "some_data_1": {"resource_key_3": "resource_value_3"},
    }


def test_refresh_drop_tables():
    first_run = True

    @dlt.source
    def my_source():
        @dlt.resource
        def some_data_1():
            # Set some source and resource state
            if first_run:
                dlt.state()["source_key_1"] = "source_value_1"
                resource_state("some_data_1")["resource_key_1"] = "resource_value_1"
                resource_state("some_data_1")["resource_key_2"] = "resource_value_2"
            else:
                # State is cleared for all resources on second run
                assert "source_key_1" in dlt.state()
                assert "source_key_2" in dlt.state()
                assert "source_key_3" in dlt.state()
                # Resource 3 is not wiped
                assert dlt.state()["resources"] == {
                    "some_data_3": {"resource_key_5": "resource_value_5"}
                }
            yield {"id": 1, "name": "John"}
            yield {"id": 2, "name": "Jane"}

        @dlt.resource
        def some_data_2():
            if first_run:
                dlt.state()["source_key_2"] = "source_value_2"
                resource_state("some_data_2")["resource_key_3"] = "resource_value_3"
                resource_state("some_data_2")["resource_key_4"] = "resource_value_4"
            else:
                resource_state("some_data_2")["resource_key_6"] = "resource_value_6"
            yield {"id": 3, "name": "Joe"}
            yield {"id": 4, "name": "Jill"}

        @dlt.resource
        def some_data_3():
            if first_run:
                dlt.state()["source_key_3"] = "source_value_3"
                resource_state("some_data_3")["resource_key_5"] = "resource_value_5"
            yield {"id": 5, "name": "Jack"}
            yield {"id": 6, "name": "Jill"}

        return [some_data_1, some_data_2, some_data_3]

    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_tables",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(my_source())
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    first_run = False
    info = pipeline.run(my_source().with_resources("some_data_1", "some_data_2"))

    # Confirm resource tables not selected on second run got wiped
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_3 ORDER BY id")
    assert result == [(5,), (6,)]

    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_1 ORDER BY id")
    assert result == [(1,), (2,)]

    # Loaded state contains only keys created in second run
    with pipeline.destination_client() as dest_client:
        destination_state = load_pipeline_state_from_destination(
            pipeline.pipeline_name, dest_client  # type: ignore[arg-type]
        )
    assert destination_state["sources"]["my_source"]["resources"] == {
        "some_data_2": {"resource_key_6": "resource_value_6"},
        "some_data_3": {"resource_key_5": "resource_value_5"},
    }


def test_refresh_drop_data_only():
    """Refresh drop_data should truncate all selected tables before load"""
    first_run = True

    @dlt.source
    def my_source():
        @dlt.resource
        def some_data_1():
            # Set some source and resource state
            if first_run:
                dlt.state()["source_key_1"] = "source_value_1"
                resource_state("some_data_1")["resource_key_1"] = "resource_value_1"
                resource_state("some_data_1")["resource_key_2"] = "resource_value_2"
            else:
                # State is cleared for all resources on second run
                assert "source_key_1" in dlt.state()
                assert "source_key_2" in dlt.state()
                assert "source_key_3" in dlt.state()
                # Resource 3 is not wiped
                assert dlt.state()["resources"] == {
                    "some_data_3": {"resource_key_5": "resource_value_5"}
                }
            yield {"id": 1, "name": "John"}
            yield {"id": 2, "name": "Jane"}

        @dlt.resource
        def some_data_2():
            if first_run:
                dlt.state()["source_key_2"] = "source_value_2"
                resource_state("some_data_2")["resource_key_3"] = "resource_value_3"
                resource_state("some_data_2")["resource_key_4"] = "resource_value_4"
            yield {"id": 3, "name": "Joe"}
            yield {"id": 4, "name": "Jill"}

        @dlt.resource
        def some_data_3():
            if first_run:
                dlt.state()["source_key_3"] = "source_value_3"
                resource_state("some_data_3")["resource_key_5"] = "resource_value_5"
            yield {"id": 5, "name": "Jack"}
            yield {"id": 6, "name": "Jill"}

        return [some_data_1, some_data_2, some_data_3]

    # First run pipeline with load to destination so tables are created
    pipeline = dlt.pipeline(
        "refresh_full_test",
        destination="duckdb",
        refresh="drop_data",
        dataset_name="refresh_full_test",
    )

    info = pipeline.run(my_source(), write_disposition="append")
    assert_load_info(info)

    # Second run of pipeline with only selected resources
    first_run = False

    # Mock wrap sql client to capture all queries executed
    with mock.patch.object(
        DuckDbSqlClient, "execute_query", side_effect=DuckDbSqlClient.execute_query, autospec=True
    ) as mock_execute_query:
        info = pipeline.run(
            my_source().with_resources("some_data_1", "some_data_2"), write_disposition="append"
        )

    all_queries = [k[0][1] for k in mock_execute_query.call_args_list]
    assert all_queries
    for q in all_queries:
        assert "drop table" not in q.lower()  # Tables are only truncated, never dropped

    # Tables selected in second run are truncated and should only have data from second run
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_2 ORDER BY id")
    assert result == [(3,), (4,)]

    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_1 ORDER BY id")
    assert result == [(1,), (2,)]

    # Tables not selected in second run are not truncated, still have data from first run
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_3 ORDER BY id")
    assert result == [(5,), (6,)]
