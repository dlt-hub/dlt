import pytest

import dlt
from dlt.common.pipeline import resource_state
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from tests.utils import clean_test_storage, preserve_environ
from tests.pipeline.utils import assert_load_info


def test_refresh_full():
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

    # Confirm resource tables not selected on second run got wiped
    with pytest.raises(DatabaseUndefinedRelation):
        with pipeline.sql_client() as client:
            result = client.execute_sql("SELECT * FROM some_data_3")

    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT id FROM some_data_1 ORDER BY id")
    assert result == [(1,), (2,)]


def test_refresh_replace():
    first_run = True

    @dlt.source
    def my_source():
        @dlt.resource
        def some_data_1():
            # Set some source and resource state
            state = dlt.state()
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
        refresh="replace",
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
