import pytest

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.destinations.adapters import bigquery_adapter
from dlt.load.exceptions import LoadClientJobFailed
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.pipeline.utils import assert_load_info


def test_bigquery_adapter_streaming_insert():
    @dlt.resource
    def test_resource():
        yield {"field1": 1, "field2": 2}

    bigquery_adapter(test_resource, insert_api="streaming")

    pipe = dlt.pipeline(pipeline_name="insert_test", destination="bigquery", dev_mode=True)
    pack = pipe.run(test_resource, table_name="test_streaming_items44")

    assert_load_info(pack)

    with pipe.sql_client() as client:
        with client.execute_query("SELECT * FROM test_streaming_items44;") as cursor:
            res = cursor.fetchall()
            assert tuple(res[0])[:2] == (1, 2)


def test_bigquery_adapter_streaming_wrong_disposition():
    @dlt.resource(write_disposition="merge")
    def test_resource():
        yield {"field1": 1, "field2": 2}

    with pytest.raises(ValueError):
        bigquery_adapter(test_resource, insert_api="streaming")


def test_bigquery_streaming_wrong_disposition():
    @dlt.resource(write_disposition="merge")
    def test_resource():
        yield {"field1": 1, "field2": 2}

    test_resource.apply_hints(additional_table_hints={"x-insert-api": "streaming"})

    pipe = dlt.pipeline(pipeline_name="insert_test", destination="bigquery")
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipe.run(test_resource)
    assert isinstance(pip_ex.value.step_info, LoadInfo)
    assert pip_ex.value.step_info.has_failed_jobs
    # pick the failed job
    assert isinstance(pip_ex.value.__cause__, LoadClientJobFailed)
    assert (
        """BigQuery streaming insert can only be used with `append`"""
        """ write_disposition, while the given resource has `merge`."""
    ) in pip_ex.value.__cause__.failed_message


def test_bigquery_streaming_nested_data():
    @dlt.resource
    def test_resource():
        yield {"field1": {"nested_field": 1}, "field2": [{"nested_field": 2}]}

    bigquery_adapter(test_resource, insert_api="streaming")

    pipe = dlt.pipeline(pipeline_name="insert_test", destination="bigquery", dev_mode=True)
    pack = pipe.run(test_resource, table_name="test_streaming_items")

    assert_load_info(pack)

    with pipe.sql_client() as client:
        with client.execute_query("SELECT * FROM test_streaming_items;") as cursor:
            res = cursor.fetchall()
            assert res[0]["field1__nested_field"] == 1  # type: ignore

        with client.execute_query("SELECT * FROM test_streaming_items__field2;") as cursor:
            res = cursor.fetchall()
            assert res[0]["nested_field"] == 2  # type: ignore
