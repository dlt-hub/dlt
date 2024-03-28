import dlt
from dlt.destinations.exceptions import DatabaseTransientException
from dlt.destinations.impl.bigquery.bigquery_adapter import bigquery_adapter
from tests.pipeline.utils import assert_load_info


def test_bigquery_adapter_streaming_insert():
    @dlt.resource
    def test_resource():
        yield {"field1": 1, "field2": 2}

    bigquery_adapter(test_resource, insert_api="streaming")

    pipe = dlt.pipeline(pipeline_name="insert_test", destination="bigquery")
    pack = pipe.run(test_resource, table_name="test_streaming_items")

    assert_load_info(pack)

    with pipe.sql_client() as client:
        with client.execute_query("SELECT * FROM test_streaming_items;") as cursor:
            res = cursor.fetchall()
            assert tuple(res[0])[:2] == (1, 2)


def test_bigquery_streaming_nested_data():
    @dlt.resource
    def test_resource():
        yield {"field1": {"nested_field": 1}, "field2": {"nested_field": 2}}

    bigquery_adapter(test_resource, insert_api="streaming")

    pipe = dlt.pipeline(pipeline_name="insert_test", destination="bigquery")
    pack = pipe.run(test_resource, table_name="test_streaming_items")

    assert_load_info(pack)

    with pipe.sql_client() as client:
        with client.execute_query("SELECT * FROM test_streaming_items;") as cursor:
            res = cursor.fetchall()
            assert res[0]["field1__nested_field"] == 1
            assert res[0]["field2__nested_field"] == 2
