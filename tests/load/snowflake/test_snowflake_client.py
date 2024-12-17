from copy import deepcopy
import os
from typing import Iterator
from pytest_mock import MockerFixture
import pytest

from dlt.common.schema.schema import Schema
from dlt.destinations.impl.snowflake.snowflake import SUPPORTED_HINTS, SnowflakeClient
from dlt.destinations.job_client_impl import SqlJobClientBase

from dlt.destinations.sql_client import TJobQueryTags

from tests.cases import TABLE_UPDATE
from tests.load.utils import yield_client_with_storage, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

QUERY_TAG = (
    '{{"source":"{source}", "resource":"{resource}", "table": "{table}", "load_id":"{load_id}",'
    ' "pipeline_name":"{pipeline_name}"}}'
)
QUERY_TAGS_DICT: TJobQueryTags = {
    "source": "test_source",
    "resource": "test_resource",
    "table": "test_table",
    "load_id": "1109291083091",
    "pipeline_name": "test_pipeline",
}


@pytest.fixture(scope="function")
def client() -> Iterator[SqlJobClientBase]:
    os.environ["QUERY_TAG"] = QUERY_TAG
    yield from yield_client_with_storage("snowflake")


def test_create_table_with_hints(client: SnowflakeClient, empty_schema: Schema) -> None:
    mod_update = deepcopy(TABLE_UPDATE[:11])
    # mock hints
    client.config.create_indexes = True
    client.active_hints = SUPPORTED_HINTS
    client.schema = empty_schema

    mod_update[0]["primary_key"] = True
    mod_update[5]["primary_key"] = True

    mod_update[0]["sort"] = True
    mod_update[4]["parent_key"] = True

    # unique constraints are always single columns
    mod_update[1]["unique"] = True
    mod_update[7]["unique"] = True

    sql = ";".join(client._get_table_update_sql("event_test_table", mod_update, False))

    print(sql)
    client.sql_client.execute_sql(sql)

    # generate alter table
    mod_update = deepcopy(TABLE_UPDATE[11:])
    mod_update[0]["primary_key"] = True
    mod_update[1]["unique"] = True

    sql = ";".join(client._get_table_update_sql("event_test_table", mod_update, True))

    print(sql)
    client.sql_client.execute_sql(sql)


def test_query_tag(client: SnowflakeClient, mocker: MockerFixture):
    assert client.config.query_tag == QUERY_TAG
    # make sure we generate proper query
    execute_sql_spy = mocker.spy(client.sql_client, "execute_sql")
    # reset the query if tags are not set
    client.sql_client.set_query_tags(None)
    execute_sql_spy.assert_called_once_with(sql="ALTER SESSION UNSET QUERY_TAG")
    execute_sql_spy.reset_mock()
    client.sql_client.set_query_tags({})  # type: ignore[typeddict-item]
    execute_sql_spy.assert_called_once_with(sql="ALTER SESSION UNSET QUERY_TAG")
    execute_sql_spy.reset_mock()
    # set query tags
    client.sql_client.set_query_tags(QUERY_TAGS_DICT)
    execute_sql_spy.assert_called_once_with(
        sql=(
            'ALTER SESSION SET QUERY_TAG = \'{"source":"test_source", "resource":"test_resource",'
            ' "table": "test_table", "load_id":"1109291083091", "pipeline_name":"test_pipeline"}\''
        )
    )
    # remove query tag from config
    client.sql_client.query_tag = None
    execute_sql_spy.reset_mock()
    client.sql_client.set_query_tags(QUERY_TAGS_DICT)
    execute_sql_spy.assert_not_called
    execute_sql_spy.reset_mock()
    client.sql_client.set_query_tags(None)
    execute_sql_spy.assert_not_called
