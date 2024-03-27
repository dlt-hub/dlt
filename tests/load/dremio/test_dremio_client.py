import pytest

from dlt.common.schema import TColumnSchema
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration, DremioCredentials
from dlt.destinations.impl.dremio.dremio import DremioClient
from tests.load.utils import empty_schema


@pytest.fixture
def dremio_client(empty_schema) -> DremioClient:
    creds = DremioCredentials()
    creds.database = "test_database"
    config = DremioClientConfiguration(credentials=creds)
    config.dataset_name = "test_dataset"
    return DremioClient(empty_schema, config)


@pytest.mark.parametrize(
    argnames=("new_columns", "generate_alter", "expected_sql"),
    argvalues=[
        (
            [
                TColumnSchema(name="foo", data_type="text", partition=True),
                TColumnSchema(name="bar", data_type="bigint", sort=True),
                TColumnSchema(name="baz", data_type="double"),
            ],
            False,
            [
                'CREATE TABLE "test_database"."test_dataset"."event_test_table"'
                ' (\n"foo" VARCHAR ,\n"bar" BIGINT ,\n"baz" DOUBLE )\nPARTITION BY'
                ' ("foo")\nLOCALSORT BY ("bar")'
            ],
        ),
        (
            [
                TColumnSchema(name="foo", data_type="text", partition=True),
                TColumnSchema(name="bar", data_type="bigint", partition=True),
                TColumnSchema(name="baz", data_type="double"),
            ],
            False,
            [
                'CREATE TABLE "test_database"."test_dataset"."event_test_table"'
                ' (\n"foo" VARCHAR ,\n"bar" BIGINT ,\n"baz" DOUBLE )\nPARTITION BY'
                ' ("foo","bar")'
            ],
        ),
        (
            [
                TColumnSchema(name="foo", data_type="text"),
                TColumnSchema(name="bar", data_type="bigint"),
                TColumnSchema(name="baz", data_type="double"),
            ],
            False,
            [
                'CREATE TABLE "test_database"."test_dataset"."event_test_table"'
                ' (\n"foo" VARCHAR ,\n"bar" BIGINT ,\n"baz" DOUBLE )'
            ],
        ),
    ],
)
def test_get_table_update_sql(dremio_client, new_columns, generate_alter, expected_sql):
    assert (
        dremio_client._get_table_update_sql(
            table_name="event_test_table", new_columns=new_columns, generate_alter=generate_alter
        )
        == expected_sql
    )
