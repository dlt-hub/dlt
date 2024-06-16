import pytest

from dlt.common.schema import TColumnSchema, Schema

from dlt.destinations import dremio
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration, DremioCredentials
from dlt.destinations.impl.dremio.dremio import DremioClient
from tests.load.utils import empty_schema


@pytest.fixture
def dremio_client(empty_schema: Schema) -> DremioClient:
    creds = DremioCredentials()
    creds.database = "test_database"
    # ignore any configured values
    creds.resolve()
    return dremio(credentials=creds).client(
        empty_schema,
        DremioClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
    )


def test_dremio_factory() -> None:
    from dlt.destinations import dremio

    dest = dremio(
        "grpc://username:password@host:1111/data_source", staging_data_source="s3_dlt_stage"
    )
    config = dest.configuration(DremioClientConfiguration()._bind_dataset_name("test_dataset"))
    assert config.staging_data_source == "s3_dlt_stage"
    assert (
        config.credentials.to_url().render_as_string(hide_password=False)
        == "grpc://username:password@host:1111/data_source"
    )
    assert (
        config.credentials.to_native_representation()
        == "grpc://username:password@host:1111/data_source"
    )
    # simplified url ad needed by pydremio
    assert config.credentials.to_native_credentials() == "grpc://host:1111"
    assert config.credentials.db_kwargs() == {"password": "password", "username": "username"}


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
