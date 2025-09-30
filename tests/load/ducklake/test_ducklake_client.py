import os
import pathlib

import duckdb
import pytest

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException, ConfigurationValueError
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DuckLakeClientConfiguration,
    DEFAULT_DUCKLAKE_NAME,
)

from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient
from tests.utils import TEST_STORAGE_ROOT


def test_native_duckdb_workflow(tmp_path):
    """Test our basic assumptions about how DuckLake works.

    This uses:
        - ducklake client: in-memory duckdb
        - catalog database: on-disk duckdb `catalog.ducklake`
        - storage: on-disk filesystem automatically determined by duckdb

    ref: https://ducklake.select/docs/stable/duckdb/introduction
    """

    ducklake_name = "my_ducklake"
    catalog_database = f"{tmp_path}/catalog.ducklake"

    ducklake_client = duckdb.connect(":memory:")
    ducklake_client.execute("INSTALL ducklake; LOAD ducklake")
    ducklake_client.execute(f"ATTACH 'ducklake:{catalog_database}' AS {ducklake_name}")
    ducklake_client.execute(f"USE {ducklake_name}")

    ducklake_client.execute("""
        CREATE TABLE people (id INTEGER, name TEXT);
        INSERT INTO people VALUES (1, 'Alice');
    """)

    catalog = ducklake_client.execute("SELECT current_catalog()").fetchone()[0]
    schema = ducklake_client.execute("SELECT current_schema()").fetchone()[0]
    database = ducklake_client.execute("SELECT current_database()").fetchone()[0]

    assert catalog == ducklake_name
    assert database == ducklake_name
    assert schema == "main"


def test_default_credentials() -> None:
    credentials = DuckLakeCredentials()

    assert credentials.is_partial() is True
    assert credentials.is_resolved() is False

    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    assert credentials.catalog is None
    assert credentials.storage is None

    # resolve empty credentials
    resolve_configuration(credentials)
    assert credentials.is_partial() is False
    assert credentials.is_resolved() is True

    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    assert credentials.catalog is not None
    assert credentials.catalog.drivername == "sqlite"
    assert credentials.storage is not None
    assert credentials.storage.local_dir == "."


def test_ducklake_urls() -> None:
    os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__DUCKLAKE_NAME"] = "env_ducklake_1"
    credentials = resolve_configuration(DuckLakeCredentials(), sections=("destination", "ducklake"))
    assert credentials.ducklake_name == "env_ducklake_1"


def test_ducklake_configuration_default() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT
    # without pipeline context and destination name
    configuration = resolve_configuration(
        DuckLakeClientConfiguration()._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials

    assert credentials.is_partial() is False
    assert credentials.is_resolved() is True
    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    # default catalog location should point to _storage (local_dir)
    assert credentials.catalog.database == str(local_dir / "ducklake.sqlite")
    # sqlite is default catalog
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.startswith("sqlite:")
    assert conn_str.endswith(str(local_dir / "ducklake.sqlite"))
    # storage in local dir, default name
    assert credentials.storage_url == str(local_dir / "ducklake.files")
    # file url
    assert credentials.storage.bucket_url.startswith("file://")


def test_ducklake_configuration_duckdb_catalog() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT
    # plug default duckdb catalog
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            credentials=DuckLakeCredentials(
                catalog=ConnectionStringCredentials({"drivername": "duckdb"})
            )
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials

    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "ducklake.duckdb"))


def test_ducklake_configuration_ducklake_name() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT
    # catalog name sets default locations
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            credentials=DuckLakeCredentials(ducklake_name="my_ducklake")
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials

    assert credentials.ducklake_name == "my_ducklake"
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "my_ducklake.sqlite"))
    assert credentials.storage_url == str(local_dir / "my_ducklake.files")


def test_ducklake_configuration_destination_name() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT
    # destination name is set
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            destination_name="named_lake", pipeline_name="lake_pipeline"
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials
    # no impact on locations
    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "ducklake.sqlite"))
    assert credentials.storage_url == str(local_dir / "ducklake.files")


def test_ducklake_configuration_pipeline_name() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT

    # pipeline name is set
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(pipeline_name="test_ducklake")._bind_dataset_name(
            dataset_name="foo"
        )
    )
    credentials = configuration.credentials

    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    # no impact on locations
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "ducklake.sqlite"))
    assert credentials.storage_url == str(local_dir / "ducklake.files")


def test_ducklake_configuration_storage_credentials() -> None:
    # explicit values
    os.environ["CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "key"
    os.environ["CREDENTIALS__AWS_ACCESS_KEY_ID"] = "id"

    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            destination_name="named_lake",
            credentials=DuckLakeCredentials(
                "my_ducklake",
                catalog="postgresql://loader:loader@localhost:5432/dlt_data",
                storage="s3://dlt-ci-test-bucket/lake",
            ),
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials

    assert credentials.ducklake_name == "my_ducklake"
    assert (
        credentials.catalog.to_native_representation()
        == "postgresql://loader:loader@localhost:5432/dlt_data"
    )
    # NOTE: dataset folders will be created in /lake/
    assert credentials.storage_url == "s3://dlt-ci-test-bucket/lake"


def test_ducklake_configuration_catalog_credentials() -> None:
    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT

    # explicit catalog
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            pipeline_name="test_ducklake",
            credentials=DuckLakeCredentials(
                catalog="postgresql://loader:loader@localhost:5432/dlt_data"
            ),
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials

    assert credentials.ducklake_name == DEFAULT_DUCKLAKE_NAME
    assert (
        credentials.catalog.to_native_representation()
        == "postgresql://loader:loader@localhost:5432/dlt_data"
    )
    assert credentials.storage_url == str(local_dir / "ducklake.files")


def test_ducklake_attach_statement() -> None:
    """Low-level method to attach the ducklake catalog to the ducklake client.

    A bug was found because of differences between sqlalchemy <2 and >=2.
    """
    expected_attach_statement = (
        "ATTACH IF NOT EXISTS 'ducklake:postgres:postgres://loader:loader@localhost:5432/dlt_data'"
        " AS foo (DATA_PATH '/path/to/storage', METADATA_SCHEMA 'foo')"
    )

    attach_statement = DuckLakeSqlClient.build_attach_statement(
        catalog=ConnectionStringCredentials("postgres://loader:loader@localhost:5432/dlt_data"),
        ducklake_name="foo",
        storage_url="/path/to/storage",
    )

    assert expected_attach_statement == attach_statement


def test_attach_statement_doesnt_use_postgresql() -> None:
    """`drivername="postgresql"` is supported by dlt / sqlalchemy, but it doesn't exist in duckdb.

    Make sure that the produced ATTACH statement doesn't use postgresql
    """
    expected_attach_statement = (
        "ATTACH IF NOT EXISTS 'ducklake:postgres:postgres://loader:loader@localhost:5432/dlt_data'"
        " AS foo (DATA_PATH './path/to/storage', METADATA_SCHEMA 'foo')"
    )
    attach_statement = DuckLakeSqlClient.build_attach_statement(
        catalog=ConnectionStringCredentials("postgresql://loader:loader@localhost:5432/dlt_data"),
        ducklake_name="foo",
        storage_url="./path/to/storage",
    )

    assert expected_attach_statement == attach_statement
    assert "postgresql" not in expected_attach_statement


def test_ducklake_conn_pool_always_open() -> None:
    # connection pool is embedded in configuration, configuration is a singleton during loading
    # phase which the pool needs. See DuckDbConnectionPool
    configuration = resolve_configuration(
        DuckLakeClientConfiguration()._bind_dataset_name(dataset_name="foo")
    )
    pool = configuration.credentials.conn_pool
    conn = pool.borrow_conn()
    assert pool._conn_borrows == 1
    # in this mode, there's no "base" connection so it is not kept
    assert pool._conn is None
    # make sure conn open
    conn.sql("SHOW TABLES;")
    conn2 = pool.borrow_conn()
    assert pool._conn_borrows == 2
    # return one conn
    assert pool.return_conn(conn) == 1
    # conn closed
    with pytest.raises(duckdb.Error):
        conn.sql("SHOW TABLES;")
    # conn2 still functions
    conn2.sql("SHOW TABLES;")
    # still functions after calling destructor
    # TODO: pool should close all dispensed connections in this mode. this is not implemented.
    pool.__del__()
    conn2.sql("SHOW TABLES;")
    # NOTE: conn2 may be passed to ibis, pool.move is not implemented in this mode
    conn2.close()

    # test external connections
    with pytest.raises(ConfigurationValueError, match="External connections not supported"):
        configuration = resolve_configuration(
            DuckLakeClientConfiguration(credentials=duckdb.connect())._bind_dataset_name(  # type: ignore[arg-type]
                dataset_name="foo"
            )
        )


@pytest.mark.no_load
def test_ducklake_factory_instantiation() -> None:
    # force parallel loads on sqlite
    ducklake = dlt.destinations.ducklake(loader_parallelism_strategy="parallel")
    pipeline = dlt.pipeline("test_factory", destination=ducklake, dataset_name="foo")

    with pipeline.destination_client() as client:
        assert client.capabilities.loader_parallelism_strategy == "parallel"

    # set ducklake credentials using shorthands, s3 bucket requires secrets in config
    credentials = DuckLakeCredentials(
        "lake_catalog",
        catalog="postgresql://loader:pass@localhost:5432/dlt_data",
        storage="s3://dlt-ci-test-bucket/lake",
    )
    ducklake = dlt.destinations.ducklake(credentials=credentials)
    pipeline = dlt.pipeline("test_factory", destination=ducklake, dataset_name="foo")

    with pytest.raises(ConfigFieldMissingException):
        pipeline.destination_client()

    # TODO what is being asserted in the next block?
    # set catalog name using connection string credentials
    catalog_credentials = ConnectionStringCredentials()
    # use duckdb with the default name
    catalog_credentials.drivername = "duckdb"
    credentials = DuckLakeCredentials(
        "lake_catalog",
        catalog=catalog_credentials,
    )
