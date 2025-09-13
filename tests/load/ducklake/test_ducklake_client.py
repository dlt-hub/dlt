import os
import pytest
import pathlib
import duckdb

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials

from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DuckLakeClientConfiguration,
)

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

    assert credentials.is_partial() is False
    assert credentials.is_resolved() is False

    assert credentials.database is None
    assert credentials.catalog is None
    assert credentials.storage is None

    # resolve empty credentials
    credentials.resolve()
    assert credentials.is_partial() is False
    assert credentials.is_resolved() is True


def test_ducklake_urls() -> None:
    os.environ["CREDENTIALS"] = "ducklake:env_ducklake_1"
    credentials = resolve_configuration(DuckLakeCredentials())
    assert credentials.ducklake_name == "env_ducklake_1"

    os.environ["CREDENTIALS"] = "ducklake:///env_ducklake_2"
    credentials = resolve_configuration(DuckLakeCredentials())
    assert credentials.ducklake_name == "env_ducklake_2"

    # just a name is also acceptable
    os.environ["CREDENTIALS"] = "env_ducklake_3"
    credentials = resolve_configuration(DuckLakeCredentials())
    assert credentials.ducklake_name == "env_ducklake_3"


def test_default_ducklake_configuration() -> None:
    # without pipeline context and destination name
    configuration = resolve_configuration(
        DuckLakeClientConfiguration()._bind_dataset_name(dataset_name="foo")
    )

    local_dir = pathlib.Path.cwd() / TEST_STORAGE_ROOT

    credentials = configuration.credentials

    assert credentials.is_partial() is False
    assert credentials.is_resolved() is True

    assert credentials.database == "ducklake"
    assert credentials.ducklake_name == "ducklake"
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

    # plug default duckdb catalog
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            credentials=DuckLakeCredentials(
                catalog=ConnectionStringCredentials({"drivername": "duckdb"})
            )
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials
    assert credentials.ducklake_name == "ducklake"
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "ducklake.duckdb"))

    # destination name is set (precedence over pipeline)
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            destination_name="named_lake", pipeline_name="lake_pipeline"
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials
    assert credentials.ducklake_name == "named_lake"
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "named_lake.sqlite"))
    assert credentials.storage_url == str(local_dir / "named_lake.files")

    # pipeline name is set
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(pipeline_name="test_ducklake")._bind_dataset_name(
            dataset_name="foo"
        )
    )
    credentials = configuration.credentials
    assert credentials.ducklake_name == "test_ducklake"
    conn_str = credentials.catalog.to_native_representation()
    assert conn_str.endswith(str(local_dir / "test_ducklake.sqlite"))
    assert credentials.storage_url == str(local_dir / "test_ducklake.files")

    # explicit values
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(
            destination_name="named_lake",
            credentials=DuckLakeCredentials(
                "explicit_ducklake",
                catalog="postgresql://loader:loader@localhost:5432/dlt_data",
                storage="s3://dlt-ci-test-bucket/lake",
            ),
        )._bind_dataset_name(dataset_name="foo")
    )
    credentials = configuration.credentials
    assert credentials.ducklake_name == "explicit_ducklake"
    assert (
        credentials.catalog.to_native_representation()
        == "postgresql://loader:loader@localhost:5432/dlt_data"
    )
    # NOTE: dataset folders will be created in /lake/
    assert credentials.storage_url == "s3://dlt-ci-test-bucket/lake"

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
    assert credentials.ducklake_name == "test_ducklake"
    assert (
        credentials.catalog.to_native_representation()
        == "postgresql://loader:loader@localhost:5432/dlt_data"
    )
    assert credentials.storage_url == str(local_dir / "test_ducklake.files")


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
