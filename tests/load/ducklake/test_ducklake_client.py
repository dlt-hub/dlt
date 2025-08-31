import os
import pytest
import pathlib
import unittest.mock
import duckdb

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials

from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DuckLakeClientConfiguration,
    _get_ducklake_capabilities,
    DUCKLAKE_STORAGE_PATTERN,
)
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

from tests.pipeline.utils import assert_load_info
from tests.utils import (
    TEST_STORAGE_ROOT,
    # already configured via conftest
    # patch_home_dir,
    # autouse_test_storage,
    # preserve_environ,
    # wipe_pipeline,
)


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


@pytest.mark.skip("not implemented")
def test_ducklake_sqlclient(tmp_path):
    # TODO: test attach/detach here
    # expected_attach_statement = (
    #     f"ATTACH IF NOT EXISTS 'ducklake:{expected_ducklake_name}.ducklake' AS"
    #     f" {expected_ducklake_name}"
    # )
    pipeline_name = "foo"
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(pipeline_name=pipeline_name)._bind_dataset_name(
            dataset_name="test_conf"
        )
    )

    # TODO patch `ducklake` extension directory to unit test extension installation
    # NOTE: extension is loaded via general mechanism which is already tested in duckdb client
    # assert not _is_extension_loaded(ducklake_client, extension_name="ducklake")

    ducklake_sql_client = DuckLakeSqlClient(
        dataset_name=configuration.normalize_dataset_name(dlt.Schema(name=pipeline_name)),
        staging_dataset_name=None,
        credentials=configuration.credentials,
        capabilities=_get_ducklake_capabilities(),
    )

    expected_attach_statement = (
        f"ATTACH IF NOT EXISTS 'ducklake:{pipeline_name}.ducklake' AS {pipeline_name}"
    )
    assert ducklake_sql_client.attach_statement == expected_attach_statement

    extension_is_installed = False
    extension_is_loaded = False

    with ducklake_sql_client as client:
        for extension_data in client.execute("FROM duckdb_extensions();").fetchall():
            extension_name_found, is_loaded, is_installed, _, _, aliases, *_ = extension_data
            if (extension_name_found == "ducklake") or ("ducklake" in aliases):
                extension_is_installed = is_installed
                extension_is_loaded = is_loaded

    assert extension_is_installed is True
    assert extension_is_loaded is True

    with ducklake_sql_client as client:
        client.create_dataset()
        assert client.has_dataset() is True


@pytest.mark.parametrize(
    "catalog",
    (
        None,
        "sqlite:///catalog.sqlite",
        "duckdb:///catalog.duckdb",
        "postgres://loader:loader@localhost:5432/dlt_data",
    ),
)
def test_destination_defaults(catalog: str) -> None:
    """Check that catalog and storage are materialized at the right
    location and properly derive their name from the pipeline name.
    """
    if catalog is None:
        # use destination alias
        destination: TDestinationReferenceArg = "ducklake"
    else:
        destination = ducklake(credentials=DuckLakeCredentials(catalog=catalog))
    pipeline = dlt.pipeline(
        "destination_defaults", destination=destination, dataset_name="lake_schema", dev_mode=True
    )
    # print(pipeline.destination.configuration())
    # expected_location = pathlib.Path("_storage", DUCKLAKE_NAME_PATTERN % pipeline.dataset_name)

    # with (
    #     unittest.mock.patch("pendulum.now", return_value="2025-08-25T20:44:02.143226+00:00"),
    #     unittest.mock.patch("pendulum.instance", return_value="2025-08-25T20:44:02.143226+00:00"),
    #     unittest.mock.patch("pendulum.datetime", return_value=datetime.datetime.fromisoformat("2025-08-25T20:44:02.143226+00:00")),
    #     unittest.mock.patch("pendulum.from_timestamp", return_value=datetime.datetime.fromisoformat("2025-08-25T20:44:02.143226+00:00")),
    #     unittest.mock.patch("dlt.common.time.ensure_pendulum_datetime", return_value=datetime.datetime.fromisoformat("2025-08-25T20:44:02.143226+00:00")),
    #     unittest.mock.patch("dlt.pipeline.track.on_end_trace", return_value=None)
    # ):
    load_info = pipeline.run(
        [{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet"
    )
    assert_load_info(load_info)

    # test basic data access
    ds = pipeline.dataset()
    assert ds.table_foo["foo"].fetchall() == [(1,), (2,)]

    # test lake location
    expected_location = pathlib.Path(
        TEST_STORAGE_ROOT, DUCKLAKE_STORAGE_PATTERN % pipeline.pipeline_name
    )
    assert expected_location.exists()
    # test dataset in lake
    assert (expected_location / pipeline.dataset_name).exists()

    # test catalog location if applicable
    catalog_location = pipeline.destination_client().config.credentials.catalog.database  # type: ignore
    if "." in catalog_location:
        # it is a file
        assert pathlib.Path(TEST_STORAGE_ROOT, catalog_location).exists()


# def test_assert_configure_catalog_location():
#     ...


# def test_assert_configure_storage_location():
#     ...
