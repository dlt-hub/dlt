import pathlib

import dlt

import duckdb
from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DuckLakeClientConfiguration,
    _get_ducklake_capabilities,
    DUCKLAKE_NAME_PATTERN,
)
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

from tests.utils import (
    TEST_STORAGE_ROOT,
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    wipe_pipeline,
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
    assert credentials.attach_statement is None


# TODO add tests for different ways to instantiate DuckLakeCredentials


def test_default_ducklake_configuration() -> None:
    pipeline_name = "test_ducklake"
    expected_ducklake_name = pipeline_name
    expected_database = DUCKLAKE_NAME_PATTERN % expected_ducklake_name
    expected_path = pathlib.Path.cwd() / TEST_STORAGE_ROOT / expected_database
    # NOTE is this an error to have 4 slashes? `////`
    expected_db_url = "duckdb:///" + str(expected_path)
    expected_attach_statement = (
        f"ATTACH IF NOT EXISTS 'ducklake:{expected_ducklake_name}.ducklake' AS"
        f" {expected_ducklake_name}"
    )

    configuration = resolve_configuration(
        DuckLakeClientConfiguration(pipeline_name=pipeline_name)
        ._bind_dataset_name(dataset_name="FOO")
    )
    credentials = configuration.credentials

    assert credentials.is_partial() is False
    assert credentials.is_resolved() is True

    assert credentials.ducklake_name == expected_ducklake_name

    assert credentials.database == credentials.catalog.database == str(expected_path)
    
    # currently, we use `drivername="duckdb"` for the ducklake client;
    # if we change it to `drivername="ducklake"`, update this check
    ducklake_client_url = credentials.to_native_representation()
    catalog_url = credentials.catalog.to_native_representation()
    assert ducklake_client_url == catalog_url == expected_db_url
    
    assert credentials.storage is None
    # value is resolved
    assert credentials.attach_statement == expected_attach_statement


def test_ducklake_sqlclient(tmp_path):
    pipeline_name = "foo"
    schema = dlt.Schema(name=pipeline_name)
    # need to set attach statement because default credentials can't
    # be resolved outside of a pipeline context
    # attach_statement = f"ATTACH '{tmp_path}/catalog.ducklake' AS ducklake;"
    # credentials = DuckLakeCredentials(attach_statement=attach_statement)
    configuration = resolve_configuration(
        DuckLakeClientConfiguration(pipeline_name=pipeline_name)
        ._bind_dataset_name(dataset_name="test_conf")
    )
    expected_attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{pipeline_name}.ducklake' AS {pipeline_name}"
    assert configuration.credentials.attach_statement == expected_attach_statement

    # TODO patch `ducklake` extension directory to unit test extension installation
    # assert not _is_extension_loaded(ducklake_client, extension_name="ducklake")

    ducklake_client = DuckLakeSqlClient(
        dataset_name=configuration.normalize_dataset_name(schema),
        staging_dataset_name=configuration.normalize_staging_dataset_name(schema),
        credentials=configuration.credentials,
        capabilities=_get_ducklake_capabilities(),
    )

    extension_is_installed = False
    extension_is_loaded = False

    with ducklake_client as client:
        for extension_data in client.execute("FROM duckdb_extensions();").fetchall():
            extension_name_found, is_loaded, is_installed, _, _, aliases, *_ = extension_data
            if (extension_name_found == "ducklake") or ("ducklake" in aliases):
                extension_is_installed = is_installed
                extension_is_loaded = is_loaded

    assert extension_is_installed is True
    assert extension_is_loaded is True


def test_destination_defaults() -> None:
    """Check that catalog and storage are materialized at the right 
    location and properly derive their name from the pipeline name.
    
    Note that default storage is managed by the ducklake extension itself.
    """
    pipeline = dlt.pipeline("simple_write", destination="ducklake")
    expected_location = pathlib.Path(".", DUCKLAKE_NAME_PATTERN % pipeline.dataset_name)

    pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")

    assert expected_location.exists()

    dataset = pipeline.dataset()
    assert "table_foo" in dataset.tables


# def test_assert_configure_catalog_location():
#     ...


# def test_assert_configure_storage_location():
#     ...