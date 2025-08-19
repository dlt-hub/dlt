import pathlib

import dlt

import duckdb
from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
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
    expected_ducklake_name = "ducklake"
    expected_db_url = "duckdb://"
    expected_attach_statement = f"ATTACH 'ducklake:{expected_db_url}' AS {expected_ducklake_name}"

    credentials = DuckLakeCredentials()

    assert credentials.is_partial() is False  # from motherduck tests
    assert credentials.is_resolved() is False  # from motherduck tests
    assert credentials.ducklake_name == expected_ducklake_name
    assert isinstance(credentials.catalog_database, DuckDbCredentials)
    # resolved by the `DuckLakeClientConfiguration`
    assert credentials.catalog_database.to_native_representation() == expected_db_url
    assert credentials.storage is None
    # resolved by the `DuckLakeClientConfiguration`
    assert credentials.database is None
    assert credentials.attach_statement == expected_attach_statement


# TODO add tests for different ways to instantiate DuckLakeCredentials


def test_default_ducklake_configuration() -> None:
    expected_ducklake_name = "ducklake"
    expected_database_name = DUCKLAKE_NAME_PATTERN % ""
    expected_path = pathlib.Path.cwd() / TEST_STORAGE_ROOT / expected_database_name
    # NOTE is this an error to have 4 slashes? `////`
    expected_db_url = "duckdb:///" + str(expected_path)
    expected_attach_statement = f"ATTACH 'ducklake:{expected_db_url}' AS {expected_ducklake_name}"

    configuration = resolve_configuration(
        DuckLakeClientConfiguration()._bind_dataset_name(dataset_name="test_conf")
    )
    credentials = configuration.credentials

    assert credentials.is_partial() is False  # from motherduck tests
    assert credentials.is_resolved() is True  # is now True; from motherduck tests
    assert credentials.ducklake_name == expected_ducklake_name
    # value is resolved
    assert credentials.catalog_database.to_native_representation() == expected_db_url
    assert credentials.storage is None
    # value is resolved
    assert credentials.database == str(expected_path)
    assert credentials.attach_statement == expected_attach_statement


def test_ducklake_sqlclient():
    schema = dlt.Schema(name="foo")
    credentials = DuckLakeCredentials()
    config = DuckLakeClientConfiguration(credentials=credentials)
    capabilities = _get_ducklake_capabilities()

    # TODO patch `ducklake` extension directory to unit test extension installation
    # assert not _is_extension_loaded(ducklake_client, extension_name="ducklake")

    ducklake_client = DuckLakeSqlClient(
        dataset_name=config.normalize_dataset_name(schema),
        staging_dataset_name=config.normalize_staging_dataset_name(schema),
        credentials=config.credentials,
        capabilities=capabilities,
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


def test_simple_write() -> None:
    destination = dlt.destinations.ducklake()

    pipeline = dlt.pipeline("ducklake_test", destination=destination)
    pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")

    dataset = pipeline.dataset()

    assert "table_foo" in dataset.tables
