import os
import pytest

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.exceptions import NativeValueError

from dlt.destinations.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration, DEFAULT_DUCK_DB_NAME

from tests.utils import autouse_test_storage, preserve_environ, TEST_STORAGE_ROOT
from tests.pipeline.utils import patch_working_dir


@pytest.fixture(autouse=True)
def delete_default_duckdb_credentials() -> None:
    # remove the default duckdb config
    # os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    os.environ.clear()


def test_duckdb_open_conn_default() -> None:
    delete_quack_db()
    try:
        c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset"))
        conn = c.credentials.borrow_conn(read_only=False)
        assert c.credentials._conn_borrows == 1
        assert c.credentials._conn_owner is True
        # return conn
        c.credentials.return_conn(conn)
        # connection destroyed
        assert c.credentials._conn_borrows == 0
        assert c.credentials._conn_owner is True
        assert not hasattr(c.credentials, "_conn")
        # db file is created
        assert os.path.isfile(DEFAULT_DUCK_DB_NAME)
    finally:
        delete_quack_db()


def test_duckdb_database_path() -> None:
    # resolve without any path provided
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset"))
    assert c.credentials.database.lower() == os.path.abspath(DEFAULT_DUCK_DB_NAME).lower()
    # resolve without any path but with pipeline context
    p = dlt.pipeline(pipeline_name="quack_pipeline")
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset"))
    db_path = os.path.abspath(os.path.join(p.working_dir, DEFAULT_DUCK_DB_NAME))
    assert c.credentials.database.lower() == db_path.lower()
    # connect
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)

    # provide relative path
    db_path = "_storage/test_quack.duckdb"
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials="duckdb:///_storage/test_quack.duckdb"))
    assert c.credentials.database.lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)

    # provide absolute path
    db_path = os.path.abspath("_storage/abs_test_quack.duckdb")
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=f"duckdb:///{db_path}"))
    assert os.path.isabs(c.credentials.database)
    assert c.credentials.database.lower() == db_path.lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)

    # set just path as credentials
    db_path = "_storage/path_test_quack.duckdb"
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=db_path))
    assert c.credentials.database.lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)

    db_path = os.path.abspath("_storage/abs_path_test_quack.duckdb")
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=db_path))
    assert os.path.isabs(c.credentials.database)
    assert c.credentials.database.lower() == db_path.lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)

    # invalid path
    import duckdb

    with pytest.raises(duckdb.IOException):
        c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=TEST_STORAGE_ROOT))
        conn = c.credentials.borrow_conn(read_only=False)


def test_external_duckdb_database() -> None:
    import duckdb

    # pass explicit in memory database
    conn = duckdb.connect(":memory:")
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=conn))
    assert c.credentials._conn_borrows == 0
    assert c.credentials._conn is conn
    int_conn = c.credentials.borrow_conn(read_only=False)
    assert c.credentials._conn_borrows == 1
    assert c.credentials._conn_owner is False
    c.credentials.return_conn(int_conn)
    assert c.credentials._conn_borrows == 0
    assert c.credentials._conn_owner is False
    assert hasattr(c.credentials, "_conn")
    conn.close()


def delete_quack_db() -> None:
    if os.path.isfile(DEFAULT_DUCK_DB_NAME):
        os.remove(DEFAULT_DUCK_DB_NAME)
