import os
import pytest

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.utils import get_resolved_traces

from dlt.destinations.duckdb.configuration import DUCK_DB_NAME, DuckDbClientConfiguration, DEFAULT_DUCK_DB_NAME

from tests.load.pipeline.utils import drop_pipeline
from tests.utils import patch_home_dir, autouse_test_storage, preserve_environ, TEST_STORAGE_ROOT


@pytest.fixture(autouse=True)
def delete_default_duckdb_credentials() -> None:
    # remove the default duckdb config
    # os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    os.environ.clear()
    yield
    delete_quack_db()


def test_duckdb_open_conn_default() -> None:
    delete_quack_db()
    try:
        get_resolved_traces().clear()
        c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset"))
        print(str(c.credentials))
        print(str(os.getcwd()))
        print(get_resolved_traces())
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
    assert c.credentials.database.lower() == os.path.abspath("quack.duckdb").lower()
    # resolve without any path but with pipeline context
    p = dlt.pipeline(pipeline_name="quack_pipeline")
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset"))
    # still cwd
    db_path = os.path.abspath(os.path.join(".", "quack_pipeline.duckdb"))
    assert c.credentials.database.lower() == db_path.lower()
    # we do not keep default duckdb path in the local state
    with pytest.raises(KeyError):
        p.get_local_state_val("duckdb_database")

    # connect
    try:
        conn = c.credentials.borrow_conn(read_only=False)
        c.credentials.return_conn(conn)
        assert os.path.isfile(db_path)
    finally:
        if os.path.isfile(db_path):
            os.unlink(db_path)

    # test special :pipeline: path to create in pipeline folder
    c = resolve_configuration(DuckDbClientConfiguration(dataset_name="test_dataset", credentials=":pipeline:"))
    db_path = os.path.abspath(os.path.join(p.working_dir, DEFAULT_DUCK_DB_NAME))
    assert c.credentials.database.lower() == db_path.lower()
    # connect
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    assert p.get_local_state_val("duckdb_database").lower() == db_path.lower()

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


def test_keeps_initial_db_path() -> None:
    db_path = "_storage/path_test_quack.duckdb"
    p = dlt.pipeline(pipeline_name="quack_pipeline", credentials=db_path, destination="duckdb")
    print(p.pipelines_dir)
    with p.sql_client() as conn:
        # still cwd
        assert conn.credentials.database.lower() == os.path.abspath(db_path).lower()
        # but it is kept in the local state
        assert p.get_local_state_val("duckdb_database").lower() == os.path.abspath(db_path).lower()

    # attach the pipeline
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.get_local_state_val("duckdb_database").lower() == os.path.abspath(db_path).lower()
    with p.sql_client() as conn:
        # still cwd
        assert p.get_local_state_val("duckdb_database").lower() == os.path.abspath(db_path).lower()
        assert conn.credentials.database.lower() == os.path.abspath(db_path).lower()

    # now create a new pipeline
    dlt.pipeline(pipeline_name="not_quack", destination="dummy")
    with p.sql_client() as conn:
        # still cwd
        assert p.get_local_state_val("duckdb_database").lower() == os.path.abspath(db_path).lower()
        # new pipeline context took over
        # TODO: restore pipeline context on each call
        assert conn.credentials.database.lower() != os.path.abspath(db_path).lower()


def test_duckdb_database_delete() -> None:
    db_path = "_storage/path_test_quack.duckdb"
    p = dlt.pipeline(pipeline_name="quack_pipeline", credentials=db_path, destination="duckdb")
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # attach the pipeline
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.first_run is False
    # drop the database
    os.remove(db_path)
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.first_run is False
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # we reverted to a default path in cwd
    with pytest.raises(KeyError):
        p.get_local_state_val("duckdb_database")


def test_duck_database_path_delete() -> None:
    # delete path
    db_folder = "_storage/db_path"
    os.makedirs(db_folder)
    db_path = f"{db_folder}/path_test_quack.duckdb"
    p = dlt.pipeline(pipeline_name="deep_quack_pipeline", credentials=db_path, destination="duckdb")
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # attach the pipeline
    p = dlt.attach(pipeline_name="deep_quack_pipeline")
    assert p.first_run is False
    # drop the database
    os.remove(db_path)
    os.rmdir(db_folder)
    p = dlt.attach(pipeline_name="deep_quack_pipeline")
    assert p.first_run is False
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # we reverted to a default path in cwd
    with pytest.raises(KeyError):
        p.get_local_state_val("duckdb_database")


def test_case_sensitive_database_name() -> None:
    # make case sensitive folder name
    cs_quack = os.path.join(TEST_STORAGE_ROOT, "QuAcK")
    os.makedirs(cs_quack, exist_ok=True)
    db_path = os.path.join(cs_quack, "path_TEST_quack.duckdb")
    p = dlt.pipeline(pipeline_name="NOT_QUAck", credentials=db_path, destination="duckdb")
    with p.sql_client() as conn:
        conn.execute_sql("DESCRIBE;")


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
