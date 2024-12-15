import os
import pytest
from typing import Iterator, cast

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.utils import get_resolved_traces
from dlt.common.destination.reference import Destination
from dlt.common.utils import set_working_dir

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DEFAULT_DUCK_DB_NAME,
)
from dlt.destinations import duckdb

from dlt.destinations.impl.duckdb.exceptions import InvalidInMemoryDuckdbCredentials
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.pipeline.utils import assert_table
from tests.utils import autouse_test_storage, TEST_STORAGE_ROOT

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def delete_default_duckdb_credentials(autouse_test_storage) -> Iterator[None]:
    # remove the default duckdb config
    # os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    os.environ.clear()
    with set_working_dir("_storage"):
        yield
    delete_quack_db()


def test_duckdb_open_conn_default() -> None:
    delete_quack_db()
    try:
        get_resolved_traces().clear()
        c = resolve_configuration(
            DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
        )
        # print(str(c.credentials))
        # print(str(os.getcwd()))
        # print(get_resolved_traces())
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


def test_duckdb_in_memory_mode_via_factory():
    delete_quack_db()
    try:
        import duckdb

        # Check if passing external duckdb connection works fine
        db = duckdb.connect(":memory:")
        dlt.pipeline(pipeline_name="booboo", destination=dlt.destinations.duckdb(db))

        # Check if passing :memory: to factory fails
        with pytest.raises(PipelineStepFailed) as exc:
            p = dlt.pipeline(
                pipeline_name="booboo", destination=dlt.destinations.duckdb(credentials=":memory:")
            )
            p.run([1, 2, 3])

        assert isinstance(exc.value.exception, InvalidInMemoryDuckdbCredentials)

        os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = ":memory:"
        with pytest.raises(PipelineStepFailed):
            p = dlt.pipeline(
                pipeline_name="booboo",
                destination="duckdb",
            )
            p.run([1, 2, 3])

        assert isinstance(exc.value.exception, InvalidInMemoryDuckdbCredentials)

        with pytest.raises(PipelineStepFailed) as exc:
            p = dlt.pipeline(
                pipeline_name="booboo",
                destination=Destination.from_reference("duckdb", credentials=":memory:"),
            )
            p.run([1, 2, 3], table_name="numbers")

        assert isinstance(exc.value.exception, InvalidInMemoryDuckdbCredentials)
    finally:
        delete_quack_db()


def test_duckdb_database_path() -> None:
    # resolve without any path provided
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials._conn_str().lower() == os.path.abspath("quack.duckdb").lower()

    # resolve without any path but with pipeline context
    p = dlt.pipeline(pipeline_name="quack_pipeline")
    # pipeline context must be passed explicitly
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials._conn_str().lower() == os.path.abspath("quack.duckdb").lower()
    # pass explicitly
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        explicit_value={"bound_to_pipeline": p},
    )
    # still cwd
    db_path = os.path.abspath(os.path.join(".", "quack_pipeline.duckdb"))
    assert c.credentials._conn_str().lower() == db_path.lower()

    # must work via factory
    factory_ = dlt.destinations.duckdb(bound_to_pipeline=p)
    c = factory_.configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials._conn_str().lower() == db_path.lower()

    # connect
    try:
        conn = c.credentials.borrow_conn(read_only=False)
        c.credentials.return_conn(conn)
        assert os.path.isfile(db_path)
    finally:
        if os.path.isfile(db_path):
            os.unlink(db_path)

    # must work via pipeline
    duck_p = dlt.pipeline(pipeline_name="quack_pipeline_exp", destination="duckdb")
    db_path = os.path.abspath(os.path.join(".", "quack_pipeline_exp.duckdb"))
    assert duck_p.sql_client().credentials._conn_str().lower() == db_path.lower()

    duck_p = dlt.pipeline(pipeline_name="quack_pipeline_exp", destination=dlt.destinations.duckdb())
    creds_ = duck_p.sql_client().credentials
    assert creds_._conn_str().lower() == db_path.lower()

    # connect
    try:
        conn = creds_.borrow_conn(read_only=False)
        creds_.return_conn(conn)
        assert os.path.isfile(db_path)
    finally:
        if os.path.isfile(db_path):
            os.unlink(db_path)

    # test special :pipeline: path to create in pipeline folder
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials=":pipeline:")._bind_dataset_name(
            dataset_name="test_dataset"
        ),
        explicit_value={"bound_to_pipeline": p},  # not an active pipeline
    )
    db_path = os.path.abspath(os.path.join(p.working_dir, DEFAULT_DUCK_DB_NAME))
    assert c.credentials._conn_str().lower() == db_path.lower()
    # connect
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # provide relative path
    db_path = "test_quack.duckdb"
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials="duckdb:///test_quack.duckdb")._bind_dataset_name(
            dataset_name="test_dataset"
        ),
        explicit_value={"bound_to_pipeline": p},
    )
    assert c.credentials._conn_str().lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # provide absolute path
    db_path = os.path.abspath("abs_test_quack.duckdb")
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials=f"duckdb:///{db_path}")._bind_dataset_name(
            dataset_name="test_dataset",
        ),
        explicit_value={"bound_to_pipeline": p},
    )
    assert os.path.isabs(c.credentials.database)
    assert c.credentials._conn_str().lower() == db_path.lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # set just path as credentials
    db_path = "path_test_quack.duckdb"
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials=db_path)._bind_dataset_name(
            dataset_name="test_dataset"
        ),
        explicit_value={"bound_to_pipeline": p},
    )
    assert c.credentials._conn_str().lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    db_path = os.path.abspath("abs_path_test_quack.duckdb")
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials=db_path)._bind_dataset_name(
            dataset_name="test_dataset"
        ),
        explicit_value={"bound_to_pipeline": p},
    )
    assert os.path.isabs(c.credentials.database)
    assert c.credentials._conn_str().lower() == db_path.lower()
    conn = c.credentials.borrow_conn(read_only=False)
    c.credentials.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # invalid path
    import duckdb

    with pytest.raises(duckdb.IOException):
        c = resolve_configuration(
            DuckDbClientConfiguration(credentials=".")._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
        conn = c.credentials.borrow_conn(read_only=False)


def test_keeps_initial_db_path() -> None:
    db_path = "path_test_quack.duckdb"
    # this must be present in credentials so attach also sees it
    os.environ["CREDENTIALS"] = db_path

    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=dlt.destinations.duckdb())
    print(p.pipelines_dir)
    assert p.state["_local"]["initial_cwd"] == os.path.abspath(os.path.curdir).lower()
    with p.sql_client() as conn:
        # still cwd
        assert conn.credentials._conn_str().lower() == os.path.abspath(db_path).lower()

    # attach the pipeline
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.state["_local"]["initial_cwd"] == os.path.abspath(os.path.curdir).lower()
    with p.sql_client() as conn:
        # still cwd
        assert conn.credentials._conn_str().lower() == os.path.abspath(db_path).lower()

    # now create a new pipeline
    dlt.pipeline(pipeline_name="not_quack", destination="dummy")
    with p.sql_client() as conn:
        # still cwd
        assert conn.credentials._conn_str().lower() == os.path.abspath(db_path).lower()


def test_uses_duckdb_local_path_compat() -> None:
    db_path = "./path_test_quack.duckdb"
    p = dlt.pipeline(pipeline_name="quack_pipeline")
    # old db location is still recognized
    p.set_local_state_val("duckdb_database", os.path.abspath(db_path))
    p = dlt.attach("quack_pipeline", destination="duckdb")
    with p.sql_client() as conn:
        # still cwd
        assert conn.credentials._conn_str().lower() == os.path.abspath(db_path).lower()


def test_drops_pipeline_changes_bound() -> None:
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination="duckdb")
    p.run([1, 2, 3], table_name="p_table")
    p = p.drop()
    assert len(p.dataset().p_table.fetchall()) == 3

    # drops internal duckdb
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duckdb(":pipeline:"))
    p.run([1, 2, 3], table_name="p_table")
    p = p.drop()
    with pytest.raises(DatabaseUndefinedRelation):
        p.dataset().p_table.fetchall()


def test_duckdb_database_delete() -> None:
    db_path = "./path_test_quack.duckdb"
    os.environ["CREDENTIALS"] = db_path

    p = dlt.pipeline(pipeline_name="quack_pipeline", destination="duckdb")
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # attach the pipeline
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.first_run is False
    # drop the database
    os.remove(db_path)
    p = dlt.attach(pipeline_name="quack_pipeline")
    assert p.first_run is False
    assert not os.path.exists(db_path)
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    assert os.path.exists(db_path)


def test_duck_database_path_delete() -> None:
    # delete path
    db_folder = "./db_path"
    os.makedirs(db_folder)
    db_path = f"{db_folder}/path_test_quack.duckdb"
    os.environ["CREDENTIALS"] = db_path

    p = dlt.pipeline(pipeline_name="deep_quack_pipeline", destination="duckdb")
    p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    # attach the pipeline
    p = dlt.attach(pipeline_name="deep_quack_pipeline")
    assert p.first_run is False
    # drop the database
    os.remove(db_path)
    os.rmdir(db_folder)
    p = dlt.attach(pipeline_name="deep_quack_pipeline")
    assert p.first_run is False

    # we won't be able to recreate the database because folder was deleted
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.run([1, 2, 3], table_name="table", dataset_name="dataset")
    assert "No such file or directory" in str(py_ex.value)
    # no database
    assert not os.path.exists(db_path)
    # restore folder, otherwise cleanup fails
    os.makedirs(db_folder)


def test_case_sensitive_database_name() -> None:
    # make case sensitive folder name
    cs_quack = os.path.join(TEST_STORAGE_ROOT, "QuAcK")
    os.makedirs(cs_quack, exist_ok=True)
    db_path = os.path.join(cs_quack, "path_TEST_quack.duckdb")
    p = dlt.pipeline(pipeline_name="NOT_QUAck", destination=duckdb(credentials=db_path))
    with p.sql_client() as conn:
        conn.execute_sql("DESCRIBE;")


def test_external_duckdb_database() -> None:
    import duckdb

    # pass explicit in memory database
    conn = duckdb.connect(":memory:")
    c = resolve_configuration(
        DuckDbClientConfiguration(credentials=conn)._bind_dataset_name(dataset_name="test_dataset")
    )
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
    assert not os.path.exists(":memory:")


def test_default_duckdb_dataset_name() -> None:
    # Check if dataset_name does not collide with pipeline_name
    data = ["a", "b", "c"]
    info = dlt.run(data, destination="duckdb", table_name="data")
    assert_table(cast(dlt.Pipeline, info.pipeline), "data", data, info=info)


def delete_quack_db() -> None:
    if os.path.isfile(DEFAULT_DUCK_DB_NAME):
        os.remove(DEFAULT_DUCK_DB_NAME)
