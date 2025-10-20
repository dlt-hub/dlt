import os
import pytest
from typing import Any, Iterator, List, cast

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.destination import Destination
from dlt.common.known_env import DLT_LOCAL_DIR
from dlt.common.utils import set_working_dir, uniq_id

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DuckDbCredentials,
)
from dlt.destinations import duckdb
from dlt.destinations.impl.duckdb.exceptions import InvalidInMemoryDuckdbCredentials
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.common.configuration.utils import toml_providers as toml_providers
from tests.pipeline.utils import assert_table_column
from tests.utils import TEST_STORAGE_ROOT

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def run_in_storage(
    autouse_test_storage, toml_providers: ConfigProvidersContainer
) -> Iterator[None]:
    with set_working_dir("_storage"):
        yield


def test_duckdb_open_conn_default() -> None:
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    # default name: destination_type.duckdb == duckdb.duckdb
    assert c.credentials.database == os.path.abspath("duckdb.duckdb")
    # print(str(c.credentials))
    # print(str(os.getcwd()))
    # print(get_resolved_traces())
    conn = c.credentials.conn_pool.borrow_conn()
    assert c.credentials.conn_pool._conn_borrows == 1
    assert c.credentials.conn_pool._conn_owner is True
    # return conn
    c.credentials.conn_pool.return_conn(conn)
    # connection destroyed
    assert c.credentials.conn_pool._conn_borrows == 0
    assert c.credentials.conn_pool._conn_owner is True
    assert c.credentials.conn_pool._conn is None
    # db file is created
    assert os.path.isfile("duckdb.duckdb")


@pytest.mark.no_load
def test_duckdb_connection_config() -> None:
    import duckdb

    # toml_providers injected custom config from ./tests/common/cases/configuration/.dlt (see auto fixture above)
    credentials = resolve_configuration(
        DuckDbCredentials(), sections=("destination", "duckdb_configured")
    )
    assert credentials.database == "localx.duckdb"
    assert credentials.extensions == ["spatial"]
    assert credentials.pragmas == ["enable_progress_bar"]  # "enable_logging"
    assert credentials.global_config == {"azure_transport_option_type": True}
    assert credentials.local_config == {"errors_as_json": True}

    # install spatial
    duckdb.sql("INSTALL spatial;")

    def _read_config(conn: duckdb.DuckDBPyConnection) -> List[Any]:
        rel = conn.sql("""
            SELECT name, value
            FROM duckdb_settings()
            WHERE name IN ('azure_transport_option_type', 'enable_progress_bar', 'errors_as_json')
            ORDER BY name ASC;
            """)
        return rel.fetchall()

    # check if set
    conn = credentials.conn_pool.borrow_conn()
    try:
        # all settings enabled
        assert _read_config(conn) == [
            ("azure_transport_option_type", "true"),
            ("enable_progress_bar", "true"),
            ("errors_as_json", "true"),
        ]
        # make cursor (new session)
        cur = conn.cursor()
        # local settings are gone
        assert _read_config(cur) == [
            ("azure_transport_option_type", "true"),
            ("enable_progress_bar", "false"),
            ("errors_as_json", "false"),
        ]
        cur.close()
        # settings persisted via borrow
        cur = credentials.conn_pool.borrow_conn()
        assert _read_config(conn) == [
            ("azure_transport_option_type", "true"),
            ("enable_progress_bar", "true"),
            ("errors_as_json", "true"),
        ]
        credentials.conn_pool.return_conn(cur)
    finally:
        credentials.conn_pool.return_conn(conn)
    assert credentials.conn_pool._conn_borrows == 0

    # check if borrow overwrites
    conn = credentials.conn_pool.borrow_conn(
        global_config={"azure_transport_option_type": False},
        local_config={"errors_as_json": False},
        pragmas=["disable_progress_bar"],
    )
    try:
        # all settings disabled
        assert _read_config(conn) == [
            ("azure_transport_option_type", "false"),
            ("enable_progress_bar", "false"),
            ("errors_as_json", "false"),
        ]
    finally:
        credentials.conn_pool.return_conn(conn)
    assert credentials.conn_pool._conn_borrows == 0

    # check credentials init for external connection
    ext_conn = duckdb.connect()
    credentials = DuckDbCredentials(
        ext_conn,
        extensions=["spatial"],
        global_config={"azure_transport_option_type": True},
        local_config={"errors_as_json": True},
        pragmas=["enable_progress_bar"],
    )
    credentials.resolve()
    assert credentials.conn_pool._conn_owner is False
    conn = credentials.conn_pool.borrow_conn()
    assert credentials.conn_pool._conn_owner is False
    try:
        # all settings disabled
        assert _read_config(conn) == [
            ("azure_transport_option_type", "true"),
            ("enable_progress_bar", "true"),
            ("errors_as_json", "true"),
        ]
    finally:
        credentials.conn_pool.return_conn(conn)
        ext_conn.close()

    # check if move conn keeps config
    credentials = resolve_configuration(
        DuckDbCredentials(), sections=("destination", "duckdb_configured")
    )
    borrowed = credentials.conn_pool.borrow_conn()
    # take connection out
    conn = credentials.conn_pool.move_conn()
    try:
        # all settings enabled, including local config
        assert _read_config(conn) == [
            ("azure_transport_option_type", "true"),
            ("enable_progress_bar", "true"),
            ("errors_as_json", "true"),
        ]
    finally:
        # should not close conn
        assert credentials.conn_pool.return_conn(borrowed) == 0
        credentials.conn_pool.__del__()
        conn.sql("SHOW TABLES;")
        # close the connection, otherwise it leaks to the next test
        conn.close()


def test_sql_client_config() -> None:
    import duckdb as _duckdb

    # install spatial
    _duckdb.sql("INSTALL spatial;")

    def _read_config(conn: _duckdb.DuckDBPyConnection) -> List[Any]:
        rel = conn.sql("""
            SELECT name, value
            FROM duckdb_settings()
            WHERE name IN ('azure_transport_option_type', 'enable_progress_bar', 'errors_as_json', 'search_path', 'checkpoint_threshold', 'TimeZone')
            ORDER BY name ASC;
            """)
        return rel.fetchall()

    dest_ = duckdb(destination_name="duckdb_configured")
    pipeline = dlt.pipeline("test_sql_client_config", destination=dest_)
    c: DuckDbSqlClient
    with pipeline.destination_client() as job:
        assert job.config.destination_name == "duckdb_configured"
        with job.sql_client as c:  # type: ignore[attr-defined]
            # no search path because dataset is not present
            assert _read_config(c.native_connection) == [
                ("TimeZone", "UTC"),
                ("azure_transport_option_type", "true"),
                ("checkpoint_threshold", "953.6 MiB"),
                ("enable_progress_bar", "true"),
                ("errors_as_json", "true"),
                ("search_path", ""),
            ]
            # create dataset
            c.create_dataset()
            with pipeline.sql_client() as c2:
                assert _read_config(c2.native_connection) == [
                    ("TimeZone", "UTC"),
                    ("azure_transport_option_type", "true"),
                    ("checkpoint_threshold", "953.6 MiB"),
                    ("enable_progress_bar", "true"),
                    ("errors_as_json", "true"),
                    ("search_path", "test_sql_client_config_dataset"),
                ]

            c.native_connection.sql(
                "SELECT count(1) FROM duckdb_logs where message LIKE"
                " '%enable_checkpoint_on_shutdown%';"
            ).fetchall()

            # TODO: had to disable this test after downgrade to 1.2.1 (logs available on 1.3)
            # 5 calls to pragma (two for each connection, log is common)
            # assert cnt[0][0] == 5
            # # one call to load extension (from second connection, we enabled logging after the first LOAD)
            # cnt = c.native_connection.sql(
            #     "SELECT count(1) FROM duckdb_logs where message LIKE '%spatial%';"
            # ).fetchall()
            # assert cnt[0][0] == 1


@pytest.mark.no_load
def test_destination_credentials_with_config() -> None:
    import duckdb
    from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

    # install spatial
    duckdb.sql("INSTALL spatial;")

    os.environ["DESTINATION__DUCKDB__CREDENTIALS__PRAGMAS"] = '["enable_profiling"]'

    plan_path = os.path.abspath("plan.txt")
    dest_ = dlt.destinations.duckdb(
        DuckDbCredentials(
            "duck.db", extensions=["spatial"], local_config={"profiling_output": plan_path}
        )
    )
    pipeline = dlt.pipeline("test_destination_credentials_with_config", destination=dest_)
    c: DuckDbSqlClient
    with pipeline.sql_client() as c:  # type: ignore[assignment]
        # logging not enabled on 1.2.1
        assert c.native_connection.sql("SELECT count(1) FROM duckdb_logs").fetchall()[0][0] == 0
    # check if query plan is created so pragma is working
    assert os.path.isfile(plan_path)


def test_credentials_wrong_config() -> None:
    import duckdb

    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(global_config={"wrong_conf": 0})
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(local_config={"wrong_conf": 0})
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    with pytest.raises(duckdb.CatalogException):
        c.credentials.conn_pool.borrow_conn(pragmas=["unkn_pragma"])
    # connection closed
    assert c.credentials.conn_pool._conn is None
    assert c.credentials.conn_pool._conn_borrows == 0

    # open and borrow conn
    conn = c.credentials.conn_pool.borrow_conn()
    assert c.credentials.conn_pool._conn_borrows == 1
    try:
        with pytest.raises(duckdb.CatalogException):
            c.credentials.conn_pool.borrow_conn(pragmas=["unkn_pragma"])
        assert c.credentials.conn_pool._conn is not None
        # refcount not increased
        assert c.credentials.conn_pool._conn_borrows == 1
    finally:
        conn.close()

    # check extensions fail
    os.environ["CREDENTIALS__EXTENSIONS"] = '["unk_extension"]'
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    with pytest.raises(duckdb.IOException):
        c.credentials.conn_pool.borrow_conn()
    assert not hasattr(c.credentials, "_conn")
    assert c.credentials.conn_pool._conn_borrows == 0


@pytest.mark.no_load
def test_duckdb_in_memory_mode_via_factory():
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


def test_duckdb_database_path() -> None:
    # resolve without any path provided
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials._conn_str().lower() == os.path.abspath("duckdb.duckdb").lower()

    # resolve without any path but with pipeline context
    p = dlt.pipeline(pipeline_name="quack_pipeline")
    # pipeline context must be passed explicitly
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    assert c.credentials._conn_str().lower() == os.path.abspath("duckdb.duckdb").lower()
    # pass explicitly
    c = resolve_configuration(
        p._bind_local_files(
            DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
        )
    )
    # still cwd
    db_path = os.path.abspath(os.path.join(".", "quack_pipeline.duckdb"))
    assert c.credentials._conn_str().lower() == db_path.lower()

    # must work via factory
    factory_ = dlt.destinations.duckdb()
    c = factory_.configuration(
        p._bind_local_files(
            DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
        )
    )
    assert c.credentials._conn_str().lower() == db_path.lower()

    # connect
    try:
        conn = c.credentials.conn_pool.borrow_conn()
        c.credentials.conn_pool.return_conn(conn)
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
        conn = creds_.conn_pool.borrow_conn()
        creds_.conn_pool.return_conn(conn)
        assert os.path.isfile(db_path)
    finally:
        if os.path.isfile(db_path):
            os.unlink(db_path)

    # test special :pipeline: path to create in pipeline folder
    c = resolve_configuration(
        # not an active pipeline
        p._bind_local_files(
            DuckDbClientConfiguration(credentials=":pipeline:")._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
    )
    db_path = os.path.abspath(os.path.join(p.working_dir, p.pipeline_name + ".duckdb"))
    assert c.credentials._conn_str().lower() == db_path.lower()
    # connect
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # provide relative path
    db_path = "test_quack.duckdb"
    c = resolve_configuration(
        p._bind_local_files(
            DuckDbClientConfiguration(credentials="duckdb:///test_quack.duckdb")._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
    )
    assert c.credentials._conn_str().lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # provide absolute path
    db_path = os.path.abspath("abs_test_quack.duckdb")
    c = resolve_configuration(
        p._bind_local_files(
            DuckDbClientConfiguration(credentials=f"duckdb:///{db_path}")._bind_dataset_name(
                dataset_name="test_dataset",
            )
        )
    )
    assert os.path.isabs(c.credentials.database)
    assert c.credentials._conn_str().lower() == db_path.lower()
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    # set just path as credentials
    db_path = "path_test_quack.duckdb"
    c = resolve_configuration(
        p._bind_local_files(
            DuckDbClientConfiguration(credentials=db_path)._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
    )
    assert c.credentials._conn_str().lower() == os.path.abspath(db_path).lower()
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
    assert os.path.isfile(db_path)
    p = p.drop()

    db_path = os.path.abspath("abs_path_test_quack.duckdb")
    c = resolve_configuration(
        p._bind_local_files(
            DuckDbClientConfiguration(credentials=db_path)._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
    )
    assert os.path.isabs(c.credentials.database)
    assert c.credentials._conn_str().lower() == db_path.lower()
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
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
        conn = c.credentials.conn_pool.borrow_conn()


def test_named_destination_path() -> None:
    # if destination is named, its name is always used
    c = resolve_configuration(
        DuckDbClientConfiguration(destination_name="named")._bind_dataset_name(
            dataset_name="test_dataset"
        )
    )
    assert c.credentials._conn_str().lower() == os.path.abspath("named.duckdb").lower()
    # even if part of of pipeline
    named_duck = dlt.destinations.duckdb(destination_name="named")
    pipeline = dlt.pipeline(pipeline_name="quack_pipeline", destination=named_duck)
    c = resolve_configuration(
        pipeline._bind_local_files(
            DuckDbClientConfiguration(destination_name="named")._bind_dataset_name(
                dataset_name="test_dataset"
            )
        )
    )
    db_path = os.path.abspath("named.duckdb")
    assert c.credentials._conn_str().lower() == db_path.lower()
    conn = c.credentials.conn_pool.borrow_conn()
    c.credentials.conn_pool.return_conn(conn)
    assert os.path.isfile(db_path)


def test_db_path_follows_local_dir() -> None:
    local_dir = os.path.join(TEST_STORAGE_ROOT, uniq_id())
    os.makedirs(local_dir)
    # mock tmp dir
    os.environ[DLT_LOCAL_DIR] = local_dir
    # we expect duckdb db to appear there
    c = resolve_configuration(
        DuckDbClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(local_dir, "duckdb.duckdb")
    assert c.credentials._conn_str().lower() == os.path.abspath(db_path).lower()


@pytest.mark.no_load
def test_keeps_initial_db_path() -> None:
    db_path = "path_test_quack.duckdb"
    # this must be present in credentials so attach also sees it
    os.environ["CREDENTIALS"] = db_path

    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=dlt.destinations.duckdb())
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

    # now create and activate a new pipeline
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
    p.sync_destination()
    assert len(p.dataset().p_table.fetchall()) == 3

    # drops internal duckdb
    p = dlt.pipeline(pipeline_name="quack_pipeline", destination=duckdb(":pipeline:"))
    p.run([1, 2, 3], table_name="p_table")
    p = p.drop()
    p.sync_destination()
    with pytest.raises(DatabaseUndefinedRelation):
        with p.sql_client() as conn:
            conn.execute_sql("SELECT * FROM p_table")


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
    assert c.credentials.conn_pool._conn_borrows == 0
    assert c.credentials.conn_pool._conn is conn
    int_conn = c.credentials.conn_pool.borrow_conn()
    assert c.credentials.conn_pool._conn_borrows == 1
    assert c.credentials.conn_pool._conn_owner is False
    c.credentials.conn_pool.return_conn(int_conn)
    assert c.credentials.conn_pool._conn_borrows == 0
    assert c.credentials.conn_pool._conn_owner is False
    assert c.credentials.conn_pool._conn is not None
    conn.close()
    assert not os.path.exists(":memory:")


def test_default_duckdb_dataset_name() -> None:
    # drop previous pipeline from container
    dlt.current.pipeline().deactivate()

    # Check if dataset_name does not collide with pipeline_name
    data = ["a", "b", "c"]
    info = dlt.run(data, destination="duckdb", table_name="data")
    assert_table_column(cast(dlt.Pipeline, info.pipeline), "data", data, info=info)
