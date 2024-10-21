import os
import pytest
import datetime  # noqa: I251
from typing import Iterator, Any, Tuple, Type, Union
from threading import Thread, Event
from time import sleep

from dlt.common import pendulum, Decimal
from dlt.common.destination.exceptions import IdentifierTooLongException
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import (
    DatabaseException,
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.sql_client import DBApiCursor, SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.destinations.typing import TNativeConn
from dlt.common.time import ensure_pendulum_datetime, to_py_datetime

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage
from tests.load.utils import (
    yield_client_with_storage,
    prepare_table,
    AWS_BUCKET,
    destinations_configs,
    DestinationTestConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential
TEST_NAMING_CONVENTIONS = (
    "snake_case",
    "tests.common.cases.normalizers.sql_upper",
    "tests.common.cases.normalizers.title_case",
)


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(scope="function")
def client(request, naming) -> Iterator[SqlJobClientBase]:
    param: DestinationTestConfiguration = request.param
    yield from yield_client_with_storage(param.destination_factory())


@pytest.fixture(scope="function")
def naming(request) -> str:
    # NOTE: this fixture is forced by `client` fixture which requires it goes first
    # so sometimes there's no request available
    if hasattr(request, "param"):
        os.environ["SCHEMA__NAMING"] = request.param
        return request.param
    return None


@pytest.mark.parametrize(
    "client",
    destinations_configs(
        # Only databases that support search path or equivalent
        default_sql_configs=True,
        exclude=["mssql", "synapse", "dremio", "clickhouse", "sqlalchemy"],
    ),
    indirect=True,
    ids=lambda x: x.name,
)
def test_sql_client_default_dataset_unqualified(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    curr: DBApiCursor
    # get data from unqualified name
    with client.sql_client.execute_query(
        f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY inserted_at"
    ) as curr:
        columns = [c[0] for c in curr.description]
        data = curr.fetchall()
    assert len(data) > 0

    # get data from qualified name
    load_table = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
    with client.sql_client.execute_query(
        f"SELECT * FROM {load_table} ORDER BY inserted_at"
    ) as curr:
        assert [c[0] for c in curr.description] == columns
        assert curr.fetchall() == data


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_malformed_query_parameters(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    loads_table_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)

    paramstyle = client.sql_client.dbapi.paramstyle
    is_positional = paramstyle in ("qmark", "format")
    placeholder = "?" if paramstyle == "qmark" else "%s"
    # parameters for placeholder will not be provided. the placeholder remains in query
    if is_positional:
        with pytest.raises(DatabaseTransientException) as term_ex:
            with client.sql_client.execute_query(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = {placeholder}"
            ):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

        # too many parameters
        with pytest.raises(DatabaseTransientException) as term_ex:
            with client.sql_client.execute_query(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = {placeholder}",
                pendulum.now(),
                10,
            ):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # unknown named parameter
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with pytest.raises(DatabaseTransientException) as term_ex:
            with client.sql_client.execute_query(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = %(date)s"
            ):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_has_dataset(naming: str, client: SqlJobClientBase) -> None:
    with client.sql_client.with_alternative_dataset_name("not_existing"):
        assert not client.sql_client.has_dataset()
    client.update_stored_schema()
    assert client.sql_client.has_dataset()


@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_create_drop_dataset(naming: str, client: SqlJobClientBase) -> None:
    # client.sql_client.create_dataset()
    # Dataset is already create in fixture, so next time it fails
    with pytest.raises(DatabaseException):
        client.sql_client.create_dataset()
    client.sql_client.drop_dataset()
    with pytest.raises(DatabaseUndefinedRelation):
        client.sql_client.drop_dataset()


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_malformed_execute_parameters(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    loads_table_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)

    paramstyle = client.sql_client.dbapi.paramstyle
    is_positional = paramstyle in ("qmark", "format")
    placeholder = "?" if paramstyle == "qmark" else "%s"
    # parameters for placeholder will not be provided. the placeholder remains in query
    if is_positional:
        with pytest.raises(DatabaseTransientException) as term_ex:
            client.sql_client.execute_sql(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = {placeholder}"
            )
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

        # too many parameters
        with pytest.raises(DatabaseTransientException) as term_ex:
            client.sql_client.execute_sql(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = {placeholder}",
                pendulum.now(),
                10,
            )
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # unknown named parameter
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with pytest.raises(DatabaseTransientException) as term_ex:
            client.sql_client.execute_sql(
                f"SELECT * FROM {loads_table_name} WHERE inserted_at = %(date)s"
            )
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_execute_sql(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    # ask with datetime
    # no_rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", pendulum.now().add(seconds=1))
    # assert len(no_rows) == 0
    version_table_name = client.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
    rows = client.sql_client.execute_sql(
        f"SELECT schema_name, inserted_at FROM {version_table_name}"
    )
    assert len(rows) == 1
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(
        f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE schema_name = %s", "event"
    )
    assert len(rows) == 1
    # print(rows)
    assert rows[0][0] == "event"
    assert isinstance(ensure_pendulum_datetime(rows[0][1]), datetime.datetime)
    assert rows[0][0] == "event"
    # print(rows[0][1])
    # print(type(rows[0][1]))
    # ensure datetime obj to make sure it is supported by dbapi
    inserted_at = to_py_datetime(ensure_pendulum_datetime(rows[0][1]))
    if client.config.destination_name == "sqlalchemy_sqlite":
        # timezone aware datetime is not supported by sqlite
        inserted_at = inserted_at.replace(tzinfo=None)

    rows = client.sql_client.execute_sql(
        f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at = %s",
        inserted_at,
    )
    assert len(rows) == 1
    # use rows in subsequent test
    if client.sql_client.dbapi.paramstyle == "pyformat":
        rows = client.sql_client.execute_sql(
            f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at ="
            " %(date)s",
            date=rows[0][1],
        )
        assert len(rows) == 1
        assert rows[0][0] == "event"
        rows = client.sql_client.execute_sql(
            f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at ="
            " %(date)s",
            date=pendulum.now().add(seconds=1),
        )
        assert len(rows) == 0


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_execute_ddl(client: SqlJobClientBase) -> None:
    uniq_suffix = uniq_id()
    client.update_stored_schema()
    table_name, py_type = prepare_temp_table(client)
    f_q_table_name = client.sql_client.make_qualified_table_name(table_name)
    client.sql_client.execute_sql(f"INSERT INTO {f_q_table_name} VALUES (1.0)")
    rows = client.sql_client.execute_sql(f"SELECT * FROM {f_q_table_name}")
    assert rows[0][0] == py_type("1.0")
    if client.config.destination_type == "dremio":
        username = client.config.credentials["username"]
        view_name = f'"@{username}"."view_tmp_{uniq_suffix}"'
    else:
        # create view, note that bigquery will not let you execute a view that does not have fully qualified table names.
        view_name = client.sql_client.make_qualified_table_name(f"view_tmp_{uniq_suffix}")
    client.sql_client.execute_sql(f"CREATE VIEW {view_name} AS SELECT * FROM {f_q_table_name};")
    rows = client.sql_client.execute_sql(f"SELECT * FROM {view_name}")
    assert rows[0][0] == py_type("1.0")


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_execute_query(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    version_table_name = client.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
    with client.sql_client.execute_query(
        f"SELECT schema_name, inserted_at FROM {version_table_name}"
    ) as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
    with client.sql_client.execute_query(
        f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE schema_name = %s", "event"
    ) as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
        assert isinstance(ensure_pendulum_datetime(rows[0][1]), datetime.datetime)
    with client.sql_client.execute_query(
        f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at = %s",
        rows[0][1],
    ) as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
    with client.sql_client.execute_query(
        f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at = %s",
        to_py_datetime(pendulum.now().add(seconds=1)),
    ) as curr:
        rows = curr.fetchall()
        assert len(rows) == 0
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with client.sql_client.execute_query(
            f"SELECT schema_name, inserted_at FROM {version_table_name} WHERE inserted_at ="
            " %(date)s",
            date=to_py_datetime(pendulum.now().add(seconds=1)),
        ) as curr:
            rows = curr.fetchall()
            assert len(rows) == 0


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_execute_df(client: SqlJobClientBase) -> None:
    if client.config.destination_type == "bigquery":
        chunk_size = 50
        total_records = 80
    elif client.config.destination_type == "mssql":
        chunk_size = 700
        total_records = 1000
    else:
        chunk_size = 2048
        total_records = 3000

    client.update_stored_schema()
    table_name, py_type = prepare_temp_table(client)
    f_q_table_name = client.sql_client.make_qualified_table_name(table_name)

    if client.capabilities.insert_values_writer_type == "default":
        insert_query = ",".join([f"({idx})" for idx in range(0, total_records)])
        sql_stmt = f"INSERT INTO {f_q_table_name} VALUES {insert_query};"
    elif client.capabilities.insert_values_writer_type == "select_union":
        insert_query = " UNION ALL ".join([f"SELECT {idx}" for idx in range(0, total_records)])
        sql_stmt = f"INSERT INTO {f_q_table_name} {insert_query};"

    client.sql_client.execute_sql(sql_stmt)
    with client.sql_client.execute_query(
        f"SELECT * FROM {f_q_table_name} ORDER BY col ASC"
    ) as curr:
        df = curr.df()
        # Force lower case df columns, snowflake has all cols uppercase
        df.columns = [dfcol.lower() for dfcol in df.columns]
        assert list(df["col"]) == list(range(0, total_records))
    # get chunked
    with client.sql_client.execute_query(
        f"SELECT * FROM {f_q_table_name} ORDER BY col ASC"
    ) as curr:
        # be compatible with duckdb vector size
        iterator = curr.iter_df(chunk_size)
        df_1 = next(iterator)
        df_2 = next(iterator)
        try:
            df_3 = next(iterator)
        except StopIteration:
            df_3 = None
        # Force lower case df columns, snowflake has all cols uppercase
        for df in [df_1, df_2, df_3]:
            if df is not None:
                df.columns = [dfcol.lower() for dfcol in df.columns]

    assert list(df_1["col"]) == list(range(0, chunk_size))
    assert list(df_2["col"]) == list(range(chunk_size, total_records))
    assert df_3 is None


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_database_exceptions(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    term_ex: Any
    # invalid table
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        with client.sql_client.execute_query("SELECT * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        with client.sql_client.execute_query("DELETE FROM TABLE_XXX WHERE 1=1"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        with client.sql_client.execute_query("DROP TABLE TABLE_XXX"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        client.sql_client.execute_many(
            [
                "DELETE FROM TABLE_XXX WHERE 1=1;",
                "DELETE FROM ticket_forms__ticket_field_ids WHERE 1=1;",
            ]
        )
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        client.sql_client.execute_many(
            ["DROP TABLE TABLE_XXX;", "DROP TABLE ticket_forms__ticket_field_ids;"]
        )

    # invalid syntax
    with pytest.raises(DatabaseTransientException) as term_ex:
        with client.sql_client.execute_query("SELECTA * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid column
    with pytest.raises(DatabaseTerminalException) as term_ex:
        loads_table_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        with client.sql_client.execute_query(
            f"SELECT * FROM {loads_table_name} ORDER BY column_XXX"
        ):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid parameters to dbapi
    # with pytest.raises(DatabaseTransientException) as term_ex:
    #     with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s", b'XXXX'):
    #         pass
    # assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid schema/dataset
    with client.sql_client.with_alternative_dataset_name("UNKNOWN"):
        qualified_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        with pytest.raises(DatabaseUndefinedRelation) as term_ex:
            with client.sql_client.execute_query(
                f"SELECT * FROM {qualified_name} ORDER BY inserted_at"
            ):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
        with pytest.raises(DatabaseUndefinedRelation) as term_ex:
            with client.sql_client.execute_query(f"DELETE FROM {qualified_name} WHERE 1=1"):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
        if client.config.destination_type not in ["dremio", "clickhouse"]:
            with pytest.raises(DatabaseUndefinedRelation) as term_ex:
                client.sql_client.drop_dataset()
            assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["databricks"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_commit_transaction(client: SqlJobClientBase) -> None:
    table_name, py_type = prepare_temp_table(client)
    f_q_table_name = client.sql_client.make_qualified_table_name(table_name)
    with client.sql_client.begin_transaction():
        client.sql_client.execute_sql(f"INSERT INTO {f_q_table_name} VALUES (%s)", py_type("1.0"))
        # check row still in transaction
        rows = client.sql_client.execute_sql(
            f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
        )
        assert len(rows) == 1
    # check row after commit
    rows = client.sql_client.execute_sql(
        f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
    )
    assert len(rows) == 1
    assert rows[0][0] == 1.0
    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(
            f"DELETE FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
        )
        # explicit commit
        tx.commit_transaction()
    rows = client.sql_client.execute_sql(
        f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
    )
    assert len(rows) == 0


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["databricks"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_rollback_transaction(client: SqlJobClientBase) -> None:
    if client.capabilities.supports_transactions is False:
        pytest.skip("Destination does not support tx")
    table_name, py_type = prepare_temp_table(client)
    f_q_table_name = client.sql_client.make_qualified_table_name(table_name)
    # test python exception
    with pytest.raises(RuntimeError):
        with client.sql_client.begin_transaction():
            client.sql_client.execute_sql(
                f"INSERT INTO {f_q_table_name} VALUES (%s)", py_type("1.0")
            )
            rows = client.sql_client.execute_sql(
                f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
            )
            assert len(rows) == 1
            # python exception triggers rollback
            raise RuntimeError("ROLLBACK")
    rows = client.sql_client.execute_sql(
        f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
    )
    assert len(rows) == 0

    # test rollback on invalid query
    f_q_wrong_table_name = client.sql_client.make_qualified_table_name(f"{table_name}_X")
    with pytest.raises(DatabaseException):
        with client.sql_client.begin_transaction():
            client.sql_client.execute_sql(
                f"INSERT INTO {f_q_table_name} VALUES (%s)", py_type("1.0")
            )
            # table does not exist
            client.sql_client.execute_sql(
                f"SELECT col FROM {f_q_wrong_table_name} WHERE col = %s", py_type("1.0")
            )
    rows = client.sql_client.execute_sql(
        f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
    )
    assert len(rows) == 0

    # test explicit rollback
    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(f"INSERT INTO {f_q_table_name} VALUES (%s)", py_type("1.0"))
        tx.rollback_transaction()
        rows = client.sql_client.execute_sql(
            f"SELECT col FROM {f_q_table_name} WHERE col = %s", py_type("1.0")
        )
        assert len(rows) == 0

    # test double rollback - behavior inconsistent across databases (some raise some not)
    # with pytest.raises(DatabaseException):
    # with client.sql_client.begin_transaction() as tx:
    #     client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
    #     tx.rollback_transaction()
    #     tx.rollback_transaction()


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["databricks"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_transaction_isolation(client: SqlJobClientBase) -> None:
    if client.capabilities.supports_transactions is False:
        pytest.skip("Destination does not support tx")
    if client.config.destination_name == "sqlalchemy_sqlite":
        # because other schema names must be attached for each connection
        client.sql_client.dataset_name = "main"
    table_name, py_type = prepare_temp_table(client)
    f_q_table_name = client.sql_client.make_qualified_table_name(table_name)
    event = Event()
    event.clear()

    def test_thread(thread_id: Union[Decimal, float]) -> None:
        # make a copy of the sql_client
        thread_client = client.sql_client.__class__(
            client.sql_client.dataset_name,
            client.sql_client.staging_dataset_name,
            client.sql_client.credentials,
            client.capabilities,
        )
        with thread_client:
            with thread_client.begin_transaction():
                thread_client.execute_sql(f"INSERT INTO {f_q_table_name} VALUES (%s)", thread_id)
                event.wait()

    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(f"INSERT INTO {f_q_table_name} VALUES (%s)", py_type("1.0"))
        t = Thread(target=test_thread, daemon=True, args=(py_type("2.0"),))
        t.start()
        # thread 2.0 inserts
        sleep(3.0)
        # main thread rollback
        tx.rollback_transaction()
        # thread 2.0 commits
        event.set()
        t.join()

    # just in case close the connection
    if (
        client.config.destination_name != "sqlalchemy_sqlite"
    ):  # keep sqlite connection to maintain attached datasets
        client.sql_client.close_connection()
        # re open connection
        client.sql_client.open_connection()
    rows = client.sql_client.execute_sql(f"SELECT col FROM {f_q_table_name} ORDER BY col")
    assert len(rows) == 1
    # only thread 2 is left
    assert rows[0][0] == py_type("2.0")


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["sqlalchemy"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_max_table_identifier_length(client: SqlJobClientBase) -> None:
    if client.capabilities.max_identifier_length >= 65536:
        pytest.skip(
            f"destination {client.config.destination_type} has no table name length restriction"
        )
    table_name = (
        8
        * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations"
    )
    with pytest.raises(IdentifierTooLongException) as py_ex:
        prepare_table(client, "long_table_name", table_name, make_uniq_table=False)
    assert py_ex.value.identifier_type == "table"
    assert py_ex.value.identifier_name == table_name
    # remove the table from the schema so further tests are not affected.
    client.schema.tables.pop(table_name)

    # each database handles the too long identifier differently
    # postgres and redshift trim the names on create
    # interestingly postgres is also trimming the names in queries so the table "exists"
    # BQ is failing on the HTTP protocol level

    # exists, _ = client.get_storage_table(long_table_name)
    # assert exists is (client.config.destination_type == "postgres")
    # exists, table_def = client.get_storage_table(long_table_name[:client.capabilities.max_identifier_length])
    # assert exists is True


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["sqlalchemy"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_max_column_identifier_length(client: SqlJobClientBase) -> None:
    if client.capabilities.max_column_identifier_length >= 65536:
        pytest.skip(
            f"destination {client.config.destination_type} has no column name length restriction"
        )
    table_name = "prospects_external_data__data365_member__member"
    column_name = (
        7
        * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    )
    with pytest.raises(IdentifierTooLongException) as py_ex:
        prepare_table(client, "long_column_name", table_name, make_uniq_table=False)
    assert py_ex.value.identifier_type == "column"
    assert py_ex.value.identifier_name == f"{table_name}.{column_name}"
    # remove the table from the schema so further tests are not affected
    client.schema.tables.pop(table_name)
    # long_column_name = 7 * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    # assert long_column_name not in table_def
    # assert long_column_name[:client.capabilities.max_column_identifier_length] in table_def


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=["databricks", "sqlalchemy"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_recover_on_explicit_tx(client: SqlJobClientBase) -> None:
    if client.capabilities.supports_transactions is False:
        pytest.skip("Destination does not support tx")
    client.schema._bump_version()
    client.update_stored_schema()
    version_table = client.sql_client.make_qualified_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(DatabaseTransientException):
        client.sql_client.execute_sql(sql)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "ProgrammingError")
    # still can execute dml and selects
    assert client.get_stored_schema(client.schema.name) is not None
    client.complete_load("ABC")
    assert_load_id(client.sql_client, "ABC")

    # syntax error within tx
    statements = ["BEGIN TRANSACTION;", f"INVERT INTO {version_table} VALUES(1);", "COMMIT;"]
    with pytest.raises(DatabaseTransientException):
        client.sql_client.execute_many(statements)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "ProgrammingError")
    assert client.get_stored_schema(client.schema.name) is not None
    client.complete_load("EFG")
    assert_load_id(client.sql_client, "EFG")

    # wrong value inserted
    statements = [
        "BEGIN TRANSACTION;",
        f"INSERT INTO {version_table}(version) VALUES(1);",
        "COMMIT;",
    ]
    # cannot insert NULL value
    with pytest.raises(DatabaseTerminalException):
        client.sql_client.execute_many(statements)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "IntegrityError")
    # assert isinstance(term_ex.value.dbapi_exception, (psycopg2.InternalError, psycopg2.))
    assert client.get_stored_schema(client.schema.name) is not None
    client.complete_load("HJK")
    assert_load_id(client.sql_client, "HJK")


def assert_load_id(sql_client: SqlClientBase[TNativeConn], load_id: str) -> None:
    # and data is actually committed when connection reopened
    sql_client.close_connection()
    sql_client.open_connection()
    loads_table = sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
    rows = sql_client.execute_sql(f"SELECT load_id FROM {loads_table} WHERE load_id = %s", load_id)
    assert len(rows) == 1


def prepare_temp_table(client: SqlJobClientBase) -> Tuple[str, Type[Union[Decimal, float]]]:
    """Return the table name and py type of value to insert"""
    uniq_suffix = uniq_id()
    table_name = f"tmp_{uniq_suffix}"
    ddl_suffix = ""
    coltype = "numeric"
    py_type: Union[Type[Decimal], Type[float]] = Decimal
    if client.config.destination_type == "athena":
        ddl_suffix = (
            f"LOCATION '{AWS_BUCKET}/ci/{table_name}' TBLPROPERTIES ('table_type'='ICEBERG',"
            " 'format'='parquet');"
        )
        coltype = "bigint"
        qualified_table_name = table_name
    elif client.config.destination_name == "sqlalchemy_sqlite":
        coltype = "float"
        py_type = float
        qualified_table_name = client.sql_client.make_qualified_table_name(table_name)
    elif client.config.destination_type == "clickhouse":
        ddl_suffix = "ENGINE = MergeTree() ORDER BY col"
        qualified_table_name = client.sql_client.make_qualified_table_name(table_name)
    else:
        qualified_table_name = client.sql_client.make_qualified_table_name(table_name)
    client.sql_client.execute_sql(
        f"CREATE TABLE {qualified_table_name} (col {coltype}) {ddl_suffix};"
    )
    return table_name, py_type
