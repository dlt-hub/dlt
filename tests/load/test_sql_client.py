import pytest
import datetime  # noqa: I251
from typing import Iterator
from threading import Thread, Event
from time import sleep

from dlt.common import pendulum, Decimal
from dlt.common.exceptions import IdentifierTooLongException
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.utils import derives_from_class_of_name, uniq_id
from dlt.destinations.exceptions import DatabaseException, DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation

from dlt.destinations.sql_client import DBApiCursor, SqlClientBase
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage
from tests.load.utils import yield_client_with_storage, prepare_table, ALL_CLIENTS


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(scope="module")
def redshift_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("redshift")


@pytest.fixture(scope="module")
def bigquery_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("bigquery")


@pytest.fixture(scope="module")
def postgres_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("postgres")


# recreate client every test
@pytest.fixture(scope="function")
def duckdb_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("duckdb")


@pytest.fixture(scope="function")
def client(request) -> SqlJobClientBase:
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_sql_client_default_dataset_unqualified(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    curr: DBApiCursor
    # get data from unqualified name
    with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY inserted_at") as curr:
        columns = [c[0] for c in curr.description]
        data = curr.fetchall()
    assert len(data) > 0

    # get data from qualified name
    load_table = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
    with client.sql_client.execute_query(f"SELECT * FROM {load_table} ORDER BY inserted_at") as curr:
        assert [c[0] for c in curr.description] == columns
        assert curr.fetchall() == data


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_malformed_query_parameters(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    # parameters for placeholder will not be provided. the placeholder remains in query
    with pytest.raises(DatabaseTransientException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # too many parameters
    with pytest.raises(DatabaseTransientException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s", pendulum.now(), 10):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # unknown named parameter
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with pytest.raises(DatabaseTransientException) as term_ex:
            with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %(date)s"):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_malformed_execute_parameters(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    # parameters for placeholder will not be provided. the placeholder remains in query
    with pytest.raises(DatabaseTransientException) as term_ex:
        client.sql_client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s")
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # too many parameters
    with pytest.raises(DatabaseTransientException) as term_ex:
        client.sql_client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s", pendulum.now(), 10)
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)

    # unknown named parameter
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with pytest.raises(DatabaseTransientException) as term_ex:
            client.sql_client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %(date)s")
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_execute_sql(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    # ask with datetime
    no_rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", pendulum.now().add(seconds=1))
    assert len(no_rows) == 0
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME}")
    assert len(rows) == 1
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE schema_name = %s", "event")
    assert len(rows) == 1
    assert rows[0][0] == "event"
    assert isinstance(rows[0][1], datetime.datetime)
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", rows[0][1])
    assert len(rows) == 1
    # use rows in subsequent test
    if client.sql_client.dbapi.paramstyle == "pyformat":
        rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=rows[0][1])
        assert len(rows) == 1
        assert rows[0][0] == "event"
        rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=pendulum.now().add(seconds=1))
        assert len(rows) == 0


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_execute_ddl(client: SqlJobClientBase) -> None:
    uniq_suffix = uniq_id()
    client.update_storage_schema()
    client.sql_client.execute_sql(f"CREATE TABLE tmp_{uniq_suffix} (col NUMERIC);")
    client.sql_client.execute_sql(f"INSERT INTO tmp_{uniq_suffix} VALUES (1.0)")
    rows = client.sql_client.execute_sql(f"SELECT * FROM tmp_{uniq_suffix}")
    assert rows[0][0] == Decimal("1.0")
    # create view, note that bigquery will not let you execute a view that does not have fully qualified table names.
    f_q_table_name = client.sql_client.make_qualified_table_name(f"tmp_{uniq_suffix}")
    client.sql_client.execute_sql(f"CREATE OR REPLACE VIEW view_tmp_{uniq_suffix} AS (SELECT * FROM {f_q_table_name});")
    rows = client.sql_client.execute_sql(f"SELECT * FROM view_tmp_{uniq_suffix}")
    assert rows[0][0] == Decimal("1.0")


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_execute_query(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME}") as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE schema_name = %s", "event") as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
        assert isinstance(rows[0][1], datetime.datetime)
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", rows[0][1]) as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", pendulum.now().add(seconds=1)) as curr:
        rows = curr.fetchall()
        assert len(rows) == 0
    if client.sql_client.dbapi.paramstyle == "pyformat":
        with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=pendulum.now().add(seconds=1)) as curr:
            rows = curr.fetchall()
            assert len(rows) == 0


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_execute_df(client: SqlJobClientBase) -> None:
    if client.config.destination_name == "bigquery":
        chunk_size = 50
        total_records = 80
    else:
        chunk_size = 2048
        total_records = 3000

    uniq_suffix = uniq_id()
    client.update_storage_schema()
    client.sql_client.execute_sql(f"CREATE TABLE tmp_{uniq_suffix} (col INT);")
    insert_query = ",".join([f"({idx})" for idx in range(0, total_records)])

    client.sql_client.execute_sql(f"INSERT INTO tmp_{uniq_suffix} VALUES {insert_query};")
    with client.sql_client.execute_query(f"SELECT * FROM tmp_{uniq_suffix} ORDER BY col ASC") as curr:
        df = curr.df()
        assert list(df["col"]) == list(range(0, total_records))
    # get chunked
    with client.sql_client.execute_query(f"SELECT * FROM tmp_{uniq_suffix} ORDER BY col ASC") as curr:
        # be compatible with duckdb vector size
        df_1 = curr.df(chunk_size=chunk_size)
        df_2 = curr.df(chunk_size=chunk_size)
        df_3 = curr.df(chunk_size=chunk_size)
    assert list(df_1["col"]) == list(range(0, chunk_size))
    assert list(df_2["col"]) == list(range(chunk_size, total_records))
    assert df_3 is None


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_database_exceptions(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    # invalid table
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        with client.sql_client.execute_query("SELECT * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid syntax
    with pytest.raises(DatabaseTransientException) as term_ex:
        with client.sql_client.execute_query("SELECTA * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid column
    with pytest.raises(DatabaseTerminalException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY column_XXX"):
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
            with client.sql_client.execute_query(f"SELECT * FROM {qualified_name} ORDER BY inserted_at"):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_commit_transaction(client: SqlJobClientBase) -> None:
    table_name = prepare_temp_table(client.sql_client)
    with client.sql_client.begin_transaction():
        client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
        # check row still in transaction
        rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
        assert len(rows) == 1
    # check row after commit
    rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
    assert len(rows) == 1
    assert rows[0][0] == 1.0
    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(f"DELETE FROM {table_name} WHERE col = %s", Decimal("1.0"))
        # explicit commit
        tx.commit_transaction()
    rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
    assert len(rows) == 0


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_rollback_transaction(client: SqlJobClientBase) -> None:
    table_name = prepare_temp_table(client.sql_client)
    # test python exception
    with pytest.raises(RuntimeError):
        with client.sql_client.begin_transaction():
            client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
            rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
            assert len(rows) == 1
            # python exception triggers rollback
            raise RuntimeError("ROLLBACK")
    rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
    assert len(rows) == 0

    # test rollback on invalid query
    with pytest.raises(DatabaseException):
        with client.sql_client.begin_transaction():
            client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
            # table does not exist
            client.sql_client.execute_sql(f"SELECT col FROM {table_name}_X WHERE col = %s", Decimal("1.0"))
    rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
    assert len(rows) == 0

    # test explicit rollback
    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
        tx.rollback_transaction()
        rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} WHERE col = %s", Decimal("1.0"))
        assert len(rows) == 0

    # test double rollback - behavior inconsistent across databases (some raise some not)
    # with pytest.raises(DatabaseException):
    # with client.sql_client.begin_transaction() as tx:
    #     client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
    #     tx.rollback_transaction()
    #     tx.rollback_transaction()


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_transaction_isolation(client: SqlJobClientBase) -> None:
    table_name = prepare_temp_table(client.sql_client)
    event = Event()
    event.clear()

    def test_thread(thread_id: Decimal) -> None:
        # make a copy of the sql_client
        thread_client = client.sql_client.__class__(client.sql_client.dataset_name, client.sql_client.credentials)
        with thread_client:
            with thread_client.begin_transaction():
                thread_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", thread_id)
                event.wait()

    with client.sql_client.begin_transaction() as tx:
        client.sql_client.execute_sql(f"INSERT INTO {table_name} VALUES (%s)", Decimal("1.0"))
        t = Thread(target=test_thread, daemon=True, args=(Decimal("2.0"),))
        t.start()
        # thread 2.0 inserts
        sleep(3.0)
        # main thread rollback
        tx.rollback_transaction()
        # thread 2.0 commits
        event.set()
        t.join()

    # just in case close the connection
    client.sql_client.close_connection()
    # re open connection
    client.sql_client.open_connection()
    rows = client.sql_client.execute_sql(f"SELECT col FROM {table_name} ORDER BY col")
    assert len(rows) == 1
    # only thread 2 is left
    assert rows[0][0] == Decimal("2.0")


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_max_table_identifier_length(client: SqlJobClientBase) -> None:
    if client.capabilities.max_identifier_length >= 65536:
        pytest.skip(f"destination {client.config.destination_name} has no table name length restriction")
    table_name = 8 * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations"
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
    # assert exists is (client.config.destination_name == "postgres")
    # exists, table_def = client.get_storage_table(long_table_name[:client.capabilities.max_identifier_length])
    # assert exists is True


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_max_column_identifier_length(client: SqlJobClientBase) -> None:
    if client.capabilities.max_column_identifier_length >= 65536:
        pytest.skip(f"destination {client.config.destination_name} has no column name length restriction")
    table_name = "prospects_external_data__data365_member__member"
    column_name = 7 * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    with pytest.raises(IdentifierTooLongException) as py_ex:
        prepare_table(client, "long_column_name", table_name, make_uniq_table=False)
    assert py_ex.value.identifier_type == "column"
    assert py_ex.value.identifier_name == f"{table_name}.{column_name}"
    # remove the table from the schema so further tests are not affected
    client.schema.tables.pop(table_name)
    # long_column_name = 7 * "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    # assert long_column_name not in table_def
    # assert long_column_name[:client.capabilities.max_column_identifier_length] in table_def


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_recover_on_explicit_tx(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    version_table = client.sql_client.make_qualified_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(DatabaseTransientException):
        client.sql_client.execute_sql(sql)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "ProgrammingError")
    # still can execute dml and selects
    assert client.get_newest_schema_from_storage() is not None
    client.complete_load("ABC")
    assert_load_id(client.sql_client, "ABC")

    # syntax error within tx
    sql = f"BEGIN TRANSACTION;INVERT INTO {version_table} VALUES(1);COMMIT TRANSACTION;"
    with pytest.raises(DatabaseTransientException):
        client.sql_client.execute_sql(sql)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "ProgrammingError")
    assert client.get_newest_schema_from_storage() is not None
    client.complete_load("EFG")
    assert_load_id(client.sql_client, "EFG")

    # wrong value inserted
    sql = f"BEGIN TRANSACTION;INSERT INTO {version_table}(version) VALUES(1);COMMIT TRANSACTION;"
    # cannot insert NULL value
    with pytest.raises(DatabaseTerminalException):
        client.sql_client.execute_sql(sql)
    # assert derives_from_class_of_name(term_ex.value.dbapi_exception, "IntegrityError")
    # assert isinstance(term_ex.value.dbapi_exception, (psycopg2.InternalError, psycopg2.))
    assert client.get_newest_schema_from_storage() is not None
    client.complete_load("HJK")
    assert_load_id(client.sql_client, "HJK")


def assert_load_id(sql_client:SqlClientBase, load_id: str) -> None:
    # and data is actually committed when connection reopened
    sql_client.close_connection()
    sql_client.open_connection()
    rows = sql_client.execute_sql(f"SELECT load_id FROM {LOADS_TABLE_NAME} WHERE load_id = %s", load_id)
    assert len(rows) == 1


def prepare_temp_table(sql_client: SqlClientBase) -> str:
    uniq_suffix = uniq_id()
    table_name = f"tmp_{uniq_suffix}"
    sql_client.execute_sql(f"CREATE TABLE {table_name} (col NUMERIC);")
    return table_name
