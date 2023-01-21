import pytest
import datetime  # noqa: I251
from typing import Iterator

from dlt.common import pendulum, Decimal
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation

from dlt.destinations.sql_client import DBCursor
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage
from tests.load.utils import yield_client_with_storage, ALL_CLIENTS


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="module")
def redshift_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("redshift")


@pytest.fixture(scope="module")
def bigquery_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("bigquery")


@pytest.fixture(scope="module")
def postgres_client() -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage("postgres")


@pytest.fixture(scope="module")
def client(request) -> SqlJobClientBase:
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_sql_client_default_dataset_unqualified(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    curr: DBCursor
    # get data from unqualified name
    with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY inserted_at") as curr:
        columns = [c[0] for c in curr.description]
        data = curr.fetchall()

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
    with pytest.raises(DatabaseTransientException) as term_ex:
        client.sql_client.execute_sql(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %(date)s")
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_execute_sql(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME}")
    assert len(rows) == 1
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE schema_name = %s", "event")
    assert len(rows) == 1
    assert rows[0][0] == "event"
    assert isinstance(rows[0][1], datetime.datetime)
    # ask with datetime
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %s", rows[0][1])
    assert len(rows) == 1
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=rows[0][1])
    assert len(rows) == 1
    assert rows[0][0] == "event"
    rows = client.sql_client.execute_sql(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=pendulum.now())
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
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=rows[0][1]) as curr:
        rows = curr.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "event"
    with client.sql_client.execute_query(f"SELECT schema_name, inserted_at FROM {VERSION_TABLE_NAME} WHERE inserted_at = %(date)s", date=pendulum.now()) as curr:
        rows = curr.fetchall()
        assert len(rows) == 0


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
    with pytest.raises(DatabaseTransientException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s", b'bytes'):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid schema/dataset
    with client.sql_client.with_alternative_dataset_name("UNKNOWN"):
        qualified_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        with pytest.raises(DatabaseUndefinedRelation) as term_ex:
            with client.sql_client.execute_query(f"SELECT * FROM {qualified_name} ORDER BY inserted_at"):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
