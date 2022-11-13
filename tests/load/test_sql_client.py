from copy import deepcopy
import io
import pytest
import datetime  # noqa: I251
from typing import Iterator

from dlt.common import json, pendulum
from dlt.common.schema import Schema
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.schema.utils import new_table
from dlt.common.storages import FileStorage
from dlt.common.schema import TTableSchemaColumns
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseUndefinedRelation

from dlt.destinations.sql_client import DBCursor
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage
from tests.common.utils import load_json_case
from tests.load.utils import (TABLE_UPDATE, TABLE_UPDATE_COLUMNS_SCHEMA, TABLE_ROW, expect_load_file, yield_client_with_storage,
                                cm_yield_client_with_storage, write_dataset, prepare_table, ALL_CLIENTS)


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
def test_terminal_exceptions(client: SqlJobClientBase) -> None:
    client.update_storage_schema()
    # invalid table
    with pytest.raises(DatabaseUndefinedRelation) as term_ex:
        with client.sql_client.execute_query("SELECT * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid syntax
    with pytest.raises(DatabaseTerminalException) as term_ex:
        with client.sql_client.execute_query("SELECTA * FROM TABLE_XXX ORDER BY inserted_at"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid column
    with pytest.raises(DatabaseTerminalException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} ORDER BY column_XXX"):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid parameters to dbapi
    with pytest.raises(DatabaseTerminalException) as term_ex:
        with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s", b'bytes'):
            pass
    assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # with pytest.raises(LoadClientTerminalException) as term_ex:
    #     with client.sql_client.execute_query(f"SELECT * FROM {LOADS_TABLE_NAME} WHERE inserted_at = %s"):
    #         pass
    # assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
    # invalid schema/dataset
    with client.sql_client.with_alternative_dataset_name("UNKNOWN"):
        qualified_name = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        with pytest.raises(DatabaseUndefinedRelation) as term_ex:
            with client.sql_client.execute_query(f"SELECT * FROM {qualified_name} ORDER BY inserted_at"):
                pass
        assert client.sql_client.is_dbapi_exception(term_ex.value.dbapi_exception)
