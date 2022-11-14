from typing import Iterator
import pytest
from unittest.mock import patch

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.postgres.postgres import PostgresClientBase, PostgresClient, psycopg2

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage

ALL_CLIENTS = ["redshift_client", "postgres_client"]


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="function")
def redshift_client() -> Iterator[PostgresClientBase]:
    yield from yield_client_with_storage("redshift")


@pytest.fixture(scope="function")
def postgres_client() -> Iterator[PostgresClientBase]:
    yield from yield_client_with_storage("postgres")


@pytest.fixture(scope="function")
def client(request) -> PostgresClientBase:
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_recover_tx_rollback(client: PostgresClientBase) -> None:
    client.update_storage_schema()
    version_table = client.sql_client.make_qualified_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(DatabaseTransientException) as term_ex:
        client.sql_client.execute_sql(sql)
    assert isinstance(term_ex.value.dbapi_exception, psycopg2.ProgrammingError)
    # still can execute tx and selects
    client.get_newest_schema_from_storage()
    client.complete_load("ABC")

    # syntax error within tx
    sql = f"BEGIN TRANSACTION;INVERT INTO {version_table} VALUES(1);COMMIT TRANSACTION;"
    with pytest.raises(DatabaseTransientException) as term_ex:
        client.sql_client.execute_sql(sql)
    assert isinstance(term_ex.value.dbapi_exception, psycopg2.ProgrammingError)
    client.get_newest_schema_from_storage()
    client.complete_load("EFG")

    # wrong value inserted
    sql = f"BEGIN TRANSACTION;INSERT INTO {version_table}(version) VALUES(1);COMMIT TRANSACTION;"
    # cannot insert NULL value
    with pytest.raises(DatabaseTerminalException) as term_ex:
        client.sql_client.execute_sql(sql)
    assert isinstance(term_ex.value.dbapi_exception, (psycopg2.InternalError, psycopg2.IntegrityError))
    client.get_newest_schema_from_storage()
    client.complete_load("HJK")


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_simple_load(client: PostgresClientBase, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)
    canonical_name = client.sql_client.make_qualified_table_name(user_table_name)
    # create insert
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}')"
    expect_load_file(client, file_storage, insert_sql+insert_values+";", user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 1
    # insert 100 more rows
    query = insert_sql + (insert_values + ",\n") * 99 + insert_values + ";"
    expect_load_file(client, file_storage, query, user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 101
    # insert null value
    insert_sql_nc = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, text)\nVALUES\n"
    insert_values_nc = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    expect_load_file(client, file_storage, insert_sql_nc+insert_values_nc, user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 102


@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_long_names(client: PostgresClientBase) -> None:
    long_table_name = prepare_table(client, "long_names", "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations")
    # remove the table from the schema so further tests are not affected.
    # TODO: remove line when we handle too long names correctly
    client.schema._schema_tables.pop(long_table_name)
    exists, _ = client.get_storage_table(long_table_name)
    # interestingly postgres is also trimming the names in queries so the table "exists"
    assert exists is (client.config.destination_name == "postgres")
    exists, table_def = client.get_storage_table(long_table_name[:client.capabilities().max_identifier_length])
    assert exists is True
    long_column_name = "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    assert long_column_name not in table_def
    assert long_column_name[:client.capabilities().max_column_identifier_length] in table_def


@skipifpypy
@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_loading_errors(client: PostgresClientBase, file_storage: FileStorage) -> None:

    TNotNullViolation = psycopg2.errors.NotNullViolation
    TNumericValueOutOfRange = psycopg2.errors.NumericValueOutOfRange
    if client.config.destination_name == "redshift":
        # redshift does not know or psycopg does not recognize those correctly
        TNotNullViolation = TNumericValueOutOfRange = psycopg2.errors.InternalError_

    user_table_name = prepare_table(client)
    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is psycopg2.errors.UndefinedColumn
    # insert null value
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is TNotNullViolation
    # insert wrong type
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', TRUE);"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is psycopg2.errors.DatatypeMismatch
    # numeric overflow on bigint
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id)\nVALUES\n"
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is psycopg2.errors.NumericValueOutOfRange
    # numeric overflow on NUMERIC
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__intent__id)\nVALUES\n"
    # default decimal is (38, 9) (128 bit), use local context to generate decimals with 38 precision
    with numeric_default_context():
        below_limit = Decimal(10**29) - Decimal('0.001')
        above_limit = Decimal(10**29)
    # this will pass
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {below_limit});"
    expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    # this will raise
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {above_limit});"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is TNumericValueOutOfRange



@pytest.mark.parametrize('client', ALL_CLIENTS, indirect=True)
def test_query_split(client: PostgresClientBase, file_storage: FileStorage) -> None:
    mocked_caps = PostgresClient.capabilities()
    # this guarantees that we execute inserts line by line
    mocked_caps["max_query_length"] = 2

    with patch.object(PostgresClient, "capabilities") as caps:
        caps.return_value = mocked_caps
        user_table_name = prepare_table(client)
        insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
        insert_values = "('{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}')"
        ids = []
        for i in range(10):
            id_ = uniq_id()
            ids.append(id_)
            insert_sql += insert_values.format(id_, uniq_id(), str(pendulum.now().add(seconds=i)))
            if i < 10:
                insert_sql += ",\n"
            else:
                insert_sql + ";"
        expect_load_file(client, file_storage, insert_sql, user_table_name)
        rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {user_table_name}")[0][0]
        assert rows_count == 10
        # get all uniq ids in order
        with client.sql_client.execute_query(f"SELECT _dlt_id FROM {user_table_name} ORDER BY timestamp ASC;") as c:
            v_ids = list(map(lambda i: i[0], c.fetchall()))
        assert ids == v_ids
