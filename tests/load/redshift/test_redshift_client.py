from typing import Iterator
import pytest
from unittest.mock import patch

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.storages import FileStorage
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id

from dlt.load.exceptions import LoadClientTerminalInnerException
from dlt.load import Load
from dlt.load.redshift.client import RedshiftClient, RedshiftInsertLoadJob, psycopg2

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="module")
def client() -> Iterator[RedshiftClient]:
    yield from yield_client_with_storage("redshift")


def test_recover_tx_rollback(client: RedshiftClient) -> None:
    client.update_storage_schema()
    version_table = client.sql_client.make_qualified_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(psycopg2.ProgrammingError):
        client.sql_client.execute_sql(sql)
    # still can execute tx and selects
    client._get_schema_version_from_storage()
    client._update_schema_version(3)
    # syntax error within tx
    sql = f"BEGIN TRANSACTION;INVERT INTO {version_table} VALUES(1);COMMIT TRANSACTION;"
    with pytest.raises(psycopg2.ProgrammingError):
        client.sql_client.execute_sql(sql)
    client._get_schema_version_from_storage()
    client._update_schema_version(4)
    # wrong value inserted
    sql = f"BEGIN TRANSACTION;INSERT INTO {version_table}(version) VALUES(1);COMMIT TRANSACTION;"
    # cannot insert NULL value
    with pytest.raises(psycopg2.InternalError):
        client.sql_client.execute_sql(sql)
    client._get_schema_version_from_storage()
    # lower the schema version in storage so subsequent tests can run
    client._update_schema_version(1)


def test_simple_load(client: RedshiftClient, file_storage: FileStorage) -> None:
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


def test_long_names(client: RedshiftClient) -> None:
    long_table_name = prepare_table(client, "long_names", "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations")
    # remove the table from the schema so further tests are not affected.
    # TODO: remove line when we handle too long names correctly
    client.schema._schema_tables.pop(long_table_name)
    exists, _ = client._get_storage_table(long_table_name)
    assert exists is False
    exists, table_def = client._get_storage_table(long_table_name[:client.capabilities()["max_identifier_length"]])
    assert exists is True
    long_column_name = "prospects_external_data__data365_member__member__feed_activities_created_post__items__comments__items__comments__items__author_details__educations__school_name"
    assert long_column_name not in table_def
    assert long_column_name[:client.capabilities()["max_column_length"]] in table_def


@skipifpypy
def test_loading_errors(client: RedshiftClient, file_storage: FileStorage) -> None:
    caps = client.capabilities()

    user_table_name = prepare_table(client)
    # insert string longer than redshift maximum
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    # try some unicode value - redshift checks the max length based on utf-8 representation, not the number of characters
    # max_len_str = 'उ' * (65535 // 3) + 1 -> does not fit
    # max_len_str = 'a' * 65535 + 1 -> does not fit
    max_len_str = 'उ' * ((caps["max_text_data_type_length"] // 3) + 1)
    # max_len_str_b = max_len_str.encode("utf-8")
    # print(len(max_len_str_b))
    row_id = uniq_id()
    insert_values = f"('{row_id}', '{uniq_id()}', '{max_len_str}' , '{str(pendulum.now())}');"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.StringDataRightTruncation

    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.UndefinedColumn
    # insert null value
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
    # insert wrong type
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', TRUE);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.DatatypeMismatch
    # numeric overflow on bigint
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id)\nVALUES\n"
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.NumericValueOutOfRange
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
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
    # max redshift decimal is (38, 0) (128 bit) = 10**38 - 1
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**38});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_


def test_query_split(client: RedshiftClient, file_storage: FileStorage) -> None:
    mocked_caps = RedshiftClient.capabilities()
    # this guarantees that we execute inserts line by line
    mocked_caps["max_query_length"] = 2

    with patch.object(RedshiftClient, "capabilities") as caps:
        caps.return_value = mocked_caps
        print(RedshiftClient.capabilities())
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


@pytest.mark.skip
@skipifpypy
def test_maximum_query_size(client: RedshiftClient, file_storage: FileStorage) -> None:
    mocked_caps = RedshiftClient.capabilities()
    # this guarantees that we cross the redshift query limit
    mocked_caps["max_query_length"] = 2 * 20 * 1024 * 1024

    with patch.object(RedshiftClient, "capabilities") as caps:
        caps.return_value = mocked_caps

        insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
        insert_values = "('{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}'){}"
        insert_sql = insert_sql + insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ",\n") * 150000
        insert_sql += insert_values.format(uniq_id(), uniq_id(), str(pendulum.now()), ";")

        user_table_name = prepare_table(client)
        with pytest.raises(LoadClientTerminalInnerException) as exv:
            expect_load_file(client, file_storage, insert_sql, user_table_name)
        # psycopg2.errors.SyntaxError: Statement is too large. Statement Size: 20971754 bytes. Maximum Allowed: 16777216 bytes
        assert type(exv.value.inner_exc) is psycopg2.errors.SyntaxError