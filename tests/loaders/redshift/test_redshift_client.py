import io
import psycopg2
import pytest

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.configuration import PostgresConfiguration
from dlt.common.file_storage import FileStorage
from dlt.common.schema import Schema, TTableColumns
from dlt.common.schema.utils import new_table
from dlt.common.utils import uniq_id
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.dataset_writers import write_insert_values

from dlt.loaders.configuration import configuration
from dlt.loaders.exceptions import LoadClientTerminalInnerException
from dlt.loaders.redshift.client import RedshiftClient

from tests.utils import TEST_STORAGE, delete_storage
from tests.common.utils import load_json_case
from tests.loaders.utils import TABLE_UPDATE, TABLE_ROW, expect_load_file, prepare_event_user_table


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


@pytest.fixture(scope="module")
def client() -> RedshiftClient:
    # create dataset with random name
    CLIENT_CONFIG: PostgresConfiguration = configuration({"CLIENT_TYPE": "redshift"})
    CLIENT_CONFIG.PG_SCHEMA_PREFIX = "test_" + uniq_id()
    # get event default schema
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    # create client and dataset
    with RedshiftClient(schema, CLIENT_CONFIG) as client:
        client.initialize_storage()
        yield client
        # delete dataset
        schema_name = client._to_canonical_schema_name()
        client._execute_sql(f"DROP SCHEMA {schema_name} CASCADE")


@pytest.mark.order(1)
def test_initialize_storage(client: RedshiftClient) -> None:
    pass


@pytest.mark.order(2)
def test_get_version_on_empty(client: RedshiftClient) -> None:
    version = client._get_schema_version_from_storage()
    assert version == 0


@pytest.mark.order(3)
def test_get_update_basic_schema(client: RedshiftClient) -> None:
    client.update_storage_schema()
    version = client._get_schema_version_from_storage()
    assert version == 1
    # modify version
    client._update_schema_version(2)
    version = client._get_schema_version_from_storage()
    assert version == 2
    client._update_schema_version(1)
    version = client._get_schema_version_from_storage()
    assert version == 1
    # check if tables are present and identical
    exists, version_storage_table = client._get_storage_table(Schema.VERSION_TABLE_NAME)
    version_schema_table = client._get_table_by_name(Schema.VERSION_TABLE_NAME, "")
    assert exists is True
    # schema version must be contained in storage version (schema may not have all hints)
    assert version_schema_table.keys() == version_storage_table.keys()
    assert all(set(v) <= set(version_storage_table[k]) for k, v in version_schema_table.items())


@pytest.mark.order(4)
def test_complete_load(client: RedshiftClient) -> None:
    client.update_storage_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    load_table = client._to_canonical_table_name(Schema.LOADS_TABLE_NAME)
    load_rows = list(client._execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 1
    assert load_rows[0][0] == load_id
    assert load_rows[0][1] == 0
    import datetime  # noqa: I251
    assert type(load_rows[0][2]) is datetime.datetime
    client.complete_load("load2")
    load_rows = list(client._execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 2


@pytest.mark.order(5)
def test_schema_update_create_table(client: RedshiftClient) -> None:
    # infer typical rasa event schema
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    # this will be sort
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    assert timestamp["sort"] is True
    # this will be destkey
    sender_id = schema._infer_column("sender_id", "982398490809324")
    assert  sender_id["cluster"] is True
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    assert record_hash["unique"] is True
    schema.update_schema(new_table(table_name, columns=[timestamp, sender_id, record_hash]))
    client.update_storage_schema()
    exists, _ = client._get_storage_table(table_name)
    assert exists is True


@pytest.mark.order(6)
def test_schema_update_alter_table(client: RedshiftClient) -> None:
    schema = client.schema
    col1 = schema._infer_column("col1", "string")
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=[col1]))
    client.update_storage_schema()
    # with single alter table
    col2 = schema._infer_column("col2", 1)
    schema.update_schema(new_table(table_name, columns=[col2]))
    client.update_storage_schema()
    # with 2 alter tables
    col3 = schema._infer_column("col3", 1.2)
    col4 = schema._infer_column("col4", 182879721.182912)
    col4["data_type"] = "timestamp"
    schema.update_schema(new_table(table_name, columns=[col3, col4]))
    client.update_storage_schema()
    _, storage_table = client._get_storage_table(table_name)
    # 4 columns
    assert len(storage_table) == 4
    assert storage_table["col4"]["data_type"] == "timestamp"


@pytest.mark.order(7)
def test_recover_tx_rollback(client: RedshiftClient) -> None:
    client.update_storage_schema()
    version_table = client._to_canonical_table_name("_dlt_version")
    # simple syntax error
    sql = f"SELEXT * FROM {version_table}"
    with pytest.raises(psycopg2.errors.SyntaxError):
        client._execute_sql(sql)
    # still can execute tx and selects
    client._get_schema_version_from_storage()
    client._update_schema_version(3)
    # syntax error within tx
    sql = f"BEGIN TRANSACTION;INVERT INTO {version_table} VALUES(1);COMMIT TRANSACTION;"
    with pytest.raises(psycopg2.errors.SyntaxError):
        client._execute_sql(sql)
    client._get_schema_version_from_storage()
    client._update_schema_version(4)
    # wrong value inserted
    sql = f"BEGIN TRANSACTION;INSERT INTO {version_table}(version) VALUES(1);COMMIT TRANSACTION;"
    # cannot insert NULL value
    with pytest.raises(psycopg2.errors.InternalError_):
        client._execute_sql(sql)
    client._get_schema_version_from_storage()
    client._update_schema_version(4)


@pytest.mark.order(8)
def test_simple_load(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_event_user_table(client)
    canonical_name = client._to_canonical_table_name(user_table_name)
    # create insert
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}')"
    expect_load_file(client, file_storage, insert_sql+insert_values+";", user_table_name)
    rows_count = client._execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 1
    # insert 100 more rows
    query = insert_sql + (insert_values + ",\n") * 99 + insert_values + ";"
    expect_load_file(client, file_storage, query, user_table_name)
    rows_count = client._execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 101
    # insert null value
    insert_sql_nc = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, text) VALUES\n"
    insert_values_nc = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    expect_load_file(client, file_storage, insert_sql_nc+insert_values_nc, user_table_name)
    rows_count = client._execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 102


@pytest.mark.order(9)
def test_loading_errors(client: RedshiftClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_event_user_table(client)
    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.UndefinedColumn
    # insert null value
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
    # insert wrong type
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', TRUE);"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.DatatypeMismatch
    # numeric overflow on bigint
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id) VALUES\n"
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.NumericValueOutOfRange
    # numeric overflow on NUMERIC
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__intent__id) VALUES\n"
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
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id) VALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**38});"
    with pytest.raises(LoadClientTerminalInnerException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_


@pytest.mark.order(10)
def test_data_writer_load(client: RedshiftClient, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client._to_canonical_table_name(table_name)
    # write only first row
    with io.StringIO() as f:
        write_insert_values(f, [rows[0]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client._execute_sql(f"SELECT * FROM {canonical_name}")[0]
    # content must equal
    assert list(db_row) == list(rows[0].values())
    # write second row that contains two nulls
    with io.StringIO() as f:
        write_insert_values(f, [rows[1]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client._execute_sql(f"SELECT * FROM {canonical_name} WHERE f_int = {rows[1]['f_int']}")[0]
    assert db_row[3] is None
    assert db_row[5] is None


@pytest.mark.order(11)
def test_data_writer_string_escape(client: RedshiftClient, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client._to_canonical_table_name(table_name)
    row = rows[0]
    # this will really drop table without escape
    inj_str = f", NULL'); DROP TABLE {canonical_name} --"
    row["f_str"] = inj_str
    with io.StringIO() as f:
        write_insert_values(f, [rows[0]], rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client._execute_sql(f"SELECT * FROM {canonical_name}")[0]
    assert list(db_row) == list(row.values())


pytest.mark.order(12)
def test_data_writer_string_escape(client: RedshiftClient, file_storage: FileStorage) -> None:
    rows, table_name = prepare_schema(client, "weird_rows")
    canonical_name = client._to_canonical_table_name(table_name)
    with io.StringIO() as f:
        write_insert_values(f, rows, rows[0].keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    for i in range(1,4):
        db_row = client._execute_sql(f"SELECT str FROM {canonical_name} WHERE idx = {i}")
        assert db_row[0][0] == rows[i-1]["str"]


@pytest.mark.order(13)
def test_get_storage_table_with_all_types(client: RedshiftClient) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=TABLE_UPDATE))
    client.update_storage_schema()
    exists, storage_table = client._get_storage_table(table_name)
    assert exists is True
    # column order must match TABLE_UPDATE
    storage_columns = list(storage_table.values())
    for c, s_c in zip(TABLE_UPDATE, storage_columns):
        assert c["name"] == s_c["name"]
        if c["data_type"] == "complex":
            assert s_c["data_type"] == "text"
        else:
            assert c["data_type"] == s_c["data_type"]


@pytest.mark.order(14)
def test_load_with_all_types(client: RedshiftClient, file_storage: FileStorage) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=TABLE_UPDATE))
    client.update_storage_schema()
    canonical_name = client._to_canonical_table_name(table_name)
    # write row
    with io.StringIO() as f:
        write_insert_values(f, [TABLE_ROW], TABLE_ROW.keys())
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = list(client._execute_sql(f"SELECT * FROM {canonical_name}")[0])
    # content must equal
    db_row[3] = str(pendulum.instance(db_row[3]))  # serialize date
    db_row[6] = bytes.fromhex(db_row[6])  # redshift returns binary as hex string

    assert db_row == list(TABLE_ROW.values())


def prepare_schema(client: RedshiftClient, case: str) -> None:
    client.update_storage_schema()
    rows = load_json_case(case)
    # use first row to infer table
    table: TTableColumns = {k: client.schema._infer_column(k, v) for k, v in rows[0].items()}
    table_name = f"event_{case}_{uniq_id()}"
    client.schema.update_schema(new_table(table_name, columns=table.values()))
    client.update_storage_schema()
    return rows, table_name
