from typing import Iterator, List
import pytest
from unittest.mock import patch

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.insert_job_client import InsertValuesJobClient

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, skipifpypy
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration

DEFAULT_SUBSET = ["duckdb", "redshift", "postgres"]

@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)

@pytest.fixture(scope="function")
def client(request) -> Iterator[InsertValuesJobClient]:
    yield from yield_client_with_storage(request.param.destination)  # type: ignore[misc]

@pytest.mark.parametrize("client", destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET), indirect=True, ids=lambda x: x.name)
def test_simple_load(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
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


@skipifpypy
@pytest.mark.parametrize("client", destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET), indirect=True, ids=lambda x: x.name)
def test_loading_errors(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
    # test expected dbiapi exceptions for supported destinations
    import duckdb
    from dlt.destinations.impl.postgres.sql_client import psycopg2

    TNotNullViolation = psycopg2.errors.NotNullViolation
    TNumericValueOutOfRange = psycopg2.errors.NumericValueOutOfRange
    TUndefinedColumn = psycopg2.errors.UndefinedColumn
    TDatatypeMismatch = psycopg2.errors.DatatypeMismatch
    if client.config.destination_type == "redshift":
        # redshift does not know or psycopg does not recognize those correctly
        TNotNullViolation = psycopg2.errors.InternalError_
    if client.config.destination_type == "duckdb":
        TUndefinedColumn = duckdb.BinderException
        TNotNullViolation = duckdb.ConstraintException
        TNumericValueOutOfRange = TDatatypeMismatch = duckdb.ConversionException


    user_table_name = prepare_table(client)
    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) is TUndefinedColumn
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
    assert type(exv.value.dbapi_exception) is TDatatypeMismatch
    # numeric overflow on bigint
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id)\nVALUES\n"
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
    with pytest.raises(DatabaseTerminalException) as exv:
        expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
    assert type(exv.value.dbapi_exception) in (TNumericValueOutOfRange, )
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
    assert type(exv.value.dbapi_exception) in (TNumericValueOutOfRange, psycopg2.errors.InternalError_)



@pytest.mark.parametrize("client", destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET), indirect=True, ids=lambda x: x.name)
def test_query_split(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
    mocked_caps = client.sql_client.__class__.capabilities
    insert_sql = prepare_insert_statement(10)

    # this guarantees that we execute inserts line by line
    with patch.object(mocked_caps, "max_query_length", 2), patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(client, file_storage, insert_sql, user_table_name)
        # print(mocked_fragments.mock_calls)
    # split in 10 lines
    assert mocked_fragments.call_count == 10
    for idx, call in enumerate(mocked_fragments.call_args_list):
        fragment:List[str] = call.args[0]
        # last elem of fragment is a data list, first element is id, and must end with ;\n
        assert fragment[-1].startswith(f"'{idx}'")
        assert fragment[-1].endswith(");")
    assert_load_with_max_query(client, file_storage, 10, 2)

    start_idx = insert_sql.find("S\n(")
    idx = insert_sql.find("),\n", len(insert_sql)//2)

    # set query length so it reads data until "," (followed by \n)
    query_length = (idx - start_idx - 1) * 2
    with patch.object(mocked_caps, "max_query_length", query_length), patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(client, file_storage, insert_sql, user_table_name)
    # split in 2 on ','
    assert mocked_fragments.call_count == 2

    # so it reads until "\n"
    query_length = (idx - start_idx) * 2
    with patch.object(mocked_caps, "max_query_length", query_length), patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(client, file_storage, insert_sql, user_table_name)
    # split in 2 on ','
    assert mocked_fragments.call_count == 2

    # so it reads till the last ;
    query_length = (len(insert_sql) - start_idx - 3) * 2
    with patch.object(mocked_caps, "max_query_length", query_length), patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(client, file_storage, insert_sql, user_table_name)
    # split in 2 on ','
    assert mocked_fragments.call_count == 1


def assert_load_with_max_query(client: InsertValuesJobClient, file_storage: FileStorage, insert_lines: int, max_query_length: int) -> None:
    # load and check for real
    mocked_caps = client.sql_client.__class__.capabilities
    with patch.object(mocked_caps, "max_query_length", max_query_length):
        user_table_name = prepare_table(client)
        insert_sql = prepare_insert_statement(insert_lines)
        expect_load_file(client, file_storage, insert_sql, user_table_name)
        rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {user_table_name}")[0][0]
        assert rows_count == insert_lines
        # get all uniq ids in order
        with client.sql_client.execute_query(f"SELECT _dlt_id FROM {user_table_name} ORDER BY timestamp ASC;") as c:
            rows = list(c.fetchall())
            v_ids = list(map(lambda i: i[0], rows))
        assert list(map(str, range(0, insert_lines))) == v_ids
    client.sql_client.execute_sql(f"DELETE FROM {user_table_name}")


def prepare_insert_statement(lines: int) -> str:
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = "('{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}')"
    #ids = []
    for i in range(lines):
        # id_ = uniq_id()
        # ids.append(id_)
        insert_sql += insert_values.format(str(i), uniq_id(), str(pendulum.now().add(seconds=i)))
        if i < 9:
            insert_sql += ",\n"
        else:
            insert_sql += ";"
    # print(insert_sql)
    return insert_sql
