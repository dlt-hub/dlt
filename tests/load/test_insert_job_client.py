from typing import Iterator, List
import pytest
from unittest.mock import patch

from dlt.common import pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.storages import FileStorage
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.insert_job_client import InsertValuesJobClient

from tests.utils import TEST_STORAGE_ROOT, skipifpypy
from tests.load.utils import (
    expect_load_file,
    prepare_table,
    yield_client_with_storage,
    destinations_configs,
)

DEFAULT_SUBSET = ["duckdb", "redshift", "postgres", "mssql", "synapse", "motherduck"]


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(scope="function")
def client(request) -> Iterator[InsertValuesJobClient]:
    yield from yield_client_with_storage(request.param.destination_factory())  # type: ignore[misc]


@pytest.mark.essential
@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET),
    indirect=True,
    ids=lambda x: x.name,
)
def test_simple_load(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)
    canonical_name = client.sql_client.make_qualified_table_name(user_table_name)
    # create insert
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\n"
    writer_type = client.capabilities.insert_values_writer_type
    if writer_type == "default":
        insert_sql += "VALUES\n"
        pre, post, sep = ("(", ")", ",\n")
    elif writer_type == "select_union":
        pre, post, sep = ("SELECT ", "", " UNION ALL\n")
    insert_values = (
        pre
        + f"'{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}'"
        + post
    )
    expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values + ";",
        user_table_name,
        file_format="insert_values",
    )
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 1
    # insert 100 more rows
    query = insert_sql + (insert_values + sep) * 99 + insert_values + ";"
    expect_load_file(client, file_storage, query, user_table_name, file_format="insert_values")
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 101
    # insert null value (single-record insert has same syntax for both writer types)
    insert_sql_nc = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, text)\nVALUES\n"
    insert_values_nc = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', NULL);"
    )
    expect_load_file(
        client,
        file_storage,
        insert_sql_nc + insert_values_nc,
        user_table_name,
        file_format="insert_values",
    )
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == 102


@skipifpypy
@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET),
    indirect=True,
    ids=lambda x: x.name,
)
def test_loading_errors(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
    # test expected dbapi exceptions for supported destinations
    dtype = client.config.destination_type
    if dtype in ("postgres", "redshift"):
        from dlt.destinations.impl.postgres.sql_client import psycopg2

        TNotNullViolation = psycopg2.errors.NotNullViolation
        TNumericValueOutOfRange = psycopg2.errors.NumericValueOutOfRange
        TUndefinedColumn = psycopg2.errors.UndefinedColumn
        TDatatypeMismatch = psycopg2.errors.DatatypeMismatch
        if dtype == "redshift":
            # redshift does not know or psycopg does not recognize those correctly
            TNotNullViolation = psycopg2.errors.InternalError_
    elif dtype in ("duckdb", "motherduck"):
        import duckdb

        TUndefinedColumn = duckdb.BinderException
        TNotNullViolation = duckdb.ConstraintException
        TNumericValueOutOfRange = TDatatypeMismatch = duckdb.ConversionException
    elif dtype in ("mssql", "synapse"):
        import pyodbc

        TUndefinedColumn = pyodbc.ProgrammingError
        TNotNullViolation = pyodbc.IntegrityError
        TNumericValueOutOfRange = TDatatypeMismatch = pyodbc.DataError

    user_table_name = prepare_table(client)
    # insert into unknown column
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_)\nVALUES\n"
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', NULL);"
    )
    job = expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        "failed",
        file_format="insert_values",
    )
    assert type(job._exception.dbapi_exception) is TUndefinedColumn  # type: ignore
    # insert null value
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
    job = expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        "failed",
        file_format="insert_values",
    )
    assert type(job._exception.dbapi_exception) is TNotNullViolation  # type: ignore
    # insert wrong type
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\nVALUES\n"
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" {client.capabilities.escape_literal(True)});"
    )
    job = expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        "failed",
        file_format="insert_values",
    )
    assert type(job._exception.dbapi_exception) is TDatatypeMismatch  # type: ignore
    # numeric overflow on bigint
    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id)\nVALUES\n"
    )
    # 2**64//2 - 1 is a maximum bigint value
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {2**64//2});"
    )
    job = expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        "failed",
        file_format="insert_values",
    )
    assert type(job._exception) == DatabaseTerminalException  # type: ignore
    # numeric overflow on NUMERIC
    insert_sql = (
        "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp,"
        " parse_data__intent__id)\nVALUES\n"
    )
    # default decimal is (38, 9) (128 bit), use local context to generate decimals with 38 precision
    with numeric_default_context():
        below_limit = Decimal(10**29) - Decimal("0.001")
        above_limit = Decimal(10**29)
    # this will pass
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {below_limit});"
    )
    expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        file_format="insert_values",
    )
    # this will raise
    insert_values = (
        f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd',"
        f" '{str(pendulum.now())}', {above_limit});"
    )
    job = expect_load_file(
        client,
        file_storage,
        insert_sql + insert_values,
        user_table_name,
        "failed",
        file_format="insert_values",
    )
    assert type(job._exception) == DatabaseTerminalException  # type: ignore

    assert (
        type(job._exception.dbapi_exception) == psycopg2.errors.InternalError_  # type: ignore
        if dtype == "redshift"
        else TNumericValueOutOfRange
    )


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=DEFAULT_SUBSET),
    indirect=True,
    ids=lambda x: x.name,
)
def test_query_split(client: InsertValuesJobClient, file_storage: FileStorage) -> None:
    writer_type = client.capabilities.insert_values_writer_type
    insert_sql = prepare_insert_statement(10, writer_type)

    if writer_type == "default":
        pre, post, sep = ("(", ")", ",\n")
    elif writer_type == "select_union":
        pre, post, sep = ("SELECT ", "", " UNION ALL\n")

    # caps are instance and are attr of sql client instance so it is safe to mock them
    client.sql_client.capabilities.max_query_length = 2
    # this guarantees that we execute inserts line by line
    with patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(
            client, file_storage, insert_sql, user_table_name, file_format="insert_values"
        )
        # print(mocked_fragments.mock_calls)
    # split in 10 lines
    assert mocked_fragments.call_count == 10
    for idx, call in enumerate(mocked_fragments.call_args_list):
        fragment: List[str] = call.args[0]
        # last elem of fragment is a data list, first element is id, and must end with ;\n
        start = pre + "'" + str(idx) + "'"
        end = post + ";"
        assert fragment[-1].startswith(start)
        assert fragment[-1].endswith(end)
    assert_load_with_max_query(client, file_storage, 10, 2)

    if writer_type == "default":
        start_idx = insert_sql.find("S\n" + pre)
    elif writer_type == "select_union":
        start_idx = insert_sql.find(pre)
    idx = insert_sql.find(post + sep, len(insert_sql) // 2)

    # set query length so it reads data until separator ("," or " UNION ALL") (followed by \n)
    query_length = (idx - start_idx - 1) * 2
    client.sql_client.capabilities.max_query_length = query_length
    with patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(
            client, file_storage, insert_sql, user_table_name, file_format="insert_values"
        )
    # split in 2 on ','
    assert mocked_fragments.call_count == 2

    # so it reads until "\n"
    query_length = (idx - start_idx) * 2
    client.sql_client.capabilities.max_query_length = query_length
    with patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(
            client, file_storage, insert_sql, user_table_name, file_format="insert_values"
        )
    # split in 2 on separator ("," or " UNION ALL")
    assert mocked_fragments.call_count == 2

    # so it reads till the last ;
    if writer_type == "default":
        offset = 3
    elif writer_type == "select_union":
        offset = 1
    query_length = (len(insert_sql) - start_idx - offset) * 2
    client.sql_client.capabilities.max_query_length = query_length
    with patch.object(client.sql_client, "execute_fragments") as mocked_fragments:
        user_table_name = prepare_table(client)
        expect_load_file(
            client, file_storage, insert_sql, user_table_name, file_format="insert_values"
        )
    # split in 2 on ','
    assert mocked_fragments.call_count == 1


def assert_load_with_max_query(
    client: InsertValuesJobClient,
    file_storage: FileStorage,
    insert_lines: int,
    max_query_length: int,
) -> None:
    # load and check for real
    client.sql_client.capabilities.max_query_length = max_query_length
    user_table_name = prepare_table(client)
    insert_sql = prepare_insert_statement(
        insert_lines, client.capabilities.insert_values_writer_type
    )
    expect_load_file(client, file_storage, insert_sql, user_table_name, file_format="insert_values")
    canonical_name = client.sql_client.make_qualified_table_name(user_table_name)
    rows_count = client.sql_client.execute_sql(f"SELECT COUNT(1) FROM {canonical_name}")[0][0]
    assert rows_count == insert_lines
    # get all uniq ids in order
    rows = client.sql_client.execute_sql(
        f"SELECT _dlt_id FROM {canonical_name} ORDER BY timestamp ASC;"
    )
    v_ids = list(map(lambda i: i[0], rows))
    assert list(map(str, range(0, insert_lines))) == v_ids
    client.sql_client.execute_sql(f"DELETE FROM {canonical_name}")


def prepare_insert_statement(lines: int, writer_type: str = "default") -> str:
    insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp)\n"
    if writer_type == "default":
        insert_sql += "VALUES\n"
        pre, post, sep = ("(", ")", ",\n")
    elif writer_type == "select_union":
        pre, post, sep = ("SELECT ", "", " UNION ALL\n")
    insert_values = pre + "'{}', '{}', '90238094809sajlkjxoiewjhduuiuehd', '{}'" + post
    for i in range(lines):
        insert_sql += insert_values.format(str(i), uniq_id(), str(pendulum.now().add(seconds=i)))
        insert_sql += sep if i < 9 else ";"
    return insert_sql
