import contextlib
from copy import deepcopy
import io, os
from time import sleep
from unittest.mock import patch
import pytest
import datetime  # noqa: I251
from typing import Iterator, Tuple, List, Dict, Any

from dlt.common import json, pendulum
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    VERSION_TABLE_NAME,
    TWriteDisposition,
    TTableSchema,
)
from dlt.common.schema.utils import new_table, new_column, pipeline_state_table
from dlt.common.storages import FileStorage
from dlt.common.schema import TTableSchemaColumns
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import (
    DatabaseException,
    DatabaseTerminalException,
    DatabaseUndefinedRelation,
)

from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.common.destination.reference import (
    StateInfo,
    WithStagingDataset,
    DestinationClientConfiguration,
    WithStateSync,
)
from dlt.common.time import ensure_pendulum_datetime

from tests.cases import table_update_and_row, assert_all_data_types_row
from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage
from tests.common.utils import load_json_case
from tests.load.utils import (
    TABLE_UPDATE,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    expect_load_file,
    load_table,
    yield_client_with_storage,
    cm_yield_client_with_storage,
    write_dataset,
    prepare_table,
    normalize_storage_table_cols,
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
    yield from yield_client_with_storage(request.param.destination_factory())


@pytest.fixture(scope="function")
def naming(request) -> str:
    # NOTE: this fixture is forced by `client` fixture which requires it goes first
    # so sometimes there's no request available
    if hasattr(request, "param"):
        os.environ["SCHEMA__NAMING"] = request.param
        return request.param
    return None


@pytest.mark.order(1)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_initialize_storage(client: SqlJobClientBase) -> None:
    pass


@pytest.mark.order(2)
@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_get_schema_on_empty_storage(naming: str, client: SqlJobClientBase) -> None:
    # test getting schema on empty dataset without any tables
    version_table_name = client.schema.version_table_name
    table_name, table_columns = list(client.get_storage_tables([version_table_name]))[0]
    assert table_name == version_table_name
    assert len(table_columns) == 0
    schema_info = client.get_stored_schema(client.schema.name)
    assert schema_info is None
    schema_info = client.get_stored_schema_by_hash("8a0298298823928939")
    assert schema_info is None

    # now try to get several non existing tables
    storage_tables = list(client.get_storage_tables(["no_table_1", "no_table_2"]))
    assert [("no_table_1", {}), ("no_table_2", {})] == storage_tables


@pytest.mark.order(3)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_get_update_basic_schema(client: SqlJobClientBase) -> None:
    schema = client.schema
    schema_update = client.update_stored_schema()
    # expect dlt tables in schema update
    assert set(schema_update.keys()) == {VERSION_TABLE_NAME, LOADS_TABLE_NAME, "event_slot"}
    # event_bot and event_user are not present because they have no columns
    # check is event slot has variant
    assert schema_update["event_slot"]["columns"]["value"]["variant"] is True
    # now we have dlt tables
    storage_tables = list(client.get_storage_tables([VERSION_TABLE_NAME, LOADS_TABLE_NAME]))
    assert set([table[0] for table in storage_tables]) == {VERSION_TABLE_NAME, LOADS_TABLE_NAME}
    assert [len(table[1]) > 0 for table in storage_tables] == [True, True]
    # verify if schemas stored
    this_schema = client.get_stored_schema_by_hash(schema.version_hash)
    newest_schema = client.get_stored_schema(client.schema.name)
    # should point to the same schema
    assert this_schema == newest_schema
    # check fields
    # NOTE: schema version == 2 because we updated default hints after loading the schema
    assert this_schema.version == 2 == schema.version
    assert this_schema.version_hash == schema.stored_version_hash
    assert this_schema.engine_version == schema.ENGINE_VERSION
    assert this_schema.schema_name == schema.name
    assert isinstance(this_schema.inserted_at, datetime.datetime)
    # also the content must be the same
    assert this_schema.schema == json.dumps(schema.to_dict())
    first_version_schema = this_schema.schema

    # modify schema
    schema.tables["event_slot"]["write_disposition"] = "replace"
    schema._bump_version()
    assert schema.version > this_schema.version

    # update in storage
    client._update_schema_in_storage(schema)
    sleep(1)
    this_schema = client.get_stored_schema_by_hash(schema.version_hash)
    newest_schema = client.get_stored_schema(client.schema.name)
    assert this_schema == newest_schema
    assert this_schema.version == schema.version == 3
    assert this_schema.version_hash == schema.stored_version_hash

    # simulate parallel write: initial schema is modified differently and written alongside the first one
    # in that case the version will not change or go down
    first_schema = Schema.from_dict(json.loads(first_version_schema))
    first_schema.tables["event_bot"]["write_disposition"] = "replace"
    first_schema._bump_version()
    assert first_schema.version == this_schema.version == 3
    # wait to make load_newest_schema deterministic
    sleep(1)
    client._update_schema_in_storage(first_schema)
    this_schema = client.get_stored_schema_by_hash(first_schema.version_hash)
    newest_schema = client.get_stored_schema(client.schema.name)
    assert this_schema == newest_schema  # error
    assert this_schema.version == first_schema.version == 3
    assert this_schema.version_hash == first_schema.stored_version_hash

    # get schema with non existing hash
    assert client.get_stored_schema_by_hash("XAXXA") is None

    # mock other schema in client and get the newest schema. it should not exist...
    client.schema = Schema("ethereum")
    assert client.get_stored_schema(client.schema.name) is None
    client.schema._bump_version()
    schema_update = client.update_stored_schema()
    # no schema updates because schema has no tables
    assert schema_update == {}
    that_info = client.get_stored_schema(client.schema.name)
    assert that_info.schema_name == "ethereum"

    # get event schema again
    client.schema = Schema("event")
    this_schema = client.get_stored_schema(client.schema.name)
    assert this_schema == newest_schema


@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_complete_load(naming: str, client: SqlJobClientBase) -> None:
    loads_table_name = client.schema.loads_table_name
    version_table_name = client.schema.version_table_name
    client.update_stored_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    load_table = client.sql_client.make_qualified_table_name(loads_table_name)
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 1
    assert load_rows[0][0] == load_id
    assert load_rows[0][1] == client.schema.name
    assert load_rows[0][2] == 0
    import datetime  # noqa: I251

    assert isinstance(ensure_pendulum_datetime(load_rows[0][3]), datetime.datetime)
    assert load_rows[0][4] == client.schema.version_hash
    # make sure that hash in loads exists in schema versions table
    versions_table = client.sql_client.make_qualified_table_name(version_table_name)
    version_hash_column = client.sql_client.escape_column_name(
        client.schema.naming.normalize_identifier("version_hash")
    )
    version_rows = list(
        client.sql_client.execute_sql(
            f"SELECT * FROM {versions_table} WHERE {version_hash_column} = %s", load_rows[0][4]
        )
    )
    assert len(version_rows) == 1
    client.complete_load("load2")
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 2


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True),
    indirect=True,
    ids=lambda x: x.name,
)
def test_schema_update_create_table(client: SqlJobClientBase) -> None:
    # infer typical rasa event schema
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    # this will be sort
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    assert timestamp["sort"] is True
    # this will be destkey
    sender_id = schema._infer_column("sender_id", "982398490809324")
    assert sender_id["cluster"] is True
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    assert record_hash["unique"] is True
    schema.update_table(new_table(table_name, columns=[timestamp, sender_id, record_hash]))
    schema._bump_version()
    schema_update = client.update_stored_schema()
    # check hints in schema update
    table_update = schema_update[table_name]["columns"]
    assert table_update["timestamp"]["sort"] is True
    assert table_update["sender_id"]["cluster"] is True
    assert table_update["_dlt_id"]["unique"] is True
    _, storage_columns = list(client.get_storage_tables([table_name]))[0]
    assert len(storage_columns) > 0


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    indirect=True,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("dataset_name", (None, "_hidden_ds"))
def test_schema_update_create_table_bigquery(client: SqlJobClientBase, dataset_name: str) -> None:
    # patch dataset name
    if dataset_name:
        # drop existing dataset
        client.drop_storage()
        client.sql_client.dataset_name = dataset_name + "_" + uniq_id()
        client.initialize_storage()

    # infer typical rasa event schema
    schema = client.schema
    # this will be partition
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    # this will be cluster
    sender_id = schema._infer_column("sender_id", "982398490809324")
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    schema.update_table(new_table("event_test_table", columns=[timestamp, sender_id, record_hash]))
    schema._bump_version()
    schema_update = client.update_stored_schema()
    # check hints in schema update
    table_update = schema_update["event_test_table"]["columns"]
    assert table_update["timestamp"]["partition"] is True
    assert table_update["_dlt_id"]["nullable"] is False
    _, storage_columns = client.get_storage_table("event_test_table")
    # check if all columns present
    assert storage_columns.keys() == client.schema.tables["event_test_table"]["columns"].keys()
    _, storage_columns = client.get_storage_table("_dlt_version")
    assert storage_columns.keys() == client.schema.tables["_dlt_version"]["columns"].keys()


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_schema_update_alter_table(client: SqlJobClientBase) -> None:
    # force to update schema in chunks by setting the max query size to 10 bytes/chars
    with patch.object(client.capabilities, "max_query_length", new=10):
        schema = client.schema
        col1 = schema._infer_column("col1", "string")
        table_name = "event_test_table" + uniq_id()
        schema.update_table(new_table(table_name, columns=[col1]))
        schema._bump_version()
        schema_update = client.update_stored_schema()
        assert table_name in schema_update
        assert len(schema_update[table_name]["columns"]) == 1
        assert schema_update[table_name]["columns"]["col1"]["data_type"] == "text"
        # with single alter table
        col2 = schema._infer_column("col2", 1)
        schema.update_table(new_table(table_name, columns=[col2]))
        schema._bump_version()
        schema_update = client.update_stored_schema()
        assert len(schema_update) == 1
        assert len(schema_update[table_name]["columns"]) == 1
        assert schema_update[table_name]["columns"]["col2"]["data_type"] == "bigint"

        # with 2 alter tables
        col3 = schema._infer_column("col3", 1.2)
        col4 = schema._infer_column("col4", 182879721.182912)
        col4["data_type"] = "timestamp"
        schema.update_table(new_table(table_name, columns=[col3, col4]))
        schema._bump_version()
        schema_update = client.update_stored_schema()
        assert len(schema_update[table_name]["columns"]) == 2
        assert schema_update[table_name]["columns"]["col3"]["data_type"] == "double"
        assert schema_update[table_name]["columns"]["col4"]["data_type"] == "timestamp"
        _, storage_table_cols = client.get_storage_table(table_name)
        # 4 columns
        assert len(storage_table_cols) == 4
        storage_table_cols = normalize_storage_table_cols(table_name, storage_table_cols, schema)
        assert storage_table_cols["col4"]["data_type"] == "timestamp"


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_drop_tables(client: SqlJobClientBase) -> None:
    schema = client.schema
    # Add columns in all tables
    schema.tables["event_user"]["columns"] = dict(schema.tables["event_slot"]["columns"])
    schema.tables["event_bot"]["columns"] = dict(schema.tables["event_slot"]["columns"])
    schema._bump_version()
    client.update_stored_schema()

    # Create a second schema with 2 hashes
    sd = schema.to_dict()
    sd["name"] = "event_2"
    schema_2 = Schema.from_dict(sd).clone()  # type: ignore[arg-type]
    for tbl_name in list(schema_2.tables):
        if tbl_name.startswith("_dlt"):
            continue
        # rename the table properly
        schema_2.tables[tbl_name + "_2"] = schema_2.tables.pop(tbl_name)
        schema_2.tables[tbl_name + "_2"]["name"] = tbl_name + "_2"

    client.schema = schema_2
    client.schema._bump_version()
    client.update_stored_schema()
    client.schema.tables["event_slot_2"]["columns"]["value"]["nullable"] = False
    client.schema._bump_version()
    client.update_stored_schema()

    # Drop tables from the first schema
    client.schema = schema
    tables_to_drop = ["event_slot", "event_user"]
    schema.drop_tables(tables_to_drop)
    schema._bump_version()

    # add one fake table to make sure one table can be ignored
    client.drop_tables(tables_to_drop[0], "not_exists", *tables_to_drop[1:])
    client._update_schema_in_storage(schema)  # Schema was deleted, load it in again
    if isinstance(client, WithStagingDataset):
        with contextlib.suppress(DatabaseUndefinedRelation):
            with client.with_staging_dataset():
                client.drop_tables(*tables_to_drop, delete_schema=False)
    # drop again - should not break anything
    client.drop_tables(*tables_to_drop)
    client._update_schema_in_storage(schema)
    if isinstance(client, WithStagingDataset):
        with contextlib.suppress(DatabaseUndefinedRelation):
            with client.with_staging_dataset():
                client.drop_tables(*tables_to_drop, delete_schema=False)

    # Verify requested tables are dropped
    assert all(len(table[1]) == 0 for table in client.get_storage_tables(tables_to_drop))

    # Verify _dlt_version schema is updated and old versions deleted
    table_name = client.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
    rows = client.sql_client.execute_sql(
        f"SELECT version_hash FROM {table_name} WHERE schema_name = %s", schema.name
    )
    assert len(rows) == 1
    assert rows[0][0] == schema.version_hash

    # Other schema is not replaced
    rows = client.sql_client.execute_sql(
        f"SELECT version_hash FROM {table_name} WHERE schema_name = %s", schema_2.name
    )
    assert len(rows) == 2


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_get_storage_table_with_all_types(client: SqlJobClientBase) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_table(new_table(table_name, columns=TABLE_UPDATE))
    schema._bump_version()
    schema_update = client.update_stored_schema()
    # we have all columns in the update
    table_update = schema_update[table_name]["columns"]
    assert set(table_update.keys()) == set(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    # all columns match
    for name, column in table_update.items():
        assert column.items() >= TABLE_UPDATE_COLUMNS_SCHEMA[name].items()
    # now get the actual schema from the db
    _, storage_table = list(client.get_storage_tables([table_name]))[0]
    assert len(storage_table) > 0
    # column order must match TABLE_UPDATE
    storage_columns = list(storage_table.values())
    for c, expected_c in zip(
        TABLE_UPDATE, storage_columns
    ):  # TODO: c and expected_c need to be swapped
        # storage columns are returned with column names as in information schema
        assert client.capabilities.casefold_identifier(c["name"]) == expected_c["name"]
        # athena does not know wei data type and has no JSON type, time is not supported with parquet tables
        if client.config.destination_type == "athena" and c["data_type"] in (
            "wei",
            "json",
            "time",
        ):
            continue
        # mssql, clickhouse and synapse have no native data type for the nested type.
        if client.config.destination_type in ("mssql", "synapse", "clickhouse") and c[
            "data_type"
        ] in ("json"):
            continue
        if client.config.destination_type == "databricks" and c["data_type"] in ("json", "time"):
            continue
        # ClickHouse has no active data type for binary or time type.
        if client.config.destination_type == "clickhouse":
            if c["data_type"] in ("binary", "time"):
                continue
            elif c["data_type"] == "json" and c["nullable"]:
                continue
        if client.config.destination_type == "dremio" and c["data_type"] == "json":
            continue
        if not client.capabilities.supports_native_boolean and c["data_type"] == "bool":
            # The reflected data type is probably either int or boolean depending on how the client is implemented
            assert expected_c["data_type"] in ("bigint", "bool")
            continue

        assert c["data_type"] == expected_c["data_type"]


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, exclude=("sqlalchemy",)),
    indirect=True,
    ids=lambda x: x.name,
)
def test_preserve_column_order(client: SqlJobClientBase) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    import random

    columns = deepcopy(TABLE_UPDATE)
    random.shuffle(columns)

    schema.update_table(new_table(table_name, columns=columns))
    schema._bump_version()

    def _assert_columns_order(sql_: str) -> None:
        idx = 0
        for c in columns:
            if hasattr(client.sql_client, "escape_ddl_identifier"):
                col_name = client.sql_client.escape_ddl_identifier(c["name"])
            else:
                col_name = client.sql_client.escape_column_name(c["name"])
            # find column names
            idx = sql_.find(col_name, idx)
            assert idx > 0, f"column {col_name} not found in script"

    sql = ";".join(client._get_table_update_sql(table_name, columns, generate_alter=False))
    _assert_columns_order(sql)
    sql = ";".join(client._get_table_update_sql(table_name, columns, generate_alter=True))
    _assert_columns_order(sql)


@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_data_writer_load(naming: str, client: SqlJobClientBase, file_storage: FileStorage) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write only first row
    with io.BytesIO() as f:
        write_dataset(client, f, [rows[0]], client.schema.get_table(table_name)["columns"])
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0]
    # content must equal
    assert list(db_row) == list(rows[0].values())
    # write second row that contains two nulls
    with io.BytesIO() as f:
        write_dataset(client, f, [rows[1]], client.schema.get_table(table_name)["columns"])
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    f_int_name = client.schema.naming.normalize_identifier("f_int")
    f_int_name_quoted = client.sql_client.escape_column_name(f_int_name)
    db_row = client.sql_client.execute_sql(
        f"SELECT * FROM {canonical_name} WHERE {f_int_name_quoted} = {rows[1][f_int_name]}"
    )[0]
    assert db_row[3] is None
    assert db_row[5] is None


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_data_writer_string_escape(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    row = rows[0]
    # this will really drop table without escape
    inj_str = f", NULL'); DROP TABLE {canonical_name} --"
    row["f_str"] = inj_str
    with io.BytesIO() as f:
        write_dataset(client, f, [rows[0]], client.schema.get_table(table_name)["columns"])
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0]
    assert list(db_row) == list(row.values())


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_data_writer_string_escape_edge(
    client: SqlJobClientBase, file_storage: FileStorage
) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    rows, table_name = prepare_schema(client, "weird_rows")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    with io.BytesIO() as f:
        write_dataset(client, f, rows, client.schema.get_table(table_name)["columns"])
        query = f.getvalue()
    expect_load_file(client, file_storage, query, table_name)
    for i in range(1, len(rows) + 1):
        db_row = client.sql_client.execute_sql(f"SELECT str FROM {canonical_name} WHERE idx = {i}")
        row_value, expected = db_row[0][0], rows[i - 1]["str"]
        assert row_value == expected


@pytest.mark.parametrize("naming", TEST_NAMING_CONVENTIONS, indirect=True)
@pytest.mark.parametrize("write_disposition", ["append", "replace"])
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_load_with_all_types(
    naming: str,
    client: SqlJobClientBase,
    write_disposition: TWriteDisposition,
    file_storage: FileStorage,
) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    table_name = "event_test_table" + uniq_id()
    column_schemas, data_row = get_columns_and_row_all_types(client.config)

    # we should have identical content with all disposition types
    partial = client.schema.update_table(
        new_table(
            table_name, write_disposition=write_disposition, columns=list(column_schemas.values())
        )
    )
    # get normalized schema
    table_name = partial["name"]
    column_schemas = partial["columns"]
    normalize_rows([data_row], client.schema.naming)
    client.schema._bump_version()
    client.update_stored_schema()

    if isinstance(client, WithStagingDataset):
        should_load_to_staging = client.should_load_data_to_staging_dataset(table_name)
        if should_load_to_staging:
            with client.with_staging_dataset():
                # create staging for merge dataset
                client.initialize_storage()
                client.update_stored_schema()

        with client.sql_client.with_alternative_dataset_name(
            client.sql_client.staging_dataset_name
            if should_load_to_staging
            else client.sql_client.dataset_name
        ):
            canonical_name = client.sql_client.make_qualified_table_name(table_name)
    else:
        canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write row
    print(data_row)
    with io.BytesIO() as f:
        write_dataset(client, f, [data_row], column_schemas)
        query = f.getvalue()
    print(client.schema.to_pretty_yaml())
    expect_load_file(client, file_storage, query, table_name)
    db_row = list(client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0])
    assert len(db_row) == len(data_row)
    # assert_all_data_types_row has many hardcoded columns so for now skip that part
    if naming == "snake_case":
        # content must equal
        assert_all_data_types_row(
            db_row,
            data_row,
            schema=column_schemas,
            allow_base64_binary=client.config.destination_type in ["clickhouse"],
        )


@pytest.mark.parametrize(
    "write_disposition,replace_strategy",
    [
        ("append", ""),
        ("merge", ""),
        ("replace", "truncate-and-insert"),
        ("replace", "insert-from-staging"),
        ("replace", "staging-optimized"),
    ],
)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_write_dispositions(
    client: SqlJobClientBase,
    write_disposition: TWriteDisposition,
    replace_strategy: str,
    file_storage: FileStorage,
) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy

    table_name = "event_test_table" + uniq_id()
    column_schemas, data_row = get_columns_and_row_all_types(client.config)
    client.schema.update_table(
        new_table(table_name, write_disposition=write_disposition, columns=column_schemas.values())
    )
    child_table = client.schema.naming.make_path(table_name, "child")
    # add child table without write disposition so it will be inferred from the parent
    client.schema.update_table(
        new_table(child_table, columns=column_schemas.values(), parent_table_name=table_name)
    )
    client.schema._bump_version()
    client.update_stored_schema()

    if write_disposition == "merge":
        if not client.capabilities.supported_merge_strategies:
            pytest.skip("destination does not support merge")
        # add root key
        client.schema.tables[table_name]["columns"]["col1"]["root_key"] = True
        # create staging for merge dataset
        with client.with_staging_dataset():  # type: ignore[attr-defined]
            client.initialize_storage()
            client.schema._bump_version()
            client.update_stored_schema()
    for idx in range(2):
        # in the replace strategies, tables get truncated between loads
        truncate_tables = [table_name, child_table]
        if write_disposition == "replace":
            client.initialize_storage(truncate_tables=truncate_tables)

        for t in [table_name, child_table]:
            # write row, use col1 (INT) as row number
            data_row["col1"] = idx
            with io.BytesIO() as f:
                write_dataset(client, f, [data_row], column_schemas)
                query = f.getvalue()
            if isinstance(
                client, WithStagingDataset
            ) and client.should_load_data_to_staging_dataset(table_name):
                # load to staging dataset on merge
                with client.with_staging_dataset():
                    expect_load_file(client, file_storage, query, t)
            else:
                # load directly on other
                expect_load_file(client, file_storage, query, t)
            db_rows = list(
                client.sql_client.execute_sql(
                    f"SELECT * FROM {client.sql_client.make_qualified_table_name(t)} ORDER BY"
                    " col1 ASC"
                )
            )
            # in case of merge
            if write_disposition == "append":
                # we append 1 row to tables in each iteration
                assert len(db_rows) == idx + 1
            elif write_disposition == "replace":
                # we overwrite with the same row. merge falls back to replace when no keys specified
                assert len(db_rows) == 1
            else:
                # merge on client level, without loader, loads to staging dataset. so this table is empty
                assert len(db_rows) == 0
                # check staging
                with client.sql_client.with_staging_dataset():
                    db_rows = list(
                        client.sql_client.execute_sql(
                            f"SELECT * FROM {client.sql_client.make_qualified_table_name(t)} ORDER"
                            " BY col1 ASC"
                        )
                    )
                    assert len(db_rows) == idx + 1
            # last row must have our last idx - make sure we append and overwrite
            assert db_rows[-1][0] == idx


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_get_resumed_job(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    user_table_name = prepare_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
        "timestamp": pendulum.now(),
    }
    print(client.schema.get_table(user_table_name)["columns"])
    with io.BytesIO() as f:
        write_dataset(client, f, [load_json], client.schema.get_table(user_table_name)["columns"])
        dataset = f.getvalue()
    job = expect_load_file(client, file_storage, dataset, user_table_name)
    # now try to retrieve the job
    # TODO: we should re-create client instance as this call is intended to be run after some disruption ie. stopped loader process
    r_job = client.create_load_job(
        client.prepare_load_table(user_table_name),
        file_storage.make_full_path(job.file_name()),
        uniq_id(),
        restore=True,
    )
    assert r_job.state() == "ready"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, exclude=["dremio"]),
    ids=lambda x: x.name,
)
def test_default_schema_name_init_storage(destination_config: DestinationTestConfiguration) -> None:
    with cm_yield_client_with_storage(
        destination_config.destination_factory(),
        default_config_values={
            "default_schema_name": (  # pass the schema that is a default schema. that should create dataset with the name `dataset_name`
                "event"
            )
        },
    ) as client:
        assert client.sql_client.dataset_name == client.config.dataset_name
        assert client.sql_client.has_dataset()

    with cm_yield_client_with_storage(
        destination_config.destination_factory(),
        default_config_values={
            "default_schema_name": (
                None  # no default_schema. that should create dataset with the name `dataset_name`
            )
        },
    ) as client:
        assert client.sql_client.dataset_name == client.config.dataset_name
        assert client.sql_client.has_dataset()

    with cm_yield_client_with_storage(
        destination_config.destination_factory(),
        default_config_values={
            "default_schema_name": (  # the default schema is not event schema . that should create dataset with the name `dataset_name` with schema suffix
                "event_2"
            )
        },
    ) as client:
        assert client.sql_client.dataset_name == client.config.dataset_name + "_event"
        assert client.sql_client.has_dataset()


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize(
    "naming_convention",
    [
        "tests.common.cases.normalizers.title_case",
        "snake_case",
    ],
)
def test_get_stored_state(
    destination_config: DestinationTestConfiguration,
    naming_convention: str,
    file_storage: FileStorage,
) -> None:
    os.environ["SCHEMA__NAMING"] = naming_convention

    with cm_yield_client_with_storage(
        destination_config.destination_factory(),
        default_config_values={"default_schema_name": None},
    ) as client:
        # event schema with event table
        if not client.capabilities.preferred_loader_file_format:
            pytest.skip(
                "preferred loader file format not set, destination will only work with staging"
            )
        # load pipeline state
        state_table = pipeline_state_table()
        partial = client.schema.update_table(state_table)
        print(partial)
        client.schema._bump_version()
        client.update_stored_schema()

        state_info = StateInfo(1, 4, "pipeline", "compressed", pendulum.now(), None, "_load_id")
        doc = state_info.as_doc()
        norm_doc = {client.schema.naming.normalize_identifier(k): v for k, v in doc.items()}
        with io.BytesIO() as f:
            # use normalized columns
            write_dataset(client, f, [norm_doc], partial["columns"])
            query = f.getvalue()
        expect_load_file(client, file_storage, query, partial["name"])
        client.complete_load("_load_id")

        # get state
        stored_state = client.get_stored_state("pipeline")
        # Ensure timezone aware datetime for comparing
        stored_state.created_at = pendulum.instance(stored_state.created_at)
        assert doc == stored_state.as_doc()


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_many_schemas_single_dataset(
    destination_config: DestinationTestConfiguration, file_storage: FileStorage
) -> None:
    def _load_something(_client: SqlJobClientBase, expected_rows: int) -> None:
        # load something to event:user_table
        user_row = {
            "_dlt_id": uniq_id(),
            "_dlt_root_id": "b",
            # "_dlt_load_id": "load_id",
            "event": "user",
            "sender_id": "sender_id",
            "timestamp": pendulum.now(),
        }
        with io.BytesIO() as f:
            write_dataset(
                _client,
                f,
                [user_row],
                _client.schema.tables["event_user"]["columns"],
                file_format=destination_config.file_format,
            )
            query = f.getvalue()
        expect_load_file(
            _client, file_storage, query, "event_user", file_format=destination_config.file_format
        )
        qual_table_name = _client.sql_client.make_qualified_table_name("event_user")
        db_rows = list(_client.sql_client.execute_sql(f"SELECT * FROM {qual_table_name}"))
        assert len(db_rows) == expected_rows

    with cm_yield_client_with_storage(
        destination_config.destination_factory(),
        default_config_values={"default_schema_name": None},
    ) as client:
        # event schema with event table
        if not client.capabilities.preferred_loader_file_format:
            pytest.skip(
                "preferred loader file format not set, destination will only work with staging"
            )

        user_table = load_table("event_user")["event_user"]
        client.schema.update_table(new_table("event_user", columns=list(user_table.values())))
        client.schema._bump_version()
        schema_update = client.update_stored_schema()
        assert len(schema_update) > 0

        _load_something(client, 1)

        # event_2 schema with identical event table
        event_schema = client.schema
        schema_dict = event_schema.to_dict()
        schema_dict["name"] = "event_2"
        event_2_schema = Schema.from_stored_schema(schema_dict)
        # swap schemas in client instance
        client.schema = event_2_schema
        client.schema._bump_version()
        schema_update = client.update_stored_schema()
        # no were detected - even if the schema is new. all the tables overlap
        assert schema_update == {}
        # two different schemas in dataset
        assert event_schema.version_hash != event_2_schema.version_hash
        ev_1_info = client.get_stored_schema_by_hash(event_schema.version_hash)
        assert ev_1_info.schema_name == "event"
        ev_2_info = client.get_stored_schema_by_hash(event_2_schema.version_hash)
        assert ev_2_info.schema_name == "event_2"
        # two rows because we load to the same table
        _load_something(client, 2)

        # use third schema where one of the fields is non null, but the field exists so it is ignored
        schema_dict["name"] = "event_3"
        event_3_schema = Schema.from_stored_schema(schema_dict)
        event_3_schema.tables["event_user"]["columns"]["input_channel"]["nullable"] = False
        # swap schemas in client instance
        client.schema = event_3_schema
        client.schema._bump_version()
        schema_update = client.update_stored_schema()
        # no were detected - even if the schema is new. all the tables overlap and change in nullability does not do any updates
        assert schema_update == {}
        # 3 rows because we load to the same table
        if (
            destination_config.file_format == "parquet"
            or client.capabilities.preferred_loader_file_format == "parquet"
        ):
            event_3_schema.tables["event_user"]["columns"]["input_channel"]["nullable"] = True
        _load_something(client, 3)

        # adding new non null column will generate sync error, except for clickhouse, there it will work
        event_3_schema.tables["event_user"]["columns"]["mandatory_column"] = new_column(
            "mandatory_column", "text", nullable=False
        )
        client.schema._bump_version()
        if destination_config.destination_type == "clickhouse" or (
            # mysql allows adding not-null columns (they have an implicit default)
            destination_config.destination_type == "sqlalchemy"
            and client.sql_client.dialect_name == "mysql"
        ):
            client.update_stored_schema()
        else:
            with pytest.raises(DatabaseException) as py_ex:
                client.update_stored_schema()
            assert (
                "mandatory_column" in str(py_ex.value).lower()
                or "NOT NULL" in str(py_ex.value)
                or "Adding columns with constraints not yet supported" in str(py_ex.value)
            )


# NOTE: this could be folded into the above tests, but these only run on sql_client destinations for now
# but we want to test filesystem and vector db here too
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_vector_configs=True, all_buckets_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
def test_schema_retrieval(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("schema_test", dev_mode=True)
    from dlt.common.schema import utils

    # we create 2 versions of 2 schemas
    s1_v1 = Schema("schema_1")
    s1_v2 = s1_v1.clone()
    s1_v2.tables["items"] = utils.new_table("items")
    s2_v1 = Schema("schema_2")
    s2_v2 = s2_v1.clone()
    s2_v2.tables["other_items"] = utils.new_table("other_items")

    # sanity check
    assert s1_v1.version_hash != s1_v2.version_hash
    assert s2_v1.version_hash != s2_v2.version_hash

    client: WithStateSync

    def add_schema_to_pipeline(s: Schema) -> None:
        p._inject_schema(s)
        p.default_schema_name = s.name
        with p.destination_client() as client:
            client.initialize_storage()
            client.update_stored_schema()

    # check what happens if there is only one
    add_schema_to_pipeline(s1_v1)
    p.default_schema_name = s1_v1.name
    with p.destination_client() as client:  # type: ignore[assignment]
        assert client.get_stored_schema("schema_1").version_hash == s1_v1.version_hash
        assert client.get_stored_schema().version_hash == s1_v1.version_hash
        assert not client.get_stored_schema("other_schema")

    # now we add a different schema
    # but keep default schema name at v1
    add_schema_to_pipeline(s2_v1)
    p.default_schema_name = s1_v1.name
    with p.destination_client() as client:  # type: ignore[assignment]
        assert client.get_stored_schema("schema_1").version_hash == s1_v1.version_hash
        # here v2 will be selected as it is newer
        assert client.get_stored_schema(None).version_hash == s2_v1.version_hash
        assert not client.get_stored_schema("other_schema")

    # add two more version,
    add_schema_to_pipeline(s1_v2)
    add_schema_to_pipeline(s2_v2)
    p.default_schema_name = s1_v1.name
    with p.destination_client() as client:  # type: ignore[assignment]
        assert client.get_stored_schema("schema_1").version_hash == s1_v2.version_hash
        # here v2 will be selected as it is newer
        assert client.get_stored_schema(None).version_hash == s2_v2.version_hash
        assert not client.get_stored_schema("other_schema")

    # check same setup with other default schema name
    p.default_schema_name = s2_v1.name
    with p.destination_client() as client:  # type: ignore[assignment]
        assert client.get_stored_schema("schema_2").version_hash == s2_v2.version_hash
        # here v2 will be selected as it is newer
        assert client.get_stored_schema(None).version_hash == s2_v2.version_hash
        assert not client.get_stored_schema("other_schema")


def prepare_schema(client: SqlJobClientBase, case: str) -> Tuple[List[Dict[str, Any]], str]:
    client.update_stored_schema()
    rows = load_json_case(case)
    # normalize rows
    normalize_rows(rows, client.schema.naming)
    # use first row to infer table
    table: TTableSchemaColumns = {k: client.schema._infer_column(k, v) for k, v in rows[0].items()}
    table_name = f"event_{case}_{uniq_id()}"
    partial = client.schema.update_table(new_table(table_name, columns=list(table.values())))
    client.schema._bump_version()
    client.update_stored_schema()
    # return normalized name
    return rows, partial["name"]


def normalize_rows(rows: List[Dict[str, Any]], naming: NamingConvention) -> None:
    for row in rows:
        for k in list(row.keys()):
            row[naming.normalize_identifier(k)] = row.pop(k)


def get_columns_and_row_all_types(destination_config: DestinationClientConfiguration):
    exclude_types = []
    if destination_config.destination_type in ["databricks", "clickhouse", "motherduck"]:
        exclude_types.append("time")
    if destination_config.destination_name == "sqlalchemy_sqlite":
        exclude_types.extend(["decimal", "wei"])
    return table_update_and_row(
        # TIME + parquet is actually a duckdb problem: https://github.com/duckdb/duckdb/pull/13283
        exclude_types=exclude_types,  # type: ignore[arg-type]
        exclude_columns=(
            ["col4_precision"] if destination_config.destination_type in ["motherduck"] else None
        ),
    )
