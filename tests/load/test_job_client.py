import contextlib
from copy import deepcopy
import io, os
from time import sleep
from unittest.mock import patch
import pytest
import datetime  # noqa: I251
from typing import Iterator, Tuple, List, Dict, Any, Mapping, MutableMapping

from dlt.common import json, pendulum
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    VERSION_TABLE_NAME,
    TWriteDisposition,
    TTableSchema,
)
from dlt.common.schema.utils import new_table, new_column
from dlt.common.storages import FileStorage
from dlt.common.schema import TTableSchemaColumns
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import (
    DatabaseException,
    DatabaseTerminalException,
    DatabaseUndefinedRelation,
)

from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.common.destination.reference import WithStagingDataset

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage
from tests.common.utils import load_json_case
from tests.load.utils import (
    TABLE_UPDATE,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_ROW_ALL_DATA_TYPES,
    assert_all_data_types_row,
    expect_load_file,
    load_table,
    yield_client_with_storage,
    cm_yield_client_with_storage,
    write_dataset,
    prepare_table,
)
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(scope="function")
def client(request) -> Iterator[SqlJobClientBase]:
    yield from yield_client_with_storage(request.param.destination)


@pytest.mark.order(1)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_initialize_storage(client: SqlJobClientBase) -> None:
    pass


@pytest.mark.order(2)
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_get_schema_on_empty_storage(client: SqlJobClientBase) -> None:
    # test getting schema on empty dataset without any tables
    exists, _ = client.get_storage_table(VERSION_TABLE_NAME)
    assert exists is False
    schema_info = client.get_stored_schema()
    assert schema_info is None
    schema_info = client.get_stored_schema_by_hash("8a0298298823928939")
    assert schema_info is None


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
    exists, _ = client.get_storage_table(VERSION_TABLE_NAME)
    assert exists is True
    exists, _ = client.get_storage_table(LOADS_TABLE_NAME)
    assert exists is True
    # verify if schemas stored
    this_schema = client.get_stored_schema_by_hash(schema.version_hash)
    newest_schema = client.get_stored_schema()
    # should point to the same schema
    assert this_schema == newest_schema
    # check fields
    assert this_schema.version == 1 == schema.version
    assert this_schema.version_hash == schema.stored_version_hash
    assert this_schema.engine_version == schema.ENGINE_VERSION
    assert this_schema.schema_name == schema.name
    assert isinstance(this_schema.inserted_at, datetime.datetime)
    # also the content must be the same
    assert this_schema.schema == json.dumps(schema.to_dict())
    first_version_schema = this_schema.schema

    # modify schema
    schema.tables["event_slot"]["write_disposition"] = "replace"
    schema.bump_version()
    assert schema.version > this_schema.version

    # update in storage
    client._update_schema_in_storage(schema)
    this_schema = client.get_stored_schema_by_hash(schema.version_hash)
    newest_schema = client.get_stored_schema()
    assert this_schema == newest_schema
    assert this_schema.version == schema.version == 2
    assert this_schema.version_hash == schema.stored_version_hash

    # simulate parallel write: initial schema is modified differently and written alongside the first one
    # in that case the version will not change or go down
    first_schema = Schema.from_dict(json.loads(first_version_schema))
    first_schema.tables["event_bot"]["write_disposition"] = "replace"
    first_schema.bump_version()
    assert first_schema.version == this_schema.version == 2
    # wait to make load_newest_schema deterministic
    sleep(0.1)
    client._update_schema_in_storage(first_schema)
    this_schema = client.get_stored_schema_by_hash(first_schema.version_hash)
    newest_schema = client.get_stored_schema()
    assert this_schema == newest_schema  # error
    assert this_schema.version == first_schema.version == 2
    assert this_schema.version_hash == first_schema.stored_version_hash

    # get schema with non existing hash
    assert client.get_stored_schema_by_hash("XAXXA") is None

    # mock other schema in client and get the newest schema. it should not exist...
    client.schema = Schema("ethereum")
    assert client.get_stored_schema() is None
    client.schema.bump_version()
    schema_update = client.update_stored_schema()
    # no schema updates because schema has no tables
    assert schema_update == {}
    that_info = client.get_stored_schema()
    assert that_info.schema_name == "ethereum"

    # get event schema again
    client.schema = Schema("event")
    this_schema = client.get_stored_schema()
    assert this_schema == newest_schema


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_complete_load(client: SqlJobClientBase) -> None:
    client.update_stored_schema()
    load_id = "182879721.182912"
    client.complete_load(load_id)
    load_table = client.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 1
    assert load_rows[0][0] == load_id
    assert load_rows[0][1] == client.schema.name
    assert load_rows[0][2] == 0
    import datetime  # noqa: I251

    assert type(load_rows[0][3]) is datetime.datetime
    assert load_rows[0][4] == client.schema.version_hash
    # make sure that hash in loads exists in schema versions table
    versions_table = client.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
    version_rows = list(
        client.sql_client.execute_sql(
            f"SELECT * FROM {versions_table} WHERE version_hash = %s", load_rows[0][4]
        )
    )
    assert len(version_rows) == 1
    client.complete_load("load2")
    load_rows = list(client.sql_client.execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 2


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=["redshift", "postgres", "duckdb"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_schema_update_create_table_redshift(client: SqlJobClientBase) -> None:
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
    schema.bump_version()
    schema_update = client.update_stored_schema()
    # check hints in schema update
    table_update = schema_update[table_name]["columns"]
    assert table_update["timestamp"]["sort"] is True
    assert table_update["sender_id"]["cluster"] is True
    assert table_update["_dlt_id"]["unique"] is True
    exists, _ = client.get_storage_table(table_name)
    assert exists is True


@pytest.mark.parametrize(
    "client",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    indirect=True,
    ids=lambda x: x.name,
)
def test_schema_update_create_table_bigquery(client: SqlJobClientBase) -> None:
    # infer typical rasa event schema
    schema = client.schema
    # this will be partition
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    # this will be cluster
    sender_id = schema._infer_column("sender_id", "982398490809324")
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    schema.update_table(new_table("event_test_table", columns=[timestamp, sender_id, record_hash]))
    schema.bump_version()
    schema_update = client.update_stored_schema()
    # check hints in schema update
    table_update = schema_update["event_test_table"]["columns"]
    assert table_update["timestamp"]["partition"] is True
    assert table_update["_dlt_id"]["nullable"] is False
    exists, storage_table = client.get_storage_table("event_test_table")
    assert exists is True
    assert storage_table["timestamp"]["partition"] is True
    assert storage_table["sender_id"]["cluster"] is True
    exists, storage_table = client.get_storage_table("_dlt_version")
    assert exists is True
    assert storage_table["version"]["partition"] is False
    assert storage_table["version"]["cluster"] is False


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
        schema.bump_version()
        schema_update = client.update_stored_schema()
        assert table_name in schema_update
        assert len(schema_update[table_name]["columns"]) == 1
        assert schema_update[table_name]["columns"]["col1"]["data_type"] == "text"
        # with single alter table
        col2 = schema._infer_column("col2", 1)
        schema.update_table(new_table(table_name, columns=[col2]))
        schema.bump_version()
        schema_update = client.update_stored_schema()
        assert len(schema_update) == 1
        assert len(schema_update[table_name]["columns"]) == 1
        assert schema_update[table_name]["columns"]["col2"]["data_type"] == "bigint"

        # with 2 alter tables
        col3 = schema._infer_column("col3", 1.2)
        col4 = schema._infer_column("col4", 182879721.182912)
        col4["data_type"] = "timestamp"
        schema.update_table(new_table(table_name, columns=[col3, col4]))
        schema.bump_version()
        schema_update = client.update_stored_schema()
        assert len(schema_update[table_name]["columns"]) == 2
        assert schema_update[table_name]["columns"]["col3"]["data_type"] == "double"
        assert schema_update[table_name]["columns"]["col4"]["data_type"] == "timestamp"
        _, storage_table = client.get_storage_table(table_name)
        # 4 columns
        assert len(storage_table) == 4
        assert storage_table["col4"]["data_type"] == "timestamp"


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_drop_tables(client: SqlJobClientBase) -> None:
    schema = client.schema
    # Add columns in all tables
    schema.tables["event_user"]["columns"] = dict(schema.tables["event_slot"]["columns"])
    schema.tables["event_bot"]["columns"] = dict(schema.tables["event_slot"]["columns"])
    schema.bump_version()
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
    client.schema.bump_version()
    client.update_stored_schema()
    client.schema.tables["event_slot_2"]["columns"]["value"]["nullable"] = False
    client.schema.bump_version()
    client.update_stored_schema()

    # Drop tables from the first schema
    client.schema = schema
    tables_to_drop = ["event_slot", "event_user"]
    for tbl in tables_to_drop:
        del schema.tables[tbl]
    schema.bump_version()
    client.drop_tables(*tables_to_drop)
    if isinstance(client, WithStagingDataset):
        with contextlib.suppress(DatabaseUndefinedRelation):
            with client.with_staging_dataset():
                client.drop_tables(*tables_to_drop, replace_schema=False)
    # drop again - should not break anything
    client.drop_tables(*tables_to_drop)
    if isinstance(client, WithStagingDataset):
        with contextlib.suppress(DatabaseUndefinedRelation):
            with client.with_staging_dataset():
                client.drop_tables(*tables_to_drop, replace_schema=False)

    # Verify requested tables are dropped
    for tbl in tables_to_drop:
        exists, _ = client.get_storage_table(tbl)
        assert not exists

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
    schema.bump_version()
    schema_update = client.update_stored_schema()
    # we have all columns in the update
    table_update = schema_update[table_name]["columns"]
    assert set(table_update.keys()) == set(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    # all columns match
    for name, column in table_update.items():
        assert column.items() >= TABLE_UPDATE_COLUMNS_SCHEMA[name].items()
    # now get the actual schema from the db
    exists, storage_table = client.get_storage_table(table_name)
    assert exists is True
    # column order must match TABLE_UPDATE
    storage_columns = list(storage_table.values())
    for c, expected_c in zip(TABLE_UPDATE, storage_columns):
        # print(c["name"])
        # print(c["data_type"])
        assert c["name"] == expected_c["name"]
        # athena does not know wei data type and has no JSON type, time is not supported with parquet tables
        if client.config.destination_type == "athena" and c["data_type"] in (
            "wei",
            "complex",
            "time",
        ):
            continue
        # mssql and synapse have no native data type for the complex type.
        if client.config.destination_type in ("mssql", "synapse") and c["data_type"] in ("complex"):
            continue
        assert c["data_type"] == expected_c["data_type"]


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_preserve_column_order(client: SqlJobClientBase) -> None:
    schema = client.schema
    table_name = "event_test_table" + uniq_id()
    import random

    columns = deepcopy(TABLE_UPDATE)
    random.shuffle(columns)

    schema.update_table(new_table(table_name, columns=columns))
    schema.bump_version()

    def _assert_columns_order(sql_: str) -> None:
        idx = 0
        for c in columns:
            if hasattr(client.sql_client, "escape_ddl_identifier"):
                col_name = client.sql_client.escape_ddl_identifier(c["name"])
            else:
                col_name = client.capabilities.escape_identifier(c["name"])
            print(col_name)
            # find column names
            idx = sql_.find(col_name, idx)
            assert idx > 0, f"column {col_name} not found in script"

    sql = ";".join(client._get_table_update_sql(table_name, columns, generate_alter=False))
    _assert_columns_order(sql)
    sql = ";".join(client._get_table_update_sql(table_name, columns, generate_alter=True))
    _assert_columns_order(sql)


@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_data_writer_load(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    rows, table_name = prepare_schema(client, "simple_row")
    canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write only first row
    with io.BytesIO() as f:
        write_dataset(client, f, [rows[0]], client.schema.get_table(table_name)["columns"])
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0]
    # content must equal
    assert list(db_row) == list(rows[0].values())
    # write second row that contains two nulls
    with io.BytesIO() as f:
        write_dataset(client, f, [rows[1]], client.schema.get_table(table_name)["columns"])
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, table_name)
    db_row = client.sql_client.execute_sql(
        f"SELECT * FROM {canonical_name} WHERE f_int = {rows[1]['f_int']}"
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
        query = f.getvalue().decode()
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
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, table_name)
    for i in range(1, len(rows) + 1):
        db_row = client.sql_client.execute_sql(f"SELECT str FROM {canonical_name} WHERE idx = {i}")
        row_value, expected = db_row[0][0], rows[i - 1]["str"]
        assert row_value == expected


@pytest.mark.parametrize("write_disposition", ["append", "replace"])
@pytest.mark.parametrize(
    "client", destinations_configs(default_sql_configs=True), indirect=True, ids=lambda x: x.name
)
def test_load_with_all_types(
    client: SqlJobClientBase, write_disposition: TWriteDisposition, file_storage: FileStorage
) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    table_name = "event_test_table" + uniq_id()
    # we should have identical content with all disposition types
    client.schema.update_table(
        new_table(table_name, write_disposition=write_disposition, columns=TABLE_UPDATE)
    )
    client.schema.bump_version()
    client.update_stored_schema()

    if client.should_load_data_to_staging_dataset(client.schema.tables[table_name]):  # type: ignore[attr-defined]
        with client.with_staging_dataset():  # type: ignore[attr-defined]
            # create staging for merge dataset
            client.initialize_storage()
            client.update_stored_schema()

    with client.sql_client.with_staging_dataset(
        client.should_load_data_to_staging_dataset(client.schema.tables[table_name])  # type: ignore[attr-defined]
    ):
        canonical_name = client.sql_client.make_qualified_table_name(table_name)
    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [TABLE_ROW_ALL_DATA_TYPES], TABLE_UPDATE_COLUMNS_SCHEMA)
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, table_name)
    db_row = list(client.sql_client.execute_sql(f"SELECT * FROM {canonical_name}")[0])
    # content must equal
    assert_all_data_types_row(db_row)


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
    client.schema.update_table(
        new_table(table_name, write_disposition=write_disposition, columns=TABLE_UPDATE)
    )
    child_table = client.schema.naming.make_path(table_name, "child")
    # add child table without write disposition so it will be inferred from the parent
    client.schema.update_table(
        new_table(child_table, columns=TABLE_UPDATE, parent_table_name=table_name)
    )
    client.schema.bump_version()
    client.update_stored_schema()

    if write_disposition == "merge":
        # add root key
        client.schema.tables[table_name]["columns"]["col1"]["root_key"] = True
        # create staging for merge dataset
        with client.with_staging_dataset():  # type: ignore[attr-defined]
            client.initialize_storage()
            client.schema.bump_version()
            client.update_stored_schema()
    for idx in range(2):
        # in the replace strategies, tables get truncated between loads
        truncate_tables = [table_name, child_table]
        if write_disposition == "replace":
            client.initialize_storage(truncate_tables=truncate_tables)

        for t in [table_name, child_table]:
            # write row, use col1 (INT) as row number
            table_row = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
            table_row["col1"] = idx
            with io.BytesIO() as f:
                write_dataset(client, f, [table_row], TABLE_UPDATE_COLUMNS_SCHEMA)
                query = f.getvalue().decode()
            if client.should_load_data_to_staging_dataset(client.schema.tables[table_name]):  # type: ignore[attr-defined]
                # load to staging dataset on merge
                with client.with_staging_dataset():  # type: ignore[attr-defined]
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
                with client.sql_client.with_staging_dataset(staging=True):
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
def test_retrieve_job(client: SqlJobClientBase, file_storage: FileStorage) -> None:
    if not client.capabilities.preferred_loader_file_format:
        pytest.skip("preferred loader file format not set, destination will only work with staging")
    user_table_name = prepare_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
        "timestamp": str(pendulum.now()),
    }
    with io.BytesIO() as f:
        write_dataset(client, f, [load_json], client.schema.get_table(user_table_name)["columns"])
        dataset = f.getvalue().decode()
    job = expect_load_file(client, file_storage, dataset, user_table_name)
    # now try to retrieve the job
    # TODO: we should re-create client instance as this call is intended to be run after some disruption ie. stopped loader process
    r_job = client.restore_file_load(file_storage.make_full_path(job.file_name()))
    assert r_job.state() == "completed"
    # use just file name to restore
    r_job = client.restore_file_load(job.file_name())
    assert r_job.state() == "completed"


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_default_schema_name_init_storage(destination_config: DestinationTestConfiguration) -> None:
    with cm_yield_client_with_storage(
        destination_config.destination,
        default_config_values={
            "default_schema_name": (  # pass the schema that is a default schema. that should create dataset with the name `dataset_name`
                "event"
            )
        },
    ) as client:
        assert client.sql_client.dataset_name == client.config.dataset_name
        assert client.sql_client.has_dataset()

    with cm_yield_client_with_storage(
        destination_config.destination,
        default_config_values={
            "default_schema_name": (
                None  # no default_schema. that should create dataset with the name `dataset_name`
            )
        },
    ) as client:
        assert client.sql_client.dataset_name == client.config.dataset_name
        assert client.sql_client.has_dataset()

    with cm_yield_client_with_storage(
        destination_config.destination,
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
            "timestamp": str(pendulum.now()),
        }
        with io.BytesIO() as f:
            write_dataset(_client, f, [user_row], _client.schema.tables["event_user"]["columns"])
            query = f.getvalue().decode()
        expect_load_file(_client, file_storage, query, "event_user")
        qual_table_name = _client.sql_client.make_qualified_table_name("event_user")
        db_rows = list(_client.sql_client.execute_sql(f"SELECT * FROM {qual_table_name}"))
        assert len(db_rows) == expected_rows

    with cm_yield_client_with_storage(
        destination_config.destination, default_config_values={"default_schema_name": None}
    ) as client:
        # event schema with event table
        if not client.capabilities.preferred_loader_file_format:
            pytest.skip(
                "preferred loader file format not set, destination will only work with staging"
            )

        user_table = load_table("event_user")["event_user"]
        client.schema.update_table(new_table("event_user", columns=list(user_table.values())))
        client.schema.bump_version()
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
        client.schema.bump_version()
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
        client.schema.bump_version()
        schema_update = client.update_stored_schema()
        # no were detected - even if the schema is new. all the tables overlap and change in nullability does not do any updates
        assert schema_update == {}
        # 3 rows because we load to the same table
        _load_something(client, 3)

        # adding new non null column will generate sync error
        event_3_schema.tables["event_user"]["columns"]["mandatory_column"] = new_column(
            "mandatory_column", "text", nullable=False
        )
        client.schema.bump_version()
        with pytest.raises(DatabaseException) as py_ex:
            client.update_stored_schema()
        assert (
            "mandatory_column" in str(py_ex.value).lower()
            or "NOT NULL" in str(py_ex.value)
            or "Adding columns with constraints not yet supported" in str(py_ex.value)
        )


def prepare_schema(client: SqlJobClientBase, case: str) -> Tuple[List[Dict[str, Any]], str]:
    client.update_stored_schema()
    rows = load_json_case(case)
    # use first row to infer table
    table: TTableSchemaColumns = {k: client.schema._infer_column(k, v) for k, v in rows[0].items()}
    table_name = f"event_{case}_{uniq_id()}"
    client.schema.update_table(new_table(table_name, columns=list(table.values())))
    client.schema.bump_version()
    client.update_stored_schema()
    return rows, table_name
