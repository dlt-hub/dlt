import pytest
import io
from dlt.common import pendulum

from dlt.common.file_storage import FileStorage
from dlt.common.configuration import GcpClientConfiguration
from dlt.common.utils import uniq_id
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.dataset_writers import write_jsonl

from dlt.loaders.configuration import configuration
from dlt.loaders.gcp.client import BigQueryClient

from tests.utils import TEST_STORAGE, delete_storage
from tests.loaders.utils import TABLE_UPDATE, TABLE_ROW, expect_load_file, prepare_event_user_table


@pytest.fixture(scope="module")
def gcp_client() -> BigQueryClient:
    # create dataset with random name
    CLIENT_CONFIG: GcpClientConfiguration = configuration({"CLIENT_TYPE": "gcp"})
    CLIENT_CONFIG.DATASET = "test_" + uniq_id()
    # get event default schema
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    # create client and dataset
    with BigQueryClient(schema, CLIENT_CONFIG) as client:
        client.initialize_storage()
        yield client
        # delete dataset
        dataset_name = client._to_canonical_schema_name()
        client._client.delete_dataset(dataset_name, not_found_ok=True, delete_contents=True, retry=client.default_retry, timeout=client.C.TIMEOUT)


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


@pytest.mark.order(1)
def test_initialize_storage(gcp_client: BigQueryClient) -> None:
    pass


@pytest.mark.order(2)
def test_get_version_on_empty(gcp_client: BigQueryClient) -> None:
    version = gcp_client._get_schema_version_from_storage()
    assert version == 0


@pytest.mark.order(3)
def test_get_update_basic_schema(gcp_client: BigQueryClient) -> None:
    # schema update is not needed in gcp client
    gcp_client.update_storage_schema()
    version = gcp_client._get_schema_version_from_storage()
    assert version == 1
    # modify version
    gcp_client._update_schema_version(2)
    version = gcp_client._get_schema_version_from_storage()
    assert version == 2
    gcp_client._update_schema_version(1)
    version = gcp_client._get_schema_version_from_storage()
    assert version == 1
    # check if tables are present and identical
    exists, version_storage_table = gcp_client._get_storage_table(Schema.VERSION_TABLE_NAME)
    version_schema_table = gcp_client._get_table_by_name(Schema.VERSION_TABLE_NAME, "");
    assert exists is True
    # schema version must be contained in storage version (schema may not have all hints)
    assert version_schema_table.keys() == version_storage_table.keys()
    assert all(set(v) <= set(version_storage_table[k]) for k, v in version_schema_table.items())


@pytest.mark.order(4)
def test_complete_load(gcp_client: BigQueryClient) -> None:
    gcp_client.update_storage_schema()
    load_id = "182879721.182912"
    gcp_client.complete_load(load_id)
    load_table = gcp_client._to_canonical_table_name(Schema.LOADS_TABLE_NAME)
    load_rows = list(gcp_client._execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 1
    assert load_rows[0][0] == load_id
    assert load_rows[0][1] == 0
    import datetime
    assert type(load_rows[0][2]) is datetime.datetime
    gcp_client.complete_load("load2")
    load_rows = list(gcp_client._execute_sql(f"SELECT * FROM {load_table}"))
    assert len(load_rows) == 2


@pytest.mark.order(5)
def test_schema_update_create_table(gcp_client: BigQueryClient) -> None:
    # infer typical rasa event schema
    schema = gcp_client.schema
    # this will be partition
    timestamp = schema._infer_column("timestamp", 182879721.182912)
    # this will be cluster
    sender_id = schema._infer_column("sender_id", "982398490809324")
    # this will be not null
    record_hash = schema._infer_column("_dlt_id", "m,i0392903jdlkasjdlk")
    schema.update_schema(new_table("event_test_table", columns=[timestamp, sender_id, record_hash]))
    gcp_client.update_storage_schema()
    exists, storage_table = gcp_client._get_storage_table("event_test_table")
    assert exists is True
    assert storage_table["timestamp"]["partition"] is True
    assert storage_table["sender_id"]["cluster"] is True
    exists, storage_table = gcp_client._get_storage_table("_dlt_version")
    assert exists is True
    assert storage_table["version"]["partition"] is False
    assert storage_table["version"]["cluster"] is False


@pytest.mark.order(6)
def test_schema_update_alter_table(gcp_client: BigQueryClient) -> None:
    schema = gcp_client.schema
    col1 = schema._infer_column("col1", "string")
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=[col1]))
    gcp_client.update_storage_schema()
    # with single alter table
    col2 = schema._infer_column("col2", 1)
    schema.update_schema(new_table(table_name, columns=[col2]))
    gcp_client.update_storage_schema()
    # with 2 alter tables
    col3 = schema._infer_column("col3", 1.2)
    col4 = schema._infer_column("col4", 182879721.182912)
    col4["data_type"] = "timestamp"
    schema.update_schema(new_table(table_name, columns=[col3, col4]))
    gcp_client.update_storage_schema()
    _, storage_table = gcp_client._get_storage_table(table_name)
    # 4 columns
    assert len(storage_table) == 4
    assert storage_table["col4"]["data_type"] == "timestamp"


@pytest.mark.order(7)
def test_get_storage_table_with_all_types(gcp_client: BigQueryClient) -> None:
    schema = gcp_client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=TABLE_UPDATE))
    gcp_client.update_storage_schema()
    exists, storage_table = gcp_client._get_storage_table(table_name)
    assert exists is True
    # column order must match TABLE_UPDATE
    storage_columns = list(storage_table.values())
    for c, s_c in zip(TABLE_UPDATE, storage_columns):
        assert c["name"] == s_c["name"]
        if c["data_type"] == "complex":
            assert s_c["data_type"] == "text"
        else:
            assert c["data_type"] == s_c["data_type"]


@pytest.mark.order(8)
def test_load_with_all_types(gcp_client: BigQueryClient, file_storage: FileStorage) -> None:
    schema = gcp_client.schema
    table_name = "event_test_table" + uniq_id()
    schema.update_schema(new_table(table_name, columns=TABLE_UPDATE))
    gcp_client.update_storage_schema()
    canonical_name = gcp_client._to_canonical_table_name(table_name)
    # write row
    with io.StringIO() as f:
        write_jsonl(f, [TABLE_ROW])
        query = f.getvalue()
    expect_load_file(gcp_client, file_storage, query, table_name)
    db_row = list(list(gcp_client._execute_sql(f"SELECT * FROM {canonical_name}"))[0])
    # content must equal
    db_row[3] = str(pendulum.instance(db_row[3]))  # serialize date
    assert db_row == list(TABLE_ROW.values())


# @pytest.mark.order(9)
# def test_loading_errors(client: gcp_client, file_storage: FileStorage) -> None:
#     user_table_name = prepare_event_user_table(client)
#     # insert into unknown column
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, _unk_) VALUES\n"
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', NULL);"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.UndefinedColumn
#     # insert null value
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', NULL);"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
#     # insert wrong type
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp) VALUES\n"
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', TRUE);"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.DatatypeMismatch
#     # numeric overflow on bigint
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, metadata__rasa_x_id) VALUES\n"
#     # 2**64//2 - 1 is a maximum bigint value
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {2**64//2});"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.NumericValueOutOfRange
#     # numeric overflow on NUMERIC
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__intent__id) VALUES\n"
#     # default redshift decimal is (18, 0) (64 bit)
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**18});"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_
#     # max redshift decimal is (38, 0) (128 bit) = 10**38 - 1
#     insert_sql = "INSERT INTO {}(_dlt_id, _dlt_root_id, sender_id, timestamp, parse_data__metadata__rasa_x_id) VALUES\n"
#     insert_values = f"('{uniq_id()}', '{uniq_id()}', '90238094809sajlkjxoiewjhduuiuehd', '{str(pendulum.now())}', {10**38});"
#     with pytest.raises(LoadClientTerminalInnerException) as exv:
#         expect_load_file(client, file_storage, insert_sql+insert_values, user_table_name)
#     assert type(exv.value.inner_exc) is psycopg2.errors.InternalError_