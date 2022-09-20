from copy import copy
from typing import Iterator
import pytest

from dlt.common import json, pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.file_storage import FileStorage
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.load.exceptions import LoadJobNotExistsException, LoadJobServerTerminalException

from dlt.load import Load
from dlt.load.bigquery.client import BigQueryClient

from tests.utils import TEST_STORAGE, delete_storage
from tests.load.utils import cm_yield_client_with_storage, expect_load_file, prepare_table, yield_client_with_storage


@pytest.fixture(scope="module")
def client() -> Iterator[BigQueryClient]:
    yield from yield_client_with_storage("bigquery")


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


def test_bigquery_job_errors(client: BigQueryClient, file_storage: FileStorage) -> None:
    # non existing job
    with pytest.raises(LoadJobNotExistsException):
        client.restore_file_load(uniq_id() + ".")

    # bad name
    with pytest.raises(LoadJobServerTerminalException):
        client.restore_file_load("!!&*aaa")

    user_table_name = prepare_table(client)

    # start job with non existing file
    with pytest.raises(FileNotFoundError):
        client.start_file_load(client.schema.get_table(user_table_name), uniq_id() + ".")

    # start job with invalid name
    dest_path = file_storage.save("!!aaaa", b"data")
    with pytest.raises(LoadJobServerTerminalException):
        client.start_file_load(client.schema.get_table(user_table_name), dest_path)

    user_table_name = prepare_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id":'90238094809sajlkjxoiewjhduuiuehd',
        "timestamp": str(pendulum.now())
    }
    job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

    # start a job from the same file. it should fallback to retrieve job silently
    r_job = client.start_file_load(client.schema.get_table(user_table_name), file_storage._make_path(job.file_name()))
    assert r_job.status() == "completed"


@pytest.mark.parametrize('location', ["US", "EU"])
def test_bigquery_location(location: str, file_storage: FileStorage) -> None:
    with cm_yield_client_with_storage("bigquery", initial_values={"LOCATION": location}) as client:
        user_table_name = prepare_table(client)
        load_json = {
            "_dlt_id": uniq_id(),
            "_dlt_root_id": uniq_id(),
            "sender_id": '90238094809sajlkjxoiewjhduuiuehd',
            "timestamp": str(pendulum.now())
        }
        job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

        # start a job from the same file. it should fallback to retrieve job silently
        client.start_file_load(client.schema.get_table(user_table_name), file_storage._make_path(job.file_name()))
        canonical_name = client.sql_client.make_qualified_table_name(user_table_name)
        t = client.sql_client.native_connection.get_table(canonical_name)
        assert t.location == location


def test_loading_errors(client: BigQueryClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)
    # insert into unknown column
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id":'90238094809sajlkjxoiewjhduuiuehd',
        "timestamp": str(pendulum.now())
    }
    insert_json = copy(load_json)
    insert_json["_unk_"] = None
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "No such field: _unk_" in job.exception()

    # insert null value
    insert_json = copy(load_json)
    insert_json["timestamp"] = None
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "Only optional fields can be set to NULL. Field: timestamp;" in job.exception()

    # insert wrong type
    insert_json = copy(load_json)
    insert_json["timestamp"] = "AA"
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "Couldn't convert value to timestamp:" in job.exception()

    # numeric overflow on bigint
    insert_json = copy(load_json)
    # 2**64//2 - 1 is a maximum bigint value
    insert_json["metadata__rasa_x_id"] = 2**64//2
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "Could not convert value" in job.exception()

    # numeric overflow on NUMERIC
    insert_json = copy(load_json)
    # default decimal is (38, 9) (128 bit), use local context to generate decimals with 38 precision
    with numeric_default_context():
        below_limit = Decimal(10**29) - Decimal('0.001')
        above_limit = Decimal(10**29)
    # this will pass
    insert_json["parse_data__intent__id"] = below_limit
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="completed")
    # this will fail
    insert_json["parse_data__intent__id"] = above_limit
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "Invalid NUMERIC value: 100000000000000000000000000000 Field: parse_data__intent__id;" in job.exception()

    # max bigquery decimal is (76, 76) (256 bit) = 5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38
    insert_json = copy(load_json)
    insert_json["parse_data__metadata__rasa_x_id"] = Decimal("5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38")
    job = expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name, status="failed")
    assert "Invalid BIGNUMERIC value: 578960446186580977117854925043439539266.34992332820282019728792003956564819968 Field: parse_data__metadata__rasa_x_id;" in job.exception()
