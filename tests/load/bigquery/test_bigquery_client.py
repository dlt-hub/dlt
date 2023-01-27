import base64
from copy import copy
from typing import Any, Iterator
import pytest

from dlt.common import json, pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.storages import FileStorage
from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id, custom_environ

from dlt.destinations.bigquery.bigquery import BigQueryClient
from dlt.destinations.exceptions import LoadJobNotExistsException, LoadJobTerminalException

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, preserve_environ
from tests.common.utils import json_case_path as common_json_case_path
from tests.common.configuration.utils import environment
from tests.load.utils import expect_load_file, prepare_table, yield_client_with_storage, cm_yield_client_with_storage


@pytest.fixture(scope="module")
def client() -> Iterator[BigQueryClient]:
    yield from yield_client_with_storage("bigquery")


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


def test_gcp_credentials_with_default(environment: Any) -> None:
    gcpc = GcpClientCredentialsWithDefault()
    # resolve will miss values and try to find default credentials on the machine
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(gcpc)
    assert py_ex.value.fields == ['project_id', 'private_key', 'client_email']

    # prepare real service.json
    storage = FileStorage("_secrets", makedirs=True)
    with open(common_json_case_path("level-dragon-333019-707809ee408a") + ".b64", mode="br") as f:
        services_str = base64.b64decode(f.read().strip(), validate=True).decode()
    dest_path = storage.save("level-dragon-333019-707809ee408a.json", services_str)

    # create instance of credentials
    gcpc = GcpClientCredentialsWithDefault()
    gcpc.parse_native_representation(services_str)
    # check if credentials can be created
    assert gcpc.to_service_account_credentials() is not None

    # now set the env
    with custom_environ({"GOOGLE_APPLICATION_CREDENTIALS": storage.make_full_path(dest_path)}):
        gcpc = GcpClientCredentialsWithDefault()
        resolve_configuration(gcpc)
        # project id recovered from credentials
        assert gcpc.project_id == "level-dragon-333019"
        # check if credentials can be created
        assert gcpc.to_service_account_credentials() is not None
        # the default credentials are available
        assert gcpc.has_default_credentials() is True
        assert gcpc.default_credentials() is not None


def test_bigquery_job_errors(client: BigQueryClient, file_storage: FileStorage) -> None:
    # non existing job
    with pytest.raises(LoadJobNotExistsException):
        client.restore_file_load(uniq_id() + ".")

    # bad name
    with pytest.raises(LoadJobTerminalException):
        client.restore_file_load("!!&*aaa")

    user_table_name = prepare_table(client)

    # start job with non existing file
    with pytest.raises(FileNotFoundError):
        client.start_file_load(client.schema.get_table(user_table_name), uniq_id() + ".")

    # start job with invalid name
    dest_path = file_storage.save("!!aaaa", b"data")
    with pytest.raises(LoadJobTerminalException):
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
    r_job = client.start_file_load(client.schema.get_table(user_table_name), file_storage.make_full_path(job.file_name()))
    assert r_job.status() == "completed"


@pytest.mark.parametrize('location', ["US", "EU"])
def test_bigquery_location(location: str, file_storage: FileStorage) -> None:
    with cm_yield_client_with_storage("bigquery", default_config_values={"credentials": {"location": location}}) as client:
        user_table_name = prepare_table(client)
        load_json = {
            "_dlt_id": uniq_id(),
            "_dlt_root_id": uniq_id(),
            "sender_id": '90238094809sajlkjxoiewjhduuiuehd',
            "timestamp": str(pendulum.now())
        }
        job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

        # start a job from the same file. it should fallback to retrieve job silently
        client.start_file_load(client.schema.get_table(user_table_name), file_storage.make_full_path(job.file_name()))
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
