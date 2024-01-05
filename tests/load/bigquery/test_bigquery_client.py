import os
import base64
from copy import copy
from typing import Any, Iterator, Tuple, cast, Dict
import pytest

from dlt.common import json, pendulum, Decimal
from dlt.common.arithmetics import numeric_default_context
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpOAuthCredentials,
    GcpOAuthCredentialsWithoutDefaults,
)
from dlt.common.configuration.specs import gcp_credentials
from dlt.common.configuration.specs.exceptions import InvalidGoogleNativeCredentialsType
from dlt.common.storages import FileStorage
from dlt.common.utils import digest128, uniq_id, custom_environ

from dlt.destinations.impl.bigquery.bigquery import BigQueryClient, BigQueryClientConfiguration
from dlt.destinations.exceptions import LoadJobNotExistsException, LoadJobTerminalException

from tests.utils import TEST_STORAGE_ROOT, delete_test_storage, preserve_environ
from tests.common.utils import json_case_path as common_json_case_path
from tests.common.configuration.utils import environment
from tests.load.utils import (
    expect_load_file,
    prepare_table,
    yield_client_with_storage,
    cm_yield_client_with_storage,
)


@pytest.fixture(scope="module")
def client() -> Iterator[BigQueryClient]:
    yield from cast(Iterator[BigQueryClient], yield_client_with_storage("bigquery"))


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


def test_service_credentials_with_default(environment: Any) -> None:
    gcpc = GcpServiceAccountCredentials()
    # resolve will miss values and try to find default credentials on the machine
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(gcpc)
    assert py_ex.value.fields == ["project_id", "private_key", "client_email"]

    # prepare real service.json
    services_str, dest_path = prepare_service_json()

    # create instance of credentials
    gcpc = GcpServiceAccountCredentials()
    gcpc.parse_native_representation(services_str)
    # check if credentials can be created
    assert gcpc.to_native_credentials() is not None

    # reset failed default credentials timeout so we resolve below
    gcp_credentials.GcpDefaultCredentials._LAST_FAILED_DEFAULT = 0

    # now set the env
    with custom_environ({"GOOGLE_APPLICATION_CREDENTIALS": dest_path}):
        gcpc = GcpServiceAccountCredentials()
        resolve_configuration(gcpc)
        # project id recovered from credentials
        assert gcpc.project_id == "level-dragon-333019"
        # check if credentials can be created
        assert gcpc.to_native_credentials() is not None
        # the default credentials are available
        assert gcpc.has_default_credentials() is True
        assert gcpc.default_credentials() is not None


def test_service_credentials_native_credentials_object(environment: Any) -> None:
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials

    _, dest_path = prepare_service_json()
    credentials = ServiceAccountCredentials.from_service_account_file(dest_path)

    def _assert_credentials(gcp_credentials):
        assert gcp_credentials.to_native_credentials() is credentials
        # check props
        assert gcp_credentials.project_id == credentials.project_id == "level-dragon-333019"
        assert gcp_credentials.client_email == credentials.service_account_email
        assert gcp_credentials.private_key is credentials

    # pass as native value to bare credentials
    gcpc = GcpServiceAccountCredentialsWithoutDefaults()
    gcpc.parse_native_representation(credentials)
    _assert_credentials(gcpc)

    # pass as native value to credentials w/ default
    gcpc = GcpServiceAccountCredentials()
    gcpc.parse_native_representation(credentials)
    _assert_credentials(gcpc)

    # oauth credentials should fail on invalid type
    with pytest.raises(InvalidGoogleNativeCredentialsType):
        gcoauth = GcpOAuthCredentialsWithoutDefaults()
        gcoauth.parse_native_representation(credentials)


def test_oauth_credentials_with_default(environment: Any) -> None:
    from google.oauth2.credentials import Credentials as GoogleOAuth2Credentials

    gcoauth = GcpOAuthCredentials()
    # resolve will miss values and try to find default credentials on the machine
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(gcoauth)
    assert py_ex.value.fields == ["client_id", "client_secret", "refresh_token", "project_id"]

    # prepare real service.json
    oauth_str, _ = prepare_oauth_json()

    # create instance of credentials
    gcoauth = GcpOAuthCredentials()
    gcoauth.parse_native_representation(oauth_str)
    # check if credentials can be created
    assert isinstance(gcoauth.to_native_credentials(), GoogleOAuth2Credentials)

    # reset failed default credentials timeout so we resolve below
    gcp_credentials.GcpDefaultCredentials._LAST_FAILED_DEFAULT = 0

    # now set the env
    _, dest_path = prepare_service_json()
    with custom_environ({"GOOGLE_APPLICATION_CREDENTIALS": dest_path}):
        gcoauth = GcpOAuthCredentials()
        resolve_configuration(gcoauth)
        # project id recovered from credentials
        assert gcoauth.project_id == "level-dragon-333019"
        # check if credentials can be created
        assert gcoauth.to_native_credentials()
        # the default credentials are available
        assert gcoauth.has_default_credentials() is True
        assert gcoauth.default_credentials() is not None


def test_oauth_credentials_native_credentials_object(environment: Any) -> None:
    from google.oauth2.credentials import Credentials as GoogleOAuth2Credentials

    oauth_str, _ = prepare_oauth_json()
    oauth_dict = json.loads(oauth_str)
    # must add refresh_token
    oauth_dict["installed"]["refresh_token"] = "REFRESH TOKEN"
    credentials = GoogleOAuth2Credentials.from_authorized_user_info(oauth_dict["installed"])

    def _assert_credentials(gcp_credentials):
        # check props
        assert gcp_credentials.project_id == ""
        assert gcp_credentials.client_id == credentials.client_id
        assert gcp_credentials.client_secret is credentials.client_secret

    # pass as native value to bare credentials
    gcoauth = GcpOAuthCredentialsWithoutDefaults()
    gcoauth.parse_native_representation(credentials)
    _assert_credentials(gcoauth)

    # check if quota project id is visible
    cred_with_quota = credentials.with_quota_project("the-quota-project")
    gcoauth = GcpOAuthCredentialsWithoutDefaults()
    gcoauth.parse_native_representation(cred_with_quota)
    assert gcoauth.project_id == "the-quota-project"

    # pass as native value to credentials w/ default
    gcoauth = GcpOAuthCredentialsWithoutDefaults()
    gcoauth.parse_native_representation(credentials)
    _assert_credentials(gcoauth)

    # oauth credentials should fail on invalid type
    with pytest.raises(InvalidGoogleNativeCredentialsType):
        gcpc = GcpServiceAccountCredentials()
        gcpc.parse_native_representation(credentials)


def test_get_oauth_access_token() -> None:
    c = resolve_configuration(
        GcpOAuthCredentialsWithoutDefaults(), sections=("destination", "bigquery")
    )
    assert c.refresh_token is not None
    assert c.token is None
    c.auth()
    assert c.token is not None


def test_bigquery_configuration() -> None:
    config = resolve_configuration(
        BigQueryClientConfiguration(dataset_name="dataset"), sections=("destination", "bigquery")
    )
    assert config.location == "US"
    assert config.get_location() == "US"
    assert config.http_timeout == 15.0
    assert config.retry_deadline == 60.0
    assert config.file_upload_timeout == 1800.0
    assert config.fingerprint() == digest128("chat-analytics-rasa-ci")

    # credentials location is deprecated
    os.environ["CREDENTIALS__LOCATION"] = "EU"
    config = resolve_configuration(
        BigQueryClientConfiguration(dataset_name="dataset"), sections=("destination", "bigquery")
    )
    assert config.location == "US"
    assert config.credentials.location == "EU"
    # but if it is set, we propagate it to the config
    assert config.get_location() == "EU"
    os.environ["LOCATION"] = "ATLANTIS"
    config = resolve_configuration(
        BigQueryClientConfiguration(dataset_name="dataset"), sections=("destination", "bigquery")
    )
    assert config.get_location() == "ATLANTIS"
    os.environ["DESTINATION__FILE_UPLOAD_TIMEOUT"] = "20000"
    config = resolve_configuration(
        BigQueryClientConfiguration(dataset_name="dataset"), sections=("destination", "bigquery")
    )
    assert config.file_upload_timeout == 20000.0

    # default fingerprint is empty
    assert BigQueryClientConfiguration(dataset_name="dataset").fingerprint() == ""


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
        client.start_file_load(client.schema.get_table(user_table_name), uniq_id() + ".", uniq_id())

    # start job with invalid name
    dest_path = file_storage.save("!!aaaa", b"data")
    with pytest.raises(LoadJobTerminalException):
        client.start_file_load(client.schema.get_table(user_table_name), dest_path, uniq_id())

    user_table_name = prepare_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
        "timestamp": str(pendulum.now()),
    }
    job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

    # start a job from the same file. it should fallback to retrieve job silently
    r_job = client.start_file_load(
        client.schema.get_table(user_table_name),
        file_storage.make_full_path(job.file_name()),
        uniq_id(),
    )
    assert r_job.state() == "completed"


@pytest.mark.parametrize("location", ["US", "EU"])
def test_bigquery_location(location: str, file_storage: FileStorage) -> None:
    with cm_yield_client_with_storage(
        "bigquery", default_config_values={"credentials": {"location": location}}
    ) as client:
        user_table_name = prepare_table(client)
        load_json = {
            "_dlt_id": uniq_id(),
            "_dlt_root_id": uniq_id(),
            "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
            "timestamp": str(pendulum.now()),
        }
        job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

        # start a job from the same file. it should fallback to retrieve job silently
        client.start_file_load(
            client.schema.get_table(user_table_name),
            file_storage.make_full_path(job.file_name()),
            uniq_id(),
        )
        canonical_name = client.sql_client.make_qualified_table_name(user_table_name, escape=False)
        t = client.sql_client.native_connection.get_table(canonical_name)
        assert t.location == location


def test_loading_errors(client: BigQueryClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)
    # insert into unknown column
    load_json: Dict[str, Any] = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
        "timestamp": str(pendulum.now()),
    }
    insert_json = copy(load_json)
    insert_json["_unk_"] = None
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert "No such field: _unk_" in job.exception()

    # insert null value
    insert_json = copy(load_json)
    insert_json["timestamp"] = None
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert "Only optional fields can be set to NULL. Field: timestamp;" in job.exception()

    # insert wrong type
    insert_json = copy(load_json)
    insert_json["timestamp"] = "AA"
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert "Couldn't convert value to timestamp:" in job.exception()

    # numeric overflow on bigint
    insert_json = copy(load_json)
    # 2**64//2 - 1 is a maximum bigint value
    insert_json["metadata__rasa_x_id"] = 2**64 // 2
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert "Could not convert value" in job.exception()

    # numeric overflow on NUMERIC
    insert_json = copy(load_json)
    # default decimal is (38, 9) (128 bit), use local context to generate decimals with 38 precision
    with numeric_default_context():
        below_limit = Decimal(10**29) - Decimal("0.001")
        above_limit = Decimal(10**29)
    # this will pass
    insert_json["parse_data__intent__id"] = below_limit
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="completed"
    )
    # this will fail
    insert_json["parse_data__intent__id"] = above_limit
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert (
        "Invalid NUMERIC value: 100000000000000000000000000000 Field: parse_data__intent__id;"
        in job.exception()
    )

    # max bigquery decimal is (76, 76) (256 bit) = 5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38
    insert_json = copy(load_json)
    insert_json["parse_data__metadata__rasa_x_id"] = Decimal(
        "5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38"
    )
    job = expect_load_file(
        client, file_storage, json.dumps(insert_json), user_table_name, status="failed"
    )
    assert (
        "Invalid BIGNUMERIC value:"
        " 578960446186580977117854925043439539266.34992332820282019728792003956564819968 Field:"
        " parse_data__metadata__rasa_x_id;"
        in job.exception()
    )


def prepare_oauth_json() -> Tuple[str, str]:
    # prepare real service.json
    storage = FileStorage("_secrets", makedirs=True)
    with open(
        common_json_case_path("oauth_client_secret_929384042504"), mode="r", encoding="utf-8"
    ) as f:
        oauth_str = f.read()
    dest_path = storage.save("oauth_client_secret_929384042504.json", oauth_str)
    return oauth_str, dest_path


def prepare_service_json() -> Tuple[str, str]:
    # prepare real service.json
    storage = FileStorage("_secrets", makedirs=True)
    with open(common_json_case_path("level-dragon-333019-707809ee408a") + ".b64", mode="rb") as f:
        services_str = base64.b64decode(f.read().strip(), validate=True).decode()
    dest_path = storage.save("level-dragon-333019-707809ee408a.json", services_str)
    return services_str, dest_path
