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
from dlt.common.schema.utils import new_table
from dlt.common.storages import FileStorage
from dlt.common.utils import digest128, uniq_id, custom_environ
from dlt.common.destination.reference import RunnableLoadJob
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient, BigQueryClientConfiguration
from dlt.destinations.exceptions import LoadJobNotExistsException, LoadJobTerminalException

from dlt.destinations.impl.bigquery.bigquery_adapter import (
    AUTODETECT_SCHEMA_HINT,
    should_autodetect_schema,
)
from tests.utils import TEST_STORAGE_ROOT, delete_test_storage
from tests.common.utils import json_case_path as common_json_case_path
from tests.common.configuration.utils import environment
from tests.load.utils import (
    expect_load_file,
    prepare_table,
    yield_client_with_storage,
    cm_yield_client_with_storage,
    cm_yield_client,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(scope="module")
def client() -> Iterator[BigQueryClient]:
    yield from cast(Iterator[BigQueryClient], yield_client_with_storage("bigquery"))


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture
def bigquery_project_id() -> Iterator[str]:
    project_id = "different_project_id"
    project_id_key = "DESTINATION__BIGQUERY__PROJECT_ID"
    saved_project_id = os.environ.get(project_id_key)
    os.environ[project_id_key] = project_id
    yield project_id
    del os.environ[project_id_key]
    if saved_project_id:
        os.environ[project_id_key] = saved_project_id


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
        _extracted_from_test_service_credentials_with_default_22()


# TODO Rename this here and in `test_service_credentials_with_default`
def _extracted_from_test_service_credentials_with_default_22():
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

    # oauth credentials should fail on an invalid type
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
        _extracted_from_test_oauth_credentials_with_default_25()


# TODO Rename this here and in `test_oauth_credentials_with_default`
def _extracted_from_test_oauth_credentials_with_default_25():
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

    # oauth credentials should fail on an invalid type
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
        BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "bigquery"),
    )
    assert config.location == "US"
    assert config.get_location() == "US"
    assert config.http_timeout == 15.0
    assert config.retry_deadline == 60.0
    assert config.file_upload_timeout == 1800.0
    assert config.fingerprint() == digest128("chat-analytics-rasa-ci")

    # credential location is deprecated
    # os.environ["CREDENTIALS__LOCATION"] = "EU"
    # config = resolve_configuration(
    #     BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
    #     sections=("destination", "bigquery"),
    # )
    # assert config.location == "US"
    # assert config.credentials.location == "EU"
    # # but if it is set, we propagate it to the config
    # assert config.get_location() == "EU"
    os.environ["LOCATION"] = "ATLANTIS"
    config = resolve_configuration(
        BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "bigquery"),
    )
    assert config.get_location() == "ATLANTIS"
    os.environ["DESTINATION__FILE_UPLOAD_TIMEOUT"] = "20000"
    config = resolve_configuration(
        BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "bigquery"),
    )
    assert config.file_upload_timeout == 20000.0

    # default fingerprint is empty
    assert (
        BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset").fingerprint() == ""
    )


def test_bigquery_different_project_id(bigquery_project_id) -> None:
    """Test scenario when bigquery project_id different from gcp credentials project_id."""
    config = resolve_configuration(
        BigQueryClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "bigquery"),
    )
    assert config.project_id == bigquery_project_id
    with cm_yield_client(
        "bigquery",
        dataset_name="dataset",
        default_config_values={"project_id": bigquery_project_id},
    ) as client:
        assert bigquery_project_id in client.sql_client.catalog_name()


def test_bigquery_autodetect_configuration(client: BigQueryClient) -> None:
    # no schema autodetect
    event_slot = client.prepare_load_table("event_slot")
    _dlt_loads = client.prepare_load_table("_dlt_loads")
    assert should_autodetect_schema(event_slot) is False
    assert should_autodetect_schema(_dlt_loads) is False
    # add parent table
    child = new_table("event_slot__values", "event_slot")
    client.schema.update_table(child, normalize_identifiers=False)
    event_slot__values = client.prepare_load_table("event_slot__values")
    assert should_autodetect_schema(event_slot__values) is False

    # enable global config
    client.config.autodetect_schema = True
    # prepare again
    event_slot = client.prepare_load_table("event_slot")
    _dlt_loads = client.prepare_load_table("_dlt_loads")
    event_slot__values = client.prepare_load_table("event_slot__values")
    assert should_autodetect_schema(event_slot) is True
    assert should_autodetect_schema(_dlt_loads) is False
    assert should_autodetect_schema(event_slot__values) is True

    # enable hint per table
    client.config.autodetect_schema = False
    client.schema.get_table("event_slot")[AUTODETECT_SCHEMA_HINT] = True  # type: ignore[typeddict-unknown-key]
    event_slot = client.prepare_load_table("event_slot")
    _dlt_loads = client.prepare_load_table("_dlt_loads")
    event_slot__values = client.prepare_load_table("event_slot__values")
    assert should_autodetect_schema(event_slot) is True
    assert should_autodetect_schema(_dlt_loads) is False
    assert should_autodetect_schema(event_slot__values) is True


def test_bigquery_job_resuming(client: BigQueryClient, file_storage: FileStorage) -> None:
    user_table_name = prepare_table(client)
    load_json = {
        "_dlt_id": uniq_id(),
        "_dlt_root_id": uniq_id(),
        "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
        "timestamp": str(pendulum.now()),
    }
    job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)
    assert job._created_job  # type: ignore

    # start a job from the same file. it should be a fallback to retrieve a job silently
    r_job = cast(
        RunnableLoadJob,
        client.create_load_job(
            client.prepare_load_table(user_table_name),
            file_storage.make_full_path(job.file_name()),
            uniq_id(),
        ),
    )

    # job will be automatically found and resumed
    r_job.set_run_vars(uniq_id(), client.schema, client.prepare_load_table(user_table_name))
    r_job.run_managed(client)
    assert r_job.state() == "completed"
    assert r_job._resumed_job  # type: ignore


@pytest.mark.parametrize("location", ["US", "EU"])
def test_bigquery_location(location: str, file_storage: FileStorage, client) -> None:
    with cm_yield_client_with_storage(
        "bigquery", default_config_values={"location": location}
    ) as client:
        user_table_name = prepare_table(client)
        load_json = {
            "_dlt_id": uniq_id(),
            "_dlt_root_id": uniq_id(),
            "sender_id": "90238094809sajlkjxoiewjhduuiuehd",
            "timestamp": str(pendulum.now()),
        }
        job = expect_load_file(client, file_storage, json.dumps(load_json), user_table_name)

        # start a job from the same file. it should be a fallback to retrieve a job silently
        client.create_load_job(
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

    # insert a wrong type
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
    expect_load_file(client, file_storage, json.dumps(insert_json), user_table_name)
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
    with open(common_json_case_path("oauth_client_secret_929384042504"), encoding="utf-8") as f:
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
