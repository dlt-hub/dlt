import os
import base64
from typing import Any

import pytest
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs import PostgresCredentials, ConnectionStringCredentials, GcpClientCredentials, GcpClientCredentialsWithDefault
from dlt.common.configuration.specs.exceptions import InvalidConnectionString, InvalidServicesJson
from dlt.common.storages import FileStorage

from tests.utils import preserve_environ
from tests.common.utils import json_case_path
from tests.common.configuration.utils import environment


SERVICE_JSON = """
    {
    "type": "service_account",
    "project_id": "chat-analytics",
    "private_key_id": "921837921798379812",
    %s
    "client_email": "loader@iam.gserviceaccount.com",
    "client_id": "839283982193892138",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/loader40chat-analytics-317513.iam.gserviceaccount.com"
  }
    """


def test_connection_string_credentials_native_representation(environment) -> None:
    with pytest.raises(InvalidConnectionString):
        ConnectionStringCredentials().parse_native_representation(1)

    with pytest.raises(InvalidConnectionString):
        ConnectionStringCredentials().parse_native_representation("loader@localhost:5432/dlt_data")

    dsn = "postgres://loader:pass@localhost:5432/dlt_data?a=b&c=d"
    csc = ConnectionStringCredentials()
    csc.parse_native_representation(dsn)
    assert csc.to_native_representation() == dsn

    assert csc.drivername == "postgres"
    assert csc.username == "loader"
    assert csc.password == "pass"
    assert csc.host == "localhost"
    assert csc.port == 5432
    assert csc.database == "dlt_data"

    # test postgres timeout
    dsn = "postgres://loader:pass@localhost:5432/dlt_data?connect_timeout=600"
    csc = PostgresCredentials()
    csc.parse_native_representation(dsn)
    assert csc.connect_timeout == 600
    assert csc.to_native_representation() == dsn


def test_connection_string_resolved_from_native_representation(environment: Any) -> None:
    # sometimes it is sometimes not try URL without password
    destination_dsn = "postgres://loader@localhost:5432/dlt_data"
    c = PostgresCredentials()
    c.parse_native_representation(destination_dsn)
    assert c.is_partial()
    assert not c.is_resolved()

    resolve_configuration(c, accept_partial=True)
    assert c.is_partial()

    environment["CREDENTIALS__PASSWORD"] = "loader"
    resolve_configuration(c, accept_partial=False)


def test_gcp_credentials_native_representation(environment) -> None:
    with pytest.raises(InvalidServicesJson):
        GcpClientCredentialsWithDefault().parse_native_representation(1)

    with pytest.raises(InvalidServicesJson):
        GcpClientCredentialsWithDefault().parse_native_representation("notjson")


    gcpc = GcpClientCredentialsWithDefault()
    gcpc.parse_native_representation(SERVICE_JSON % '"private_key": "-----BEGIN PRIVATE KEY-----\\n\\n-----END PRIVATE KEY-----\\n",')
    assert gcpc.private_key == "-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n"
    assert gcpc.project_id == "chat-analytics"
    assert gcpc.client_email == "loader@iam.gserviceaccount.com"
    # get native representation, it will also include timeouts
    _repr = gcpc.to_native_representation()
    assert "retry_deadline" in _repr
    assert "location" in _repr
    # parse again
    gcpc_2 = GcpClientCredentialsWithDefault()
    gcpc_2.parse_native_representation(_repr)
    assert dict(gcpc_2) == dict(gcpc)


def test_gcp_credentials_resolved_from_native_representation(environment: Any) -> None:
    gcpc = GcpClientCredentials()
    # without password
    gcpc.parse_native_representation(SERVICE_JSON % "")
    assert gcpc.is_partial()
    assert not gcpc.is_resolved()

    resolve_configuration(gcpc, accept_partial=True)
    assert gcpc.is_partial()

    environment["CREDENTIALS__PRIVATE_KEY"] = "loader"
    resolve_configuration(gcpc, accept_partial=False)


def test_gcp_credentials_with_default(environment: Any) -> None:
    gcpc = GcpClientCredentialsWithDefault()
    # resolve will miss values and try to find default credentials on the machine
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(gcpc)
    assert py_ex.value.fields == ['project_id', 'private_key', 'client_email']

    # prepare real service.json
    storage = FileStorage("_secrets", makedirs=True)
    with open(json_case_path("level-dragon-333019-707809ee408a") + ".b64", mode="br") as f:
        services_str = base64.b64decode(f.read().strip(), validate=True).decode()
    storage.save("level-dragon-333019-707809ee408a.json", services_str)

    # now set the env
    environment["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("_secrets/level-dragon-333019-707809ee408a.json")
    resolve_configuration(gcpc)
    # project id recovered from credentials
    assert gcpc.project_id == "level-dragon-333019"