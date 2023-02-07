import os
import base64
from typing import Any

import pytest
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import PostgresCredentials, RedshiftCredentials, ConnectionStringCredentials, GcpClientCredentials, GcpClientCredentialsWithDefault
from dlt.common.configuration.specs.exceptions import InvalidConnectionString, InvalidServicesJson
from dlt.common.configuration.specs.run_configuration import RunConfiguration

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
    assert csc.query == {"a": "b", "c": "d"}

    # test postgres timeout
    dsn = "postgres://loader:pass@localhost:5432/dlt_data?connect_timeout=600"
    csc = PostgresCredentials()
    csc.parse_native_representation(dsn)
    assert csc.connect_timeout == 600
    assert csc.to_native_representation() == dsn

    # test connection string without query, database and port
    csc = ConnectionStringCredentials()
    csc.parse_native_representation("postgres://")
    assert csc.username is csc.password is csc.host is csc.port is csc.database is None
    assert csc.query == {}
    assert csc.to_native_representation() == "postgres://"

    # what id query is none
    csc.query = None
    assert csc.to_native_representation() == "postgres://"


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


def test_postgres_and_redshift_credentials_defaults() -> None:
    pg_cred = PostgresCredentials()
    assert pg_cred.port == 5432
    assert pg_cred.connect_timeout == 15
    assert PostgresCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(pg_cred, explicit_value="postgres://loader:loader@localhost/dlt_data")
    assert pg_cred.port == 5432

    red_cred = RedshiftCredentials()
    assert red_cred.port == 5439
    assert red_cred.connect_timeout == 15
    assert RedshiftCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    resolve_configuration(red_cred, explicit_value="postgres://loader:loader@localhost/dlt_data")
    assert red_cred.port == 5439


def test_gcp_credentials_native_representation(environment) -> None:
    with pytest.raises(InvalidServicesJson):
        GcpClientCredentialsWithDefault().parse_native_representation(1)

    with pytest.raises(InvalidServicesJson):
        GcpClientCredentialsWithDefault().parse_native_representation("notjson")

    assert GcpClientCredentialsWithDefault.__config_gen_annotations__ == ["location"]

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
    # default credentials are not available
    assert gcpc.has_default_credentials() is False
    assert gcpc_2.has_default_credentials() is False
    assert gcpc.default_credentials() is None
    assert gcpc_2.default_credentials() is None


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


def test_run_configuration_slack_credentials(environment: Any) -> None:
    hook = "https://im.slack.com/hook"
    environment["SLACK_INCOMING_HOOK"] = hook

    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == hook

    # and obfuscated
    environment["SLACK_INCOMING_HOOK"] = "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQEbAg=="
    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == hook

    # and obfuscated-like but really not
    environment["SLACK_INCOMING_HOOK"] = "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQ-EbAg=="
    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQ-EbAg=="
