import os
import base64
from typing import Any

import pytest
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs import PostgresCredentials, RedshiftCredentials, ConnectionStringCredentials, GcpClientCredentials, GcpClientCredentialsWithDefault, GcpOAuthCredentials, GcpOAuthCredentialsWithDefault
from dlt.common.configuration.specs.exceptions import InvalidConnectionString, InvalidGoogleNativeCredentialsType, InvalidGoogleOauth2Json, InvalidGoogleServicesJson, OAuth2ScopesRequired
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

OAUTH_USER_INFO = """
    {
        "client_id": "921382012504-3mtjaj1s7vuvf53j88mgdq4te7akkjm3.apps.googleusercontent.com",
        "project_id": "level-dragon-333983",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "gOCSPX-XdY5znbrvjSMEG3pkpA_GHuLPPth",
        "scopes": ["email", "service"],
        %s
        "redirect_uris": [
            "http://localhost"
        ]
    }
"""

OAUTH_APP_USER_INFO = """
{
    "installed": %s
}
""" % OAUTH_USER_INFO


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

    # letter case
    dsn = "postgres://loadeR:pAss@add.A.inter:5432/dlt_data/BASE/q?a=B&c=d"
    csc = ConnectionStringCredentials()
    csc.parse_native_representation(dsn)
    assert csc.to_native_representation() == dsn
    assert csc.database == "dlt_data/BASE/q"
    assert csc.query == {"a": "B", "c": "d"}


def test_connection_string_letter_case(environment: Any) -> None:
    dsn = "postgres://loadeR:pAss@add.A.inter:5432/dlt_data/BASE/q?a=B&c=d"
    os.environ["CREDENTIALS"] = dsn
    csc = resolve_configuration(ConnectionStringCredentials())
    assert csc.to_native_representation() == dsn


def test_connection_string_resolved_from_native_representation(environment: Any) -> None:
    destination_dsn = "mysql+pymsql://localhost:5432/dlt_data"
    c = ConnectionStringCredentials()
    c.parse_native_representation(destination_dsn)
    assert c.is_partial()
    assert not c.is_resolved()
    assert c.username is None
    assert c.password is None

    resolve_configuration(c, accept_partial=True)
    assert c.is_partial()

    environment["CREDENTIALS__USERNAME"] = "loader"
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


def test_gcp_service_credentials_native_representation(environment) -> None:
    with pytest.raises(InvalidGoogleNativeCredentialsType):
        GcpClientCredentialsWithDefault().parse_native_representation(1)

    with pytest.raises(InvalidGoogleServicesJson):
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


def test_gcp_service_credentials_resolved_from_native_representation(environment: Any) -> None:
    gcpc = GcpClientCredentials()

    # without PK
    gcpc.parse_native_representation(SERVICE_JSON % "")
    assert gcpc.is_partial()
    assert not gcpc.is_resolved()

    resolve_configuration(gcpc, accept_partial=True)
    assert gcpc.is_partial()

    environment["CREDENTIALS__PRIVATE_KEY"] = "loader"
    resolve_configuration(gcpc, accept_partial=False)


def test_gcp_oauth_credentials_native_representation(environment) -> None:

    with pytest.raises(InvalidGoogleNativeCredentialsType):
        GcpOAuthCredentialsWithDefault().parse_native_representation(1)

    with pytest.raises(InvalidGoogleOauth2Json):
        GcpOAuthCredentialsWithDefault().parse_native_representation("notjson")

    gcoauth = GcpOAuthCredentialsWithDefault()
    gcoauth.parse_native_representation(OAUTH_APP_USER_INFO % '"refresh_token": "refresh_token",')
    # is not resolved, we resolve only when default credentials are present
    assert gcoauth.is_resolved() is False
    # but is not partial - all required fields are present
    assert gcoauth.is_partial() is False
    assert gcoauth.project_id == "level-dragon-333983"
    assert gcoauth.client_id == "921382012504-3mtjaj1s7vuvf53j88mgdq4te7akkjm3.apps.googleusercontent.com"
    assert gcoauth.client_secret == "gOCSPX-XdY5znbrvjSMEG3pkpA_GHuLPPth"
    assert gcoauth.refresh_token == "refresh_token"
    assert gcoauth.token is None
    assert gcoauth.scopes == ["email", "service"]


    # get native representation, it will also include timeouts
    _repr = gcoauth.to_native_representation()
    assert "retry_deadline" in _repr
    assert "location" in _repr
    # parse again
    gcpc_2 = GcpOAuthCredentialsWithDefault()
    gcpc_2.parse_native_representation(_repr)
    assert dict(gcpc_2) == dict(gcoauth)
    # default credentials are not available
    assert gcoauth.has_default_credentials() is False
    assert gcpc_2.has_default_credentials() is False
    assert gcoauth.default_credentials() is None
    assert gcpc_2.default_credentials() is None

    # use OAUTH_USER_INFO without "installed"
    gcpc_3 = GcpOAuthCredentialsWithDefault()
    gcpc_3.parse_native_representation(OAUTH_USER_INFO % '"refresh_token": "refresh_token",')
    assert dict(gcpc_3) == dict(gcpc_2)


def test_gcp_oauth_credentials_resolved_from_native_representation(environment: Any) -> None:
    gcpc = GcpOAuthCredentials()

    # without refresh token
    gcpc.parse_native_representation(OAUTH_USER_INFO % "")
    assert gcpc.is_partial()
    assert not gcpc.is_resolved()

    resolve_configuration(gcpc, accept_partial=True)
    assert gcpc.is_partial()

    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(gcpc, accept_partial=False)

    environment["CREDENTIALS__REFRESH_TOKEN"] = "refresh_token"
    resolve_configuration(gcpc, accept_partial=False)


def test_needs_scopes_for_refresh_token() -> None:
    c = GcpOAuthCredentials()
    # without refresh token
    c.parse_native_representation(OAUTH_USER_INFO % "")
    assert c.refresh_token is None
    assert c.token is None
    c.scopes = []
    with pytest.raises(OAuth2ScopesRequired):
        c.auth()


def test_requires_refresh_token_no_tty():
    c = GcpOAuthCredentials()
    # without refresh token
    c.parse_native_representation(OAUTH_USER_INFO % "")
    assert c.refresh_token is None
    assert c.token is None
    with pytest.raises(AssertionError):
        c.auth()


def test_run_configuration_slack_credentials(environment: Any) -> None:
    hook = "https://im.slack.com/hook"
    environment["RUNTIME__SLACK_INCOMING_HOOK"] = hook

    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == hook

    # and obfuscated
    environment["RUNTIME__SLACK_INCOMING_HOOK"] = "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQEbAg=="
    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == hook

    # and obfuscated-like but really not
    environment["RUNTIME__SLACK_INCOMING_HOOK"] = "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQ-EbAg=="
    c = resolve_configuration(RunConfiguration())
    assert c.slack_incoming_hook == "DBgAXQFPQVsAAEteXlFRWUoPG0BdHQ-EbAg=="
