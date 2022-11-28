import datetime  # noqa: 251
from typing import Any
import pytest

import dlt
from dlt.common import json

from dlt.common.configuration.providers import EnvironProvider, ConfigTomlProvider, SecretsTomlProvider
from dlt.common.configuration.specs import GcpClientCredentials, ConnectionStringCredentials
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.utils import get_resolved_traces, ResolvedValueTrace
from dlt.common.typing import AnyType, TSecretValue


from tests.utils import preserve_environ
from tests.common.configuration.utils import environment, toml_providers

RESOLVED_TRACES = get_resolved_traces()


def test_accessor_singletons() -> None:
    assert dlt.config.value is None
    assert dlt.secrets.value is None


def test_getter_accessor(toml_providers: ConfigProvidersContext, environment: Any) -> None:
    with pytest.raises(KeyError) as py_ex:
        dlt.config["_unknown"]
    assert py_ex.value.args[0] == "_unknown"

    with pytest.raises(KeyError) as py_ex:
        dlt.secrets["_unknown"]
    assert py_ex.value.args[0] == "_unknown"

    environment["VALUE"] = "{SET"
    assert dlt.config["value"] == "{SET"
    assert RESOLVED_TRACES[".value"] == ResolvedValueTrace("value", "{SET", None, AnyType, [], EnvironProvider().name, None)
    assert dlt.secrets["value"] == "{SET"
    assert RESOLVED_TRACES[".value"] == ResolvedValueTrace("value", "{SET", None, TSecretValue, [], EnvironProvider().name, None)

    # get namespaced values
    assert dlt.config["typecheck.str_val"] == "test string"
    assert RESOLVED_TRACES["typecheck.str_val"] == ResolvedValueTrace("str_val", "test string", None, AnyType, ["typecheck"], ConfigTomlProvider().name, None)

    environment["DLT__THIS__VALUE"] = "embedded"
    assert dlt.config["dlt.this.value"] == "embedded"
    assert RESOLVED_TRACES["dlt.this.value"] == ResolvedValueTrace("value", "embedded", None, AnyType, ["dlt", "this"], EnvironProvider().name, None)
    assert dlt.secrets["dlt.this.value"] == "embedded"
    assert RESOLVED_TRACES["dlt.this.value"] == ResolvedValueTrace("value", "embedded", None, TSecretValue, ["dlt", "this"], EnvironProvider().name, None)


def test_getter_auto_cast(toml_providers: ConfigProvidersContext, environment: Any) -> None:
    environment["VALUE"] = "{SET}"
    assert dlt.config["value"] == "{SET}"
    # bool
    environment["VALUE"] = "true"
    assert dlt.config["value"] is True
    environment["VALUE"] = "False"
    assert dlt.config["value"] is False
    environment["VALUE"] = "yes"
    assert dlt.config["value"] == "yes"
    # int
    environment["VALUE"] = "17261"
    assert dlt.config["value"] == 17261
    environment["VALUE"] = "-17261"
    assert dlt.config["value"] == -17261
    # float
    environment["VALUE"] = "17261.4"
    assert dlt.config["value"] == 17261.4
    environment["VALUE"] = "-10e45"
    assert dlt.config["value"] == -10e45
    # list
    environment["VALUE"] = "[1,2,3]"
    assert dlt.config["value"] == [1, 2, 3]
    assert dlt.config["value"][2] == 3
    # dict
    environment["VALUE"] = '{"a": 1}'
    assert dlt.config["value"] == {"a": 1}
    assert dlt.config["value"]["a"] == 1
    # if not dict or list then original string must be returned, null is a JSON -> None
    environment["VALUE"] = 'null'
    assert dlt.config["value"] == "null"

    # typed values are returned as they are
    assert isinstance(dlt.config["typecheck.date_val"], datetime.datetime)

    # access dict from toml
    services_json_dict = dlt.secrets["destination.bigquery"]
    assert dlt.secrets["destination.bigquery"]["client_email"] == "loader@a7513.iam.gserviceaccount.com"
    assert RESOLVED_TRACES["destination.bigquery"] == ResolvedValueTrace("bigquery", services_json_dict, None, TSecretValue, ["destination"], SecretsTomlProvider().name, None)
    # equivalent
    assert dlt.secrets["destination.bigquery.client_email"] == "loader@a7513.iam.gserviceaccount.com"
    assert RESOLVED_TRACES["destination.bigquery.client_email"] == ResolvedValueTrace("client_email", "loader@a7513.iam.gserviceaccount.com", None, TSecretValue, ["destination", "bigquery"], SecretsTomlProvider().name, None)


def test_getter_accessor_typed(toml_providers: ConfigProvidersContext, environment: Any) -> None:
    # get a dict as str
    credentials_str = '{"secret_value":"2137","project_id":"mock-project-id-credentials"}'
    # the typed version coerces the value into desired type, in this case "dict" -> "str"
    assert dlt.secrets.get("credentials", str) == credentials_str
    # note that trace keeps original value of "credentials" which was of dictionary type
    assert RESOLVED_TRACES[".credentials"] == ResolvedValueTrace("credentials", json.loads(credentials_str), None, str, [], SecretsTomlProvider().name, None)
    # unchanged type
    assert isinstance(dlt.secrets.get("credentials"), dict)
    # fail on type coercion
    environment["VALUE"] = "a"
    with pytest.raises(ValueError):
        dlt.config.get("value", int)
    # not found -> return none
    assert dlt.config.get("_unk") is None
    # credentials string will be parsed using specified type
    credentials_str = "databricks+connector://token:<databricks_token>@<databricks_host>:443/<database_or_schema_name>?conn_timeout=15&search_path=a,b,c"
    c = dlt.secrets.get("databricks.credentials", ConnectionStringCredentials)
    # as before: the value in trace is the value coming from the provider (as is)
    assert RESOLVED_TRACES["databricks.credentials"] == ResolvedValueTrace("credentials", credentials_str, None, ConnectionStringCredentials, ["databricks"], SecretsTomlProvider().name, None)
    assert c.drivername == "databricks+connector"
    c = dlt.secrets.get("destination.credentials", GcpClientCredentials)
    assert c.client_email == "loader@a7513.iam.gserviceaccount.com"


def test_secrets_separation(toml_providers: ConfigProvidersContext) -> None:
    # secrets are available both in config and secrets
    assert dlt.config.get("credentials") is not None
    assert dlt.secrets.get("credentials") is not None

    # configs are not available in secrets
    assert dlt.config.get("api_type") is not None
    assert dlt.secrets.get("api_type") is None


def test_access_access_injection(toml_providers: ConfigProvidersContext) -> None:

    @dlt.source
    def the_source(api_type=dlt.config.value, credentials: GcpClientCredentials=dlt.secrets.value, databricks_creds: ConnectionStringCredentials=dlt.secrets.value):
        assert api_type == "REST"
        assert credentials.client_email == "loader@a7513.iam.gserviceaccount.com"
        assert databricks_creds.drivername == "databricks+connector"
        return dlt.resource([1,2,3], name="data")

    # inject first argument, the rest pass explicitly
    the_source(credentials=dlt.secrets["destination.credentials"], databricks_creds=dlt.secrets["databricks.credentials"])
