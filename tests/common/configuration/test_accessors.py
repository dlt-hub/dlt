import datetime  # noqa: 251
from typing import Any
import pytest

import dlt

from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.specs.gcp_client_credentials import GcpClientCredentials
from dlt.common.configuration.specs.postgres_credentials import ConnectionStringCredentials

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment, toml_providers


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
    assert dlt.secrets["value"] == "{SET"

    # get namespaced values
    assert dlt.config["typecheck.str_val"] == "test string"

    environment["DLT__THIS__VALUE"] = "embedded"
    assert dlt.config["dlt.this.value"] == "embedded"
    assert dlt.secrets["dlt.this.value"] == "embedded"


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
    assert dlt.secrets["destination.bigquery"]["client_email"] == "loader@a7513.iam.gserviceaccount.com"
    # equivalent
    assert dlt.secrets["destination.bigquery.client_email"] == "loader@a7513.iam.gserviceaccount.com"


def test_getter_accessor_typed(toml_providers: ConfigProvidersContext, environment: Any) -> None:
    # get a dict as str
    assert dlt.secrets.get("credentials", str) == '{"secret_value":"2137","project_id":"mock-project-id-credentials"}'
    # unchanged type
    assert isinstance(dlt.secrets.get("credentials"), dict)
    # fail on type coercion
    environment["VALUE"] = "a"
    with pytest.raises(ValueError):
        dlt.config.get("value", int)
    # not found -> return none
    assert dlt.config.get("_unk") is None
    # credentials
    c = dlt.secrets.get("databricks.credentials", ConnectionStringCredentials)
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
