import pytest
from typing import Any
import datetime  # noqa: I251

import dlt
from dlt.common import pendulum
from dlt.common.configuration import configspec, ConfigFieldMissingException, resolve
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers.toml import ConfigTomlProvider, TomlProviderReadException
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.specs import BaseConfiguration, GcpClientCredentials, PostgresCredentials, ConnectionStringCredentials
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import WithCredentialsConfiguration, CoercionTestConfiguration, COERCIONS, SecretConfiguration, environment, toml_providers


@configspec
class EmbeddedWithGcpStorage(BaseConfiguration):
    gcp_storage: GcpClientCredentials


@configspec
class EmbeddedWithGcpCredentials(BaseConfiguration):
    credentials: GcpClientCredentials


def test_secrets_from_toml_secrets() -> None:
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(SecretConfiguration())

    # only two traces because TSecretValue won't be checked in config.toml provider
    traces = py_ex.value.traces["secret_value"]
    assert len(traces) == 2
    assert traces[0] == LookupTrace("Environment Variables", [], "SECRET_VALUE", None)
    assert traces[1] == LookupTrace("secrets.toml", [], "secret_value", None)

    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(WithCredentialsConfiguration())


def test_toml_types(toml_providers: ConfigProvidersContext) -> None:
    # resolve CoercionTestConfiguration from typecheck section
    c = resolve.resolve_configuration(CoercionTestConfiguration(), sections=("typecheck",))
    for k, v in COERCIONS.items():
        # toml does not know tuples
        if isinstance(v, tuple):
            v = list(v)
        if isinstance(v, datetime.datetime):
            v = pendulum.parse("1979-05-27T07:32:00-08:00")
        assert v == c[k]


def test_config_provider_order(toml_providers: ConfigProvidersContext, environment: Any) -> None:

    # add env provider


    @with_config(sections=("api",))
    def single_val(port=None):
        return port

    # secrets have api.port=1023 and this will be used
    assert single_val(None) == 1023

    # env will make it string, also section is optional
    environment["PORT"] = "UNKNOWN"
    assert single_val() == "UNKNOWN"

    environment["API__PORT"] = "1025"
    assert single_val() == "1025"


def test_toml_mixed_config_inject(toml_providers: ConfigProvidersContext) -> None:
    # get data from both providers

    @with_config
    def mixed_val(api_type=dlt.config.value, secret_value: TSecretValue = dlt.secrets.value, typecheck: Any = dlt.config.value):
        return api_type, secret_value, typecheck

    _tup = mixed_val(None, None, None)
    assert _tup[0] == "REST"
    assert _tup[1] == "2137"
    assert isinstance(_tup[2], dict)

    _tup = mixed_val()
    assert _tup[0] == "REST"
    assert _tup[1] == "2137"
    assert isinstance(_tup[2], dict)


def test_toml_sections(toml_providers: ConfigProvidersContext) -> None:
    cfg = toml_providers["config.toml"]
    assert cfg.get_value("api_type", str) == ("REST", "api_type")
    assert cfg.get_value("port", int, "api") == (1024, "api.port")
    assert cfg.get_value("param1", str, "api", "params") == ("a", "api.params.param1")


def test_secrets_toml_credentials(environment: Any, toml_providers: ConfigProvidersContext) -> None:
    # there are credentials exactly under destination.bigquery.credentials
    c = resolve.resolve_configuration(GcpClientCredentials(), sections=("destination", "bigquery"))
    assert c.project_id.endswith("destination.bigquery.credentials")
    # there are no destination.gcp_storage.credentials so it will fallback to "destination"."credentials"
    c = resolve.resolve_configuration(GcpClientCredentials(), sections=("destination", "gcp_storage"))
    assert c.project_id.endswith("destination.credentials")
    # also explicit
    c = resolve.resolve_configuration(GcpClientCredentials(), sections=("destination",))
    assert c.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        print(dict(resolve.resolve_configuration(GcpClientCredentials())))
    # also try postgres credentials
    c = resolve.resolve_configuration(PostgresCredentials(), sections=("destination", "redshift"))
    assert c.database == "destination.redshift.credentials"
    # bigquery credentials do not match redshift credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(PostgresCredentials(), sections=("destination", "bigquery"))


def test_secrets_toml_embedded_credentials(environment: Any, toml_providers: ConfigProvidersContext) -> None:
    # will try destination.bigquery.credentials
    c = resolve.resolve_configuration(EmbeddedWithGcpCredentials(), sections=("destination", "bigquery"))
    assert c.credentials.project_id.endswith("destination.bigquery.credentials")
    # will try destination.gcp_storage.credentials and fallback to destination.credentials
    c = resolve.resolve_configuration(EmbeddedWithGcpCredentials(), sections=("destination", "gcp_storage"))
    assert c.credentials.project_id.endswith("destination.credentials")
    # will try everything until credentials in the root where incomplete credentials are present
    c = EmbeddedWithGcpCredentials()
    # create embedded config that will be passed as initial
    c.credentials = GcpClientCredentials()
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(c, sections=("middleware", "storage"))
    # so we can read partially filled configuration here
    assert c.credentials.project_id.endswith("-credentials")
    assert set(py_ex.value.traces.keys()) == {"client_email", "private_key"}

    # embed "gcp_storage" will bubble up to the very top, never reverts to "credentials"
    c = resolve.resolve_configuration(EmbeddedWithGcpStorage(), sections=("destination", "bigquery"))
    assert c.gcp_storage.project_id.endswith("-gcp-storage")

    # also explicit
    c = resolve.resolve_configuration(GcpClientCredentials(), sections=("destination",))
    assert c.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(GcpClientCredentials())


def test_dicts_are_not_enumerated() -> None:
    # dicts returned by toml provider cannot be used as explicit values or initial values for the whole configurations
    pass


def test_secrets_toml_credentials_from_native_repr(environment: Any, toml_providers: ConfigProvidersContext) -> None:
    # cfg = toml_providers["secrets.toml"]
    # print(cfg._toml)
    # print(cfg._toml["source"]["credentials"])
    # resolve gcp_credentials by parsing initial value which is str holding json doc
    c = resolve.resolve_configuration(GcpClientCredentials(), sections=("source",))
    assert c.project_id.endswith("source.credentials")
    # also try sql alchemy url (native repr)
    c = resolve.resolve_configuration(ConnectionStringCredentials(), sections=("databricks",))
    assert c.drivername == "databricks+connector"
    assert c.username == "token"
    assert c.password == "<databricks_token>"
    assert c.host == "<databricks_host>"
    assert c.port == 443
    assert c.database == "<database_or_schema_name>"
    assert c.query == {"conn_timeout": "15", "search_path": "a,b,c"}


def test_toml_get_key_as_section(toml_providers: ConfigProvidersContext) -> None:
    cfg = toml_providers["secrets.toml"]
    # [credentials]
    # secret_value="2137"
    # so the line below will try to use secrets_value value as section, this must fallback to not found
    cfg.get_value("value", str, "credentials", "secret_value")


def test_toml_read_exception() -> None:
    pipeline_root = "./tests/common/cases/configuration/.wrong.dlt"
    with pytest.raises(TomlProviderReadException) as py_ex:
        ConfigTomlProvider(project_dir=pipeline_root)
    assert py_ex.value.file_name == "config.toml"
