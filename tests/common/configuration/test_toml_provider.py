import pytest
from typing import Any, Iterator
import datetime  # noqa: I251


from dlt.common import pendulum
from dlt.common.configuration import configspec, ConfigFieldMissingException, ConfigFileNotFoundException, resolve
from dlt.common.configuration.container import Container
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers.environ import EnvironProvider
from dlt.common.configuration.providers.toml import SecretsTomlProvider, ConfigTomlProvider
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.specs import BaseConfiguration, GcpClientCredentials, PostgresCredentials
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import WithCredentialsConfiguration, CoercionTestConfiguration, COERCIONS, SecretConfiguration, environment


@configspec
class EmbeddedWithGcpStorage(BaseConfiguration):
    gcp_storage: GcpClientCredentials


@configspec
class EmbeddedWithGcpCredentials(BaseConfiguration):
    credentials: GcpClientCredentials


@pytest.fixture
def providers() -> Iterator[ConfigProvidersContext]:
    pipeline_root = "./tests/common/cases/configuration/.dlt"
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(SecretsTomlProvider(project_dir=pipeline_root))
    ctx.add_provider(ConfigTomlProvider(project_dir=pipeline_root))
    with Container().injectable_context(ctx):
        yield ctx


def test_secrets_from_toml_secrets() -> None:
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(SecretConfiguration())

    # only two traces because TSecretValue won't be checked in config.toml provider
    traces = py_ex.value.traces["secret_value"]
    assert len(traces) == 2
    assert traces[0] == LookupTrace("Environment Variables", [], "SECRET_VALUE", None)
    assert traces[1] == LookupTrace("Pipeline secrets.toml", [], "secret_value", None)

    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(WithCredentialsConfiguration())


def test_toml_types(providers: ConfigProvidersContext) -> None:
    # resolve CoercionTestConfiguration from typecheck namespace
    c = resolve.resolve_configuration(CoercionTestConfiguration(), namespaces=("typecheck",))
    for k, v in COERCIONS.items():
        # toml does not know tuples
        if isinstance(v, tuple):
            v = list(v)
        if isinstance(v, datetime.datetime):
            v = pendulum.parse("1979-05-27T07:32:00-08:00")
        assert v == c[k]


def test_config_provider_order(providers: ConfigProvidersContext, environment: Any) -> None:

    # add env provider
    providers.providers.insert(0, EnvironProvider())

    @with_config(namespaces=("api",))
    def single_val(port):
        return port

    # secrets have api.port=1023 and this will be used
    assert single_val() == 1023

    # env will make it string, also namespace is optional
    environment["PORT"] = "UNKNOWN"
    assert single_val() == "UNKNOWN"

    environment["API__PORT"] = "1025"
    assert single_val() == "1025"


def test_toml_mixed_config_inject(providers: ConfigProvidersContext) -> None:
    # get data from both providers

    @with_config
    def mixed_val(api_type, secret_value: TSecretValue, typecheck: Any):
        return api_type, secret_value, typecheck

    _tup = mixed_val()
    assert _tup[0] == "REST"
    assert _tup[1] == "2137"
    assert isinstance(_tup[2], dict)


def test_toml_namespaces(providers: ConfigProvidersContext) -> None:
    cfg = providers["Pipeline config.toml"]
    assert cfg.get_value("api_type", str) == ("REST", "api_type")
    assert cfg.get_value("port", int, "api") == (1024, "api.port")
    assert cfg.get_value("param1", str, "api", "params") == ("a", "api.params.param1")


def test_secrets_toml_credentials(providers: ConfigProvidersContext) -> None:
    # there are credentials exactly under destination.bigquery.credentials
    c = resolve.resolve_configuration(GcpClientCredentials(), namespaces=("destination", "bigquery"))
    assert c.project_id.endswith("destination.bigquery.credentials")
    # there are no destination.gcp_storage.credentials so it will fallback to "destination"."credentials"
    c = resolve.resolve_configuration(GcpClientCredentials(), namespaces=("destination", "gcp_storage"))
    assert c.project_id.endswith("destination.credentials")
    # also explicit
    c = resolve.resolve_configuration(GcpClientCredentials(), namespaces=("destination",))
    assert c.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(GcpClientCredentials())
    # also try postgres credentials
    c = resolve.resolve_configuration(PostgresCredentials(), namespaces=("destination", "redshift"))
    assert c.database == "destination.redshift.credentials"
    # bigquery credentials do not match redshift credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(PostgresCredentials(), namespaces=("destination", "bigquery"))



def test_secrets_toml_embedded_credentials(providers: ConfigProvidersContext) -> None:
    # will try destination.bigquery.credentials
    c = resolve.resolve_configuration(EmbeddedWithGcpCredentials(), namespaces=("destination", "bigquery"))
    assert c.credentials.project_id.endswith("destination.bigquery.credentials")
    # will try destination.gcp_storage.credentials and fallback to destination.credentials
    c = resolve.resolve_configuration(EmbeddedWithGcpCredentials(), namespaces=("destination", "gcp_storage"))
    assert c.credentials.project_id.endswith("destination.credentials")
    # will try everything until credentials in the root where incomplete credentials are present
    c = EmbeddedWithGcpCredentials()
    # create embedded config that will be passed as initial
    c.credentials = GcpClientCredentials()
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(c, namespaces=("middleware", "storage"))
    # so we can read partially filled configuration here
    assert c.credentials.project_id.endswith("-credentials")
    assert set(py_ex.value.traces.keys()) == {"client_email", "private_key"}

    # embed "gcp_storage" will bubble up to the very top, never reverts to "credentials"
    c = resolve.resolve_configuration(EmbeddedWithGcpStorage(), namespaces=("destination", "bigquery"))
    assert c.gcp_storage.project_id.endswith("-gcp-storage")

    # also explicit
    c = resolve.resolve_configuration(GcpClientCredentials(), namespaces=("destination",))
    assert c.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(GcpClientCredentials())


# def test_secrets_toml_ignore_dict_initial(providers: ConfigProvidersContext) -> None:



def test_secrets_toml_credentials_from_native_repr(providers: ConfigProvidersContext) -> None:
    # cfg = providers["Pipeline secrets.toml"]
    # print(cfg._toml)
    # print(cfg._toml["source"]["credentials"])
    # resolve gcp_credentials by parsing initial value which is str holding json doc
    c = resolve.resolve_configuration(GcpClientCredentials(), namespaces=("source",))
    assert c.project_id.endswith("source.credentials")
