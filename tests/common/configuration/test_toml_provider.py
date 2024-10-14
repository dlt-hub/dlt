import os
import pytest
import yaml
from typing import Any, Dict, Type
import datetime  # noqa: I251

import dlt
from dlt.common import pendulum, json
from dlt.common.configuration import configspec, ConfigFieldMissingException, resolve
from dlt.common.configuration.container import Container
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.known_env import DLT_DATA_DIR, DLT_PROJECT_DIR
from dlt.common.configuration.providers.toml import (
    SECRETS_TOML,
    CONFIG_TOML,
    BaseDocProvider,
    CustomLoaderDocProvider,
    SettingsTomlProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
    StringTomlProvider,
    TomlProviderReadException,
)
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.specs import (
    BaseConfiguration,
    GcpServiceAccountCredentialsWithoutDefaults,
    ConnectionStringCredentials,
)
from dlt.common.runners.configuration import PoolRunnerConfiguration
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ
from tests.common.configuration.utils import (
    ConnectionStringCompatCredentials,
    SecretCredentials,
    WithCredentialsConfiguration,
    CoercionTestConfiguration,
    COERCIONS,
    SecretConfiguration,
    environment,
    toml_providers,
)


@configspec
class EmbeddedWithGcpStorage(BaseConfiguration):
    gcp_storage: GcpServiceAccountCredentialsWithoutDefaults = None


@configspec
class EmbeddedWithGcpCredentials(BaseConfiguration):
    credentials: GcpServiceAccountCredentialsWithoutDefaults = None


def test_secrets_from_toml_secrets(toml_providers: ConfigProvidersContainer) -> None:
    # remove secret_value to trigger exception

    del toml_providers["secrets.toml"]._config_doc["secret_value"]  # type: ignore[attr-defined]
    del toml_providers["secrets.toml"]._config_doc["credentials"]  # type: ignore[attr-defined]

    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(SecretConfiguration())

    # only two traces because TSecretValue won't be checked in config.toml provider
    traces = py_ex.value.traces["secret_value"]
    assert len(traces) == 2
    assert traces[0] == LookupTrace("Environment Variables", [], "SECRET_VALUE", None)
    assert traces[1] == LookupTrace("secrets.toml", [], "secret_value", None)

    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(WithCredentialsConfiguration())


def test_toml_types(toml_providers: ConfigProvidersContainer) -> None:
    # resolve CoercionTestConfiguration from typecheck section
    c = resolve.resolve_configuration(CoercionTestConfiguration(), sections=("typecheck",))
    for k, v in COERCIONS.items():
        # toml does not know tuples
        if isinstance(v, tuple):
            v = list(v)
        if isinstance(v, datetime.datetime):
            v = pendulum.parse("1979-05-27T07:32:00-08:00")
        assert v == c[k]


def test_config_provider_order(toml_providers: ConfigProvidersContainer, environment: Any) -> None:
    # add env provider

    @with_config(sections=("api",))
    def single_val(port=None):
        return port

    # secrets have api.port=1023 and this will be used
    assert single_val(dlt.secrets.value) == 1023

    # env will make it string, also section is optional
    environment["PORT"] = "UNKNOWN"
    assert single_val() == "UNKNOWN"

    environment["API__PORT"] = "1025"
    assert single_val() == "1025"


def test_toml_mixed_config_inject(toml_providers: ConfigProvidersContainer) -> None:
    # get data from both providers

    @with_config
    def mixed_val(
        api_type=dlt.config.value,
        secret_value: TSecretValue = dlt.secrets.value,
        typecheck: Any = dlt.config.value,
    ):
        return api_type, secret_value, typecheck

    _tup = mixed_val(dlt.config.value, dlt.secrets.value, dlt.config.value)
    assert _tup[0] == "REST"
    assert _tup[1] == "2137"
    assert isinstance(_tup[2], dict)

    _tup = mixed_val()
    assert _tup[0] == "REST"
    assert _tup[1] == "2137"
    assert isinstance(_tup[2], dict)


def test_toml_sections(toml_providers: ConfigProvidersContainer) -> None:
    cfg = toml_providers["config.toml"]
    assert cfg.get_value("api_type", str, None) == ("REST", "api_type")
    assert cfg.get_value("port", int, None, "api") == (1024, "api.port")
    assert cfg.get_value("param1", str, None, "api", "params") == ("a", "api.params.param1")


def test_secrets_toml_credentials(
    environment: Any, toml_providers: ConfigProvidersContainer
) -> None:
    # there are credentials exactly under destination.bigquery.credentials
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination", "bigquery")
    )
    assert c.project_id.endswith("destination.bigquery.credentials")
    # there are no destination.gcp_storage.credentials so it will fallback to "destination"."credentials"
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination", "gcp_storage")
    )
    assert c.project_id.endswith("destination.credentials")
    # also explicit
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination",)
    )
    assert c.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        print(dict(resolve.resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults())))
    # also try postgres credentials
    c2 = ConnectionStringCompatCredentials()
    c2.update({"drivername": "postgres"})
    c2 = resolve.resolve_configuration(c2, sections=("destination", "redshift"))
    assert c2.database == "destination.redshift.credentials"
    # bigquery credentials do not match redshift credentials
    c3 = ConnectionStringCompatCredentials()
    c3.update({"drivername": "postgres"})
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(c3, sections=("destination", "bigquery"))


def test_secrets_toml_embedded_credentials(
    environment: Any, toml_providers: ConfigProvidersContainer
) -> None:
    # will try destination.bigquery.credentials
    c = resolve.resolve_configuration(
        EmbeddedWithGcpCredentials(), sections=("destination", "bigquery")
    )
    assert c.credentials.project_id.endswith("destination.bigquery.credentials")
    # will try destination.gcp_storage.credentials and fallback to destination.credentials
    c = resolve.resolve_configuration(
        EmbeddedWithGcpCredentials(), sections=("destination", "gcp_storage")
    )
    assert c.credentials.project_id.endswith("destination.credentials")
    # will try everything until credentials in the root where incomplete credentials are present
    c = EmbeddedWithGcpCredentials()
    # create embedded config that will be passed as initial
    c.credentials = GcpServiceAccountCredentialsWithoutDefaults()
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve.resolve_configuration(c, sections=("middleware", "storage"))
    # so we can read partially filled configuration here
    assert c.credentials.project_id.endswith("-credentials")
    assert set(py_ex.value.traces.keys()) == {"client_email", "private_key"}

    # embed "gcp_storage" will bubble up to the very top, never reverts to "credentials"
    c2 = resolve.resolve_configuration(
        EmbeddedWithGcpStorage(), sections=("destination", "bigquery")
    )
    assert c2.gcp_storage.project_id.endswith("-gcp-storage")

    # also explicit
    c3 = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination",)
    )
    assert c3.project_id.endswith("destination.credentials")
    # there's "credentials" key but does not contain valid gcp credentials
    with pytest.raises(ConfigFieldMissingException):
        resolve.resolve_configuration(GcpServiceAccountCredentialsWithoutDefaults())


def test_dicts_are_not_enumerated() -> None:
    # dicts returned by toml provider cannot be used as explicit values or initial values for the whole configurations
    pass


def test_secrets_toml_credentials_from_native_repr(
    environment: Any, toml_providers: ConfigProvidersContainer
) -> None:
    # cfg = toml_providers["secrets.toml"]
    # print(cfg._config_doc)
    # print(cfg._config_doc["source"]["credentials"])
    # resolve gcp_credentials by parsing initial value which is str holding json doc
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("source",)
    )
    assert (
        c.private_key
        == "-----BEGIN PRIVATE"
        " KEY-----\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCNEN0bL39HmD+S\n...\n-----END"
        " PRIVATE KEY-----\n"
    )
    # project id taken from the same value, will not be overridden from any other configs
    assert c.project_id.endswith("mock-project-id-source.credentials")
    # also try sql alchemy url (native repr)
    c2 = resolve.resolve_configuration(ConnectionStringCredentials(), sections=("databricks",))
    assert c2.drivername == "databricks+connector"
    assert c2.username == "token"
    assert c2.password == "<databricks_token>"
    assert c2.host == "<databricks_host>"
    assert c2.port == 443
    assert c2.database == "<database_or_schema_name>"
    assert c2.query == {"conn_timeout": "15", "search_path": "a,b,c"}


def test_toml_get_key_as_section(toml_providers: ConfigProvidersContainer) -> None:
    cfg = toml_providers["secrets.toml"]
    # [credentials]
    # secret_value="2137"
    # so the line below will try to use secrets_value value as section, this must fallback to not found
    cfg.get_value("value", str, None, "credentials", "secret_value")


def test_toml_read_exception() -> None:
    pipeline_root = "./tests/common/cases/configuration/.wrong.dlt"
    with pytest.raises(TomlProviderReadException) as py_ex:
        ConfigTomlProvider(settings_dir=pipeline_root)
    assert py_ex.value.file_name == "config.toml"


def test_toml_global_config() -> None:
    # get current providers
    providers = Container()[PluggableRunContext].providers
    secrets = providers[SECRETS_TOML]
    config = providers[CONFIG_TOML]
    # in pytest should be false
    assert secrets._global_dir is None  # type: ignore[attr-defined]
    assert config._global_dir is None  # type: ignore[attr-defined]

    # set dlt data and settings dir
    global_dir = "./tests/common/cases/configuration/dlt_home"
    settings_dir = "./tests/common/cases/configuration/.dlt"
    # create instance with global toml enabled
    config = ConfigTomlProvider(settings_dir=settings_dir, global_dir=global_dir)
    assert config._global_dir == os.path.join(global_dir, CONFIG_TOML)
    assert isinstance(config._config_doc, dict)
    assert len(config._config_doc) > 0
    # kept from global
    v, key = config.get_value("dlthub_telemetry", bool, None, "runtime")
    assert v is False
    assert key == "runtime.dlthub_telemetry"
    v, _ = config.get_value("param_global", bool, None, "api", "params")
    assert v == "G"
    # kept from project
    v, _ = config.get_value("log_level", bool, None, "runtime")
    assert v == "ERROR"
    # project overwrites
    v, _ = config.get_value("param1", bool, None, "api", "params")
    assert v == "a"

    secrets = SecretsTomlProvider(settings_dir=settings_dir, global_dir=global_dir)
    assert secrets._global_dir == os.path.join(global_dir, SECRETS_TOML)
    # check if values from project exist
    secrets_project = SecretsTomlProvider(settings_dir=settings_dir)
    assert secrets._config_doc == secrets_project._config_doc


def test_write_value(toml_providers: ConfigProvidersContainer) -> None:
    provider: SettingsTomlProvider
    for provider in toml_providers.providers:  # type: ignore[assignment]
        if not provider.is_writable:
            continue
        # set single key
        provider.set_value("_new_key_bool", True, None)
        TAny: Type[Any] = Any  # type: ignore[assignment]
        assert provider.get_value("_new_key_bool", TAny, None) == (True, "_new_key_bool")
        provider.set_value("_new_key_literal", TSecretValue("literal"), None)
        assert provider.get_value("_new_key_literal", TAny, None) == ("literal", "_new_key_literal")
        # this will create path of tables
        provider.set_value("deep_int", 2137, "deep_pipeline", "deep", "deep", "deep", "deep")
        assert (
            provider._config_doc["deep_pipeline"]["deep"]["deep"]["deep"]["deep"]["deep_int"]
            == 2137
        )
        assert provider.get_value(
            "deep_int", TAny, "deep_pipeline", "deep", "deep", "deep", "deep"
        ) == (2137, "deep_pipeline.deep.deep.deep.deep.deep_int")
        # same without the pipeline
        now = pendulum.now()
        provider.set_value("deep_date", now, None, "deep", "deep", "deep", "deep")
        assert provider.get_value("deep_date", TAny, None, "deep", "deep", "deep", "deep") == (
            now,
            "deep.deep.deep.deep.deep_date",
        )
        # in existing path
        provider.set_value("deep_list", [1, 2, 3], None, "deep", "deep", "deep")
        assert provider.get_value("deep_list", TAny, None, "deep", "deep", "deep") == (
            [1, 2, 3],
            "deep.deep.deep.deep_list",
        )
        # still there
        assert provider.get_value("deep_date", TAny, None, "deep", "deep", "deep", "deep") == (
            now,
            "deep.deep.deep.deep.deep_date",
        )
        # overwrite value
        provider.set_value("deep_list", [1, 2, 3, 4], None, "deep", "deep", "deep")
        assert provider.get_value("deep_list", TAny, None, "deep", "deep", "deep") == (
            [1, 2, 3, 4],
            "deep.deep.deep.deep_list",
        )

        # write new dict to a new key
        test_d1 = {"key": "top", "embed": {"inner": "bottom", "inner_2": True}}
        provider.set_value("deep_dict", test_d1, None, "dict_test")
        assert provider.get_value("deep_dict", TAny, None, "dict_test") == (
            test_d1,
            "dict_test.deep_dict",
        )
        # write same dict over dict
        provider.set_value("deep_dict", test_d1, None, "dict_test")
        assert provider.get_value("deep_dict", TAny, None, "dict_test") == (
            test_d1,
            "dict_test.deep_dict",
        )
        # get a fragment
        assert provider.get_value("inner_2", TAny, None, "dict_test", "deep_dict", "embed") == (
            True,
            "dict_test.deep_dict.embed.inner_2",
        )
        # write a dict over non dict
        ovr_dict = {"ovr": 1, "ocr": {"ovr": 2}}
        provider.set_value("deep_list", ovr_dict, None, "deep", "deep", "deep")
        assert provider.get_value("deep_list", TAny, None, "deep", "deep", "deep") == (
            ovr_dict,
            "deep.deep.deep.deep_list",
        )
        # merge dicts
        test_d2 = {"key": "_top", "key2": "new2", "embed": {"inner": "_bottom", "inner_3": 2121}}
        provider.set_value("deep_dict", test_d2, None, "dict_test")
        test_m_d1_d2 = {
            "key": "_top",
            "embed": {"inner": "_bottom", "inner_2": True, "inner_3": 2121},
            "key2": "new2",
        }
        assert provider.get_value("deep_dict", TAny, None, "dict_test") == (
            test_m_d1_d2,
            "dict_test.deep_dict",
        )
        # compare toml and doc repr
        assert provider._config_doc == provider._config_toml.unwrap()

        # write configuration
        pool = PoolRunnerConfiguration(pool_type="none", workers=10)
        provider.set_value("runner_config", dict(pool), "new_pipeline")
        # print(provider._config_doc["new_pipeline"]["runner_config"].as_string())
        expected_pool = dict(pool)
        # None is removed
        expected_pool.pop("start_method")
        assert provider._config_doc["new_pipeline"]["runner_config"] == expected_pool


def test_set_spec_value(toml_providers: ConfigProvidersContainer) -> None:
    provider: BaseDocProvider
    for provider in toml_providers.providers:  # type: ignore[assignment]
        if not provider.is_writable:
            continue
        provider._config_doc = {}
        # dict creates only shallow dict so embedded credentials will fail
        creds = WithCredentialsConfiguration()
        credentials = SecretCredentials(secret_value=TSecretValue("***** ***"))
        creds.credentials = credentials

        # use dataclass to dict to recursively convert base config to dict
        import dataclasses

        provider.set_value("written_creds", dataclasses.asdict(creds), None)
        # resolve config
        resolved_config = resolve.resolve_configuration(
            WithCredentialsConfiguration(), sections=("written_creds",)
        )
        assert resolved_config.credentials.secret_value == "***** ***"


def test_set_fragment(toml_providers: ConfigProvidersContainer) -> None:
    provider: SettingsTomlProvider
    for provider in toml_providers.providers:  # type: ignore[assignment]
        if not isinstance(provider, BaseDocProvider):
            continue
        new_toml = """
int_val = 2232

[table]
inner_int_val = 2121
"""

        # key == None replaces the whole document
        provider.set_fragment(None, new_toml, None)
        print(provider.to_yaml())
        assert provider.to_toml().strip() == new_toml.strip()
        val, _ = provider.get_value("table", dict, None)
        assert val is not None

        # key != None merges documents
        to_merge_yaml = """
int_val: 2137

babble:
    word1: do
    word2: you

"""
        provider.set_fragment("", to_merge_yaml, None)
        merged_doc = """
int_val = 2137

[table]
inner_int_val = 2121

[babble]
word1 = "do"
word2 = "you"

"""
    assert provider.to_toml().strip() == merged_doc.strip()

    # currently we ignore the key when merging tomlkit
    provider.set_fragment("level", to_merge_yaml, None)
    assert provider.to_toml().strip() == merged_doc.strip()

    # use JSON: empty key replaces dict
    provider.set_fragment(None, json.dumps({"prop1": "A", "nested": {"propN": "N"}}), None)
    assert provider._config_doc == {"prop1": "A", "nested": {"propN": "N"}}
    # key cannot be empty for set_value
    with pytest.raises(ValueError):
        provider.set_value(None, "VAL", None)
    # dict always merges from the top level doc, ignoring the key
    provider.set_fragment(
        "nested", json.dumps({"prop2": "B", "nested": {"prop3": "C"}, "prop1": ""}), None
    )
    assert provider._config_doc == {
        "prop2": "B",
        "nested": {"propN": "N", "prop3": "C"},
        "prop1": "",
    }


def test_toml_string_provider() -> None:
    # test basic reading
    provider = StringTomlProvider("""
[section1.subsection]
key1 = "value1"

[section2.subsection]
key2 = "value2"
""")

    assert provider.get_value("key1", "", "section1", "subsection") == ("value1", "section1.subsection.key1")  # type: ignore[arg-type]
    assert provider.get_value("key2", "", "section2", "subsection") == ("value2", "section2.subsection.key2")  # type: ignore[arg-type]

    # test basic writing
    provider = StringTomlProvider("")
    assert provider.dumps() == ""

    provider.set_value("key1", "value1", "section1", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"value1\"
"""

    provider.set_value("key1", "other_value", "section1", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"other_value\"
"""
    provider.set_value("key1", "other_value", "section2", "subsection")
    assert provider.dumps() == """[section1.subsection]
key1 = \"other_value\"

[section2.subsection]
key1 = \"other_value\"
"""


def test_custom_loader(toml_providers: ConfigProvidersContainer) -> None:
    def loader() -> Dict[str, Any]:
        with open("tests/common/cases/configuration/config.yml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    # remove all providers
    toml_providers.providers.clear()
    # create new provider
    provider = CustomLoaderDocProvider("yaml", loader, True)
    assert provider.name == "yaml"
    assert provider.supports_secrets is True
    assert provider.to_toml().startswith("[destination]")
    assert provider.to_yaml().startswith("destination:")
    value, _ = provider.get_value("datetime", datetime.datetime, None, "data_types")
    assert value == pendulum.parse("1979-05-27 07:32:00-08:00")

    # add to context
    toml_providers.add_provider(provider)

    # resolve one of configs
    config = resolve.resolve_configuration(
        ConnectionStringCredentials(),
        sections=(
            "destination",
            "postgres",
        ),
    )
    assert config.username == "dlt-loader"
