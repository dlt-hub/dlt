import os
import pytest
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple, Type
import datetime  # noqa: I251

import dlt
from dlt.common import pendulum
from dlt.common.configuration import configspec, ConfigFieldMissingException, resolve
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.configuration.providers.yaml import (
    CONFIG_YAML,
    SECRETS_YAML,
    SettingsYamlProvider,
    SecretsYamlProvider,
    ConfigYamlProvider,
    StringYamlProvider,
    YamlProviderReadException,
)
from dlt.common.configuration.providers.toml import GLOBAL_ORIGIN_PREFIX, BaseDocProvider
from dlt.common.configuration.providers.utils import warn_on_toml_yaml_collision
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.utils import get_resolved_traces
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentialsWithoutDefaults,
    ConnectionStringCredentials,
)
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ, _reset_yaml_providers, capture_dlt_logger
from tests.common.configuration.utils import (
    ConnectionStringCompatCredentials,
    CoercionTestConfiguration,
    COERCIONS,
    environment,
)

YAML_CASES_DIR = "./tests/common/cases/configuration/.dlt_yaml"


@pytest.fixture
def yaml_providers() -> Iterator[ConfigProvidersContainer]:
    """Injects yaml providers reading from ./tests/common/cases/configuration/.dlt_yaml"""
    yield from _reset_yaml_providers(os.path.abspath(YAML_CASES_DIR))


def test_toml_types(yaml_providers: ConfigProvidersContainer) -> None:
    c = resolve.resolve_configuration(CoercionTestConfiguration(), sections=("typecheck",))
    for k, v in COERCIONS.items():
        if isinstance(v, tuple):
            v = list(v)
        if isinstance(v, datetime.datetime):
            v = pendulum.parse("1979-05-27T07:32:00-08:00")
        assert v == c[k]

    tracer = get_resolved_traces()
    traces = tracer._get_log_as_dict(tracer.resolved_traces)
    for k in COERCIONS:
        assert traces[f"typecheck.{k}"].provider_location == CONFIG_YAML


def test_config_provider_order(yaml_providers: ConfigProvidersContainer, environment: Any) -> None:
    @with_config(sections=("api",))
    def single_val(port=None):
        return port

    # secrets have api.port=1023 and this will be used
    assert single_val(dlt.secrets.value) == 1023

    environment["PORT"] = "UNKNOWN"
    assert single_val() == "UNKNOWN"

    environment["API__PORT"] = "1025"
    assert single_val() == "1025"


def test_yaml_sections(yaml_providers: ConfigProvidersContainer) -> None:
    cfg = yaml_providers["config.yaml"]
    assert cfg.get_value("api_type", str, None) == ("REST", "api_type")
    assert cfg.get_value("port", int, None, "api") == (1024, "api.port")
    assert cfg.get_value("param1", str, None, "api", "params") == ("a", "api.params.param1")


def test_secrets_yaml_credentials(
    environment: Any, yaml_providers: ConfigProvidersContainer
) -> None:
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination", "bigquery")
    )
    assert c.project_id.endswith("destination.bigquery.credentials")
    tracer = get_resolved_traces()
    traces = tracer._get_log_as_dict(tracer.resolved_traces)
    assert traces["destination.bigquery.credentials.project_id"].provider_location == SECRETS_YAML
    assert traces["destination.bigquery.credentials.private_key"].provider_location == SECRETS_YAML
    # there are no destination.gcp_storage.credentials so it will fallback to "destination"."credentials"
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("destination", "gcp_storage")
    )
    assert c.project_id.endswith("destination.credentials")
    c2 = ConnectionStringCompatCredentials()
    c2.update({"drivername": "postgres"})
    c2 = resolve.resolve_configuration(c2, sections=("destination", "redshift"))
    assert c2.database == "destination.redshift.credentials"


def test_secrets_yaml_credentials_from_native_repr(
    environment: Any, yaml_providers: ConfigProvidersContainer
) -> None:
    c = resolve.resolve_configuration(
        GcpServiceAccountCredentialsWithoutDefaults(), sections=("source",)
    )
    assert (
        c.private_key
        == "-----BEGIN PRIVATE"
        " KEY-----\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCNEN0bL39HmD+S\n...\n-----END"
        " PRIVATE KEY-----\n"
    )
    assert c.project_id.endswith("mock-project-id-source.credentials")
    c2 = resolve.resolve_configuration(ConnectionStringCredentials(), sections=("databricks",))
    assert c2.drivername == "databricks+connector"
    assert c2.username == "token"
    assert c2.password == "<databricks_token>"
    assert c2.host == "<databricks_host>"
    assert c2.port == 443
    assert c2.database == "<database_or_schema_name>"
    assert c2.query == {"conn_timeout": "15", "search_path": "a,b,c"}


def test_yaml_read_exception() -> None:
    pipeline_root = "./tests/common/cases/configuration/.wrong_yaml"
    with pytest.raises(YamlProviderReadException) as py_ex:
        ConfigYamlProvider(settings_dir=pipeline_root)
    assert py_ex.value.file_name == "config.yaml"


def test_yaml_global_config() -> None:
    global_dir = "./tests/common/cases/configuration/dlt_home_yaml"
    settings_dir = YAML_CASES_DIR
    config = ConfigYamlProvider(settings_dir=settings_dir, global_dir=global_dir)
    assert config._yaml_paths[1] == os.path.join(global_dir, CONFIG_YAML)
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
    # verify global location
    assert os.path.join(global_dir, "config.yaml") in config.locations
    assert os.path.join(global_dir, "config.yaml") in config.present_locations
    # verify local location
    assert os.path.join(settings_dir, "config.yaml") in config.locations
    assert os.path.join(settings_dir, "config.yaml") in config.present_locations

    secrets = SecretsYamlProvider(settings_dir=settings_dir, global_dir=global_dir)
    secrets_project = SecretsYamlProvider(settings_dir=settings_dir)
    assert secrets._config_doc == secrets_project._config_doc
    assert os.path.join(global_dir, "secrets.yaml") in secrets.locations
    assert os.path.join(global_dir, "secrets.yaml") not in secrets.present_locations


class ProfileLikeYamlProvider(SettingsYamlProvider):
    """Loads `dev.{file_name}` before `{file_name}` in each dir, like the workspace profiles do"""

    def _resolve_yaml_paths(self, file_name: str, resolvable_dirs: List[str]) -> List[str]:
        paths = []
        for d in resolvable_dirs:
            paths.append(os.path.join(d, f"dev.{file_name}"))
            paths.append(os.path.join(d, file_name))
        return paths


ORIGINS_CASES_DIR = "./tests/common/cases/configuration/origins_yaml"
ORIGINS_GLOBAL_DIR = os.path.join(ORIGINS_CASES_DIR, "global")


def _origins_provider() -> ProfileLikeYamlProvider:
    return ProfileLikeYamlProvider(
        CONFIG_YAML,
        False,
        CONFIG_YAML,
        [ORIGINS_CASES_DIR, ORIGINS_GLOBAL_DIR],
        ORIGINS_GLOBAL_DIR,
    )


def test_yaml_value_location_merged_files() -> None:
    """Exact file is reported when files with different names are merged, base or override"""
    config = _origins_provider()
    dev_config_yaml = "dev." + CONFIG_YAML
    global_config_yaml = GLOBAL_ORIGIN_PREFIX + CONFIG_YAML
    global_dev_config_yaml = GLOBAL_ORIGIN_PREFIX + dev_config_yaml

    assert config.get_value_location("dev_only", None) == dev_config_yaml
    assert config.get_value_location("base_only", None) == CONFIG_YAML
    assert config.get_value_location("global_dev_only", None) == global_dev_config_yaml
    assert config.get_value_location("global_only", None) == global_config_yaml
    # present in all four, the settings dir profile file wins value and location
    assert config.get_value("shared", str, None) == ("from_dev", "shared")
    assert config.get_value_location("shared", None) == dev_config_yaml
    assert config.get_value("key", str, None, "gtbl") == ("from_global_dev", "gtbl.key")
    assert config.get_value_location("key", None, "gtbl") == global_dev_config_yaml
    assert config.get_value_location("from_global_base", None, "gtbl") == global_config_yaml
    assert config.get_value_location("gtbl", None) == global_config_yaml
    assert config.get_value("create_indexes", bool, None) == (False, "create_indexes")
    assert config.get_value_location("create_indexes", None) == CONFIG_YAML
    assert config.get_value_location("from_base", None, "tbl") == CONFIG_YAML
    assert config.get_value_location("shared_key", None, "tbl") == dev_config_yaml
    assert config.get_value_location("from_dev", None, "tbl") == dev_config_yaml
    assert config.get_value_location("deep", None, "tbl", "sub") == CONFIG_YAML
    assert config.get_value_location("tbl", None) == CONFIG_YAML

    assert config.get_value_location("unknown", None) == ""
    assert config.get_value_location("unknown", None, "tbl") == ""


def test_yaml_value_location_runtime_writes() -> None:
    """Values written at runtime are in no file, `preserve` brings the file origins back"""
    config = _origins_provider()
    dev_config_yaml = "dev." + CONFIG_YAML

    config.set_value("written", "x", None)
    assert config.get_value_location("written", None) == ""
    config.set_value("base_only", "x", None)
    assert config.get_value_location("base_only", None) == ""
    config.set_value("from_base", 2, None, "tbl")
    assert config.get_value_location("from_base", None, "tbl") == ""
    assert config.get_value_location("shared_key", None, "tbl") == dev_config_yaml
    config.set_value("deep", None, None, "tbl", "sub")
    assert config.get_value_location("deep", None, "tbl", "sub") == ""

    # a fragment under a key merges from the root and forgets only the keys it sets
    config = _origins_provider()
    config.set_fragment("tbl", "tbl:\n  from_base: 3\n", None)
    assert config.get_value("from_base", int, None, "tbl") == (3, "tbl.from_base")
    assert config.get_value_location("from_base", None, "tbl") == ""
    assert config.get_value_location("shared_key", None, "tbl") == dev_config_yaml

    # a fragment without a key replaces the whole document so no value comes from a file
    config = _origins_provider()
    config.set_fragment(None, "tbl:\n  from_base: 3\n", None)
    assert config.get_value_location("from_base", None, "tbl") == ""
    assert config.get_value_location("base_only", None) == ""

    # preserve restores the origins together with the document
    config = _origins_provider()
    with config.preserve():
        config.set_value("base_only", "x", None)
        assert config.get_value_location("base_only", None) == ""
    assert config.get_value_location("base_only", None) == CONFIG_YAML
    assert config.get_value_location("dev_only", None) == dev_config_yaml


@pytest.mark.parametrize("provider_kind", ["merged_names", "settings_and_global"])
def test_yaml_value_location_covers_whole_doc(provider_kind: str) -> None:
    """Origins doc keeps the config doc shape so every value and table has a location"""
    if provider_kind == "merged_names":
        config: SettingsYamlProvider = _origins_provider()
    else:
        config = ConfigYamlProvider(
            settings_dir=YAML_CASES_DIR,
            global_dir="./tests/common/cases/configuration/dlt_home_yaml",
        )

    def assert_located(doc: Dict[str, Any], sections: Tuple[str, ...] = ()) -> None:
        for key, value in doc.items():
            assert config.get_value_location(key, None, *sections) != "", ".".join((*sections, key))
            if isinstance(value, dict):
                assert_located(value, sections + (key,))

    assert config._config_doc
    assert_located(config._config_doc)


def test_write_value(yaml_providers: ConfigProvidersContainer) -> None:
    provider: SettingsYamlProvider
    for provider in yaml_providers.providers:  # type: ignore[assignment]
        if not provider.is_writable:
            continue
        provider.set_value("_new_key_bool", True, None)
        TAny: Type[Any] = Any
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
        provider.set_value("deep_list", [1, 2, 3], None, "deep", "deep", "deep")
        assert provider.get_value("deep_list", TAny, None, "deep", "deep", "deep") == (
            [1, 2, 3],
            "deep.deep.deep.deep_list",
        )
        provider.set_value("deep_list", [1, 2, 3, 4], None, "deep", "deep", "deep")
        assert provider.get_value("deep_list", TAny, None, "deep", "deep", "deep") == (
            [1, 2, 3, 4],
            "deep.deep.deep.deep_list",
        )

        test_d1 = {"key": "top", "embed": {"inner": "bottom", "inner_2": True}}
        provider.set_value("deep_dict", test_d1, None, "dict_test")
        assert provider.get_value("deep_dict", TAny, None, "dict_test") == (
            test_d1,
            "dict_test.deep_dict",
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


def test_set_value_none_deletes_key(yaml_providers: ConfigProvidersContainer) -> None:
    """value=None deletes the key from the config doc."""
    TAny: Type[Any] = Any
    provider: SettingsYamlProvider
    for provider in yaml_providers.providers:  # type: ignore[assignment]
        if not provider.is_writable:
            continue

        provider.set_value("_to_delete_top", "value", None)
        assert provider.get_value("_to_delete_top", TAny, None)[0] == "value"
        provider.set_value("_to_delete_top", None, None)
        assert provider.get_value("_to_delete_top", TAny, None)[0] is None
        assert "_to_delete_top" not in provider._config_doc

        provider.set_value("a", 1, None, "_del_section", "sub")
        provider.set_value("b", 2, None, "_del_section", "sub")
        provider.set_value("a", None, None, "_del_section", "sub")
        assert provider.get_value("a", TAny, None, "_del_section", "sub")[0] is None
        assert provider.get_value("b", TAny, None, "_del_section", "sub")[0] == 2
        assert provider._config_doc["_del_section"]["sub"] == {"b": 2}

        provider.set_value("k", None, None, "_no_such_section", "deeper")
        assert "_no_such_section" not in provider._config_doc

        provider.set_value("_never_existed", None, None, "_del_section", "sub")
        assert provider.get_value("_never_existed", TAny, None, "_del_section", "sub")[0] is None


def test_set_fragment(yaml_providers: ConfigProvidersContainer) -> None:
    provider: SettingsYamlProvider
    for provider in yaml_providers.providers:  # type: ignore[assignment]
        if not isinstance(provider, BaseDocProvider):
            continue
        new_yaml = "int_val: 2232\ntable:\n  inner_int_val: 2121\n"

        # key == None replaces the whole document
        provider.set_fragment(None, new_yaml, None)
        assert provider._config_doc == {"int_val": 2232, "table": {"inner_int_val": 2121}}
        val, _ = provider.get_value("table", dict, None)
        assert val is not None

        to_merge_yaml = "int_val: 2137\n\nbabble:\n  word1: do\n  word2: you\n"
        provider.set_fragment("", to_merge_yaml, None)
        assert provider._config_doc == {
            "int_val": 2137,
            "table": {"inner_int_val": 2121},
            "babble": {"word1": "do", "word2": "you"},
        }


def test_yaml_string_provider() -> None:
    provider = StringYamlProvider(
        "section1:\n  subsection:\n    key1: value1\nsection2:\n  subsection:\n    key2: value2\n"
    )

    assert provider.get_value("key1", "", "section1", "subsection") == ("value1", "section1.subsection.key1")  # type: ignore[arg-type]
    assert provider.get_value("key2", "", "section2", "subsection") == ("value2", "section2.subsection.key2")  # type: ignore[arg-type]
    assert provider.get_value_location("key1", None, "section1", "subsection") == ""

    provider = StringYamlProvider("")
    assert provider.dumps() == "{}\n"

    provider.set_value("key1", "value1", "section1", "subsection")
    assert provider.dumps() == "section1:\n  subsection:\n    key1: value1\n"


def test_warn_on_toml_yaml_collision(caplog: pytest.LogCaptureFixture) -> None:
    settings_dir = os.path.abspath("./tests/common/cases/configuration/.dlt")
    yaml_settings_dir = os.path.abspath(YAML_CASES_DIR)

    from dlt.common.configuration.providers.toml import ConfigTomlProvider

    toml_provider = ConfigTomlProvider(settings_dir=settings_dir)
    yaml_provider = ConfigYamlProvider(settings_dir=yaml_settings_dir)
    assert toml_provider.present_locations
    assert yaml_provider.present_locations

    with capture_dlt_logger(caplog):
        warn_on_toml_yaml_collision(toml_provider, yaml_provider)
    assert any("toml" in rec.message and "yaml" in rec.message for rec in caplog.records)
