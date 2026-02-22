"""Tests for VaultDocProvider using an in-memory mock vault."""

import pytest

from dlt.common.typing import TSecretValue, AnyType
from dlt.common.configuration.resolve import resolve_single_provider_value
from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

from tests.common.configuration.utils import MockVaultProvider


GLOBAL_TOML = """\
[sources.my_source]
api_key = "GLOBAL_KEY"
"""

PIPELINE_TOML = """\
[my_pipe.sources.my_source]
api_key = "PIPELINE_KEY"
"""

SOURCE_FRAGMENT = """\
[sources.my_source]
api_key = "SRC_FRAG"
secret_prop = "SRC_PROP"
"""

SOURCE_NAMED_FRAGMENT = """\
[sources.my_source]
api_key = "SRC_NAMED"
"""

DEST_FRAGMENT = """\
[destination.postgres]
password = "PG_PASS"
"""


def _make_provider(
    only_secrets: bool = False,
    only_toml_fragments: bool = False,
    list_secrets: bool = False,
) -> MockVaultProvider:
    return MockVaultProvider(
        only_secrets=only_secrets,
        only_toml_fragments=only_toml_fragments,
        list_secrets=list_secrets,
    )


def test_global_dlt_secrets_toml() -> None:
    """Global dlt_secrets_toml is loaded on first access and values are found."""
    provider = _make_provider()
    provider.set_secret(SECRETS_TOML_KEY, GLOBAL_TOML)

    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "GLOBAL_KEY"


def test_pipeline_scoped_dlt_secrets_toml() -> None:
    """Pipeline-scoped dlt_secrets_toml is loaded when pipeline_name is given."""
    provider = _make_provider()
    provider.set_secret("my_pipe.dlt_secrets_toml", PIPELINE_TOML)

    # without pipeline name: not found
    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value is None

    # with pipeline name: found
    value, _ = provider.get_value("api_key", TSecretValue, "my_pipe", "sources", "my_source")
    assert value == "PIPELINE_KEY"


def test_source_fragment_loading() -> None:
    """Source fragments are loaded and merged."""
    provider = _make_provider()
    provider.set_secret("sources", SOURCE_FRAGMENT)

    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "SRC_FRAG"

    value, _ = provider.get_value("secret_prop", TSecretValue, None, "sources", "my_source")
    assert value == "SRC_PROP"


def test_source_named_fragment_overrides_general() -> None:
    """More specific sources.<name> fragment overrides general sources fragment."""
    provider = _make_provider()
    provider.set_secret("sources", SOURCE_FRAGMENT)
    provider.set_secret("sources.my_source", SOURCE_NAMED_FRAGMENT)

    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "SRC_NAMED"

    # property only in general fragment still accessible
    value, _ = provider.get_value("secret_prop", TSecretValue, None, "sources", "my_source")
    assert value == "SRC_PROP"


def test_destination_fragment_loading() -> None:
    """Destination fragments are loaded."""
    provider = _make_provider()
    provider.set_secret("destination", DEST_FRAGMENT)

    value, _ = provider.get_value("password", TSecretValue, None, "destination", "postgres")
    assert value == "PG_PASS"


def test_pipeline_scoped_source_fragments() -> None:
    """Pipeline-scoped source fragments are loaded and override global ones."""
    pipeline_src = """\
[my_pipe.sources.my_source]
api_key = "PIPE_SRC"
"""
    provider = _make_provider()
    provider.set_secret("sources", SOURCE_FRAGMENT)
    provider.set_secret("my_pipe.sources", pipeline_src)

    value, _ = provider.get_value("api_key", TSecretValue, "my_pipe", "sources", "my_source")
    assert value == "PIPE_SRC"


def test_only_secrets_skips_non_secret_hints() -> None:
    """With only_secrets=True, non-secret hints return None without vault calls."""
    provider = _make_provider(only_secrets=True)
    provider.set_secret("sources", SOURCE_FRAGMENT)

    # non-secret hint: skipped
    value, _ = provider.get_value("api_key", str, None, "sources", "my_source")
    assert value is None

    # secret hint: found
    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "SRC_FRAG"


def test_only_toml_fragments_no_single_value_fallback() -> None:
    """With only_toml_fragments=True, single-value lookups are skipped."""
    provider = _make_provider(only_toml_fragments=True)
    # no fragment, but single value exists
    provider.set_secret("sources.my_source.api_key", "SINGLE_VAL")

    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value is None


def test_single_value_fallback() -> None:
    """With only_toml_fragments=False, single values are fetched as fallback."""
    provider = _make_provider(only_toml_fragments=False)
    provider.set_secret("sources.my_source.api_key", "SINGLE_VAL")

    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "SINGLE_VAL"


def test_list_secrets_skips_missing_keys() -> None:
    """With list_secrets=True, keys not in the listed set skip vault calls."""
    provider = _make_provider(list_secrets=True)
    provider.set_secret(SECRETS_TOML_KEY, GLOBAL_TOML)

    provider._look_vault_calls.clear()
    value, _ = provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    assert value == "GLOBAL_KEY"

    # only keys present in vault were looked up, missing keys were skipped
    for call in provider._look_vault_calls:
        assert call in provider._vault


def test_lookup_caching() -> None:
    """Same vault key is only fetched once."""
    provider = _make_provider()
    provider.set_secret("sources", SOURCE_FRAGMENT)

    provider.get_value("api_key", TSecretValue, None, "sources", "my_source")
    first_calls = len(provider._look_vault_calls)

    provider.get_value("secret_prop", TSecretValue, None, "sources", "my_source")
    # second call should not re-fetch "sources" fragment
    assert provider._look_vault_calls.count("sources") == 1
    # but total calls may increase for other lookups
    assert len(provider._look_vault_calls) >= first_calls


def test_pipeline_name_flows_through_resolve() -> None:
    """Pipeline name reaches the vault provider via resolve_single_provider_value."""
    provider = _make_provider()
    provider.set_secret("my_pipe.dlt_secrets_toml", PIPELINE_TOML)

    # resolve_single_provider_value passes pipeline_name directly to provider
    value, _ = resolve_single_provider_value(
        provider,
        key="api_key",
        hint=TSecretValue,
        pipeline_name="my_pipe",
        explicit_sections=("sources", "my_source"),
    )
    assert value == "PIPELINE_KEY"

    # without pipeline: not found
    provider2 = _make_provider()
    provider2.set_secret("my_pipe.dlt_secrets_toml", PIPELINE_TOML)
    value2, _ = resolve_single_provider_value(
        provider2,
        key="api_key",
        hint=TSecretValue,
        explicit_sections=("sources", "my_source"),
    )
    assert value2 is None
