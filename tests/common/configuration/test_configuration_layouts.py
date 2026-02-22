"""Unit tests for config section lookup path building and provider resolution."""

import pytest
from typing import List, Optional, Tuple

from dlt.common.typing import TSecretValue
from dlt.common.configuration.resolve import (
    _build_section_lookup_paths,
    resolve_single_provider_value,
)
from tests.common.configuration.utils import MockProvider, SecretMockProvider


@pytest.mark.parametrize(
    "explicit,embedded,config_section,expected",
    [
        (
            ("destination", "bigquery", "my_dest"),
            (),
            None,
            [
                ("destination", "bigquery", "my_dest"),
                ("destination", "bigquery"),
                ("destination",),
                (),
            ],
        ),
        (
            ("sources", "chess"),
            (),
            None,
            [("sources", "chess"), ("sources",), ()],
        ),
        ((), (), None, [()]),
        # config_section (eg. credentials) appended to each path
        (
            ("sources", "chess"),
            (),
            "credentials",
            [
                ("sources", "chess", "credentials"),
                ("sources", "credentials"),
                ("credentials",),
            ],
        ),
        # embedded sections (nested config fields) extend the path
        (
            ("sources", "chess"),
            ("api_config", "retry"),
            None,
            [
                ("sources", "chess", "api_config", "retry"),
                ("sources", "chess", "api_config"),
                ("sources", "chess"),
                ("sources",),
                (),
            ],
        ),
        # embedded + config_section
        (
            ("sources", "chess"),
            ("api_config",),
            "credentials",
            [
                ("sources", "chess", "api_config", "credentials"),
                ("sources", "chess", "credentials"),
                ("sources", "credentials"),
                ("credentials",),
            ],
        ),
    ],
    ids=[
        "dest.bigquery.my_dest",
        "sources.chess",
        "empty",
        "sources.chess+credentials",
        "sources.chess+embedded",
        "sources.chess+embedded+credentials",
    ],
)
def test_standard_pop_sequence(
    explicit: Tuple[str, ...],
    embedded: Tuple[str, ...],
    config_section: str,
    expected: List[Tuple[str, ...]],
) -> None:
    assert _build_section_lookup_paths(explicit, embedded, config_section, True) == expected


@pytest.mark.parametrize(
    "explicit,embedded,config_section,expected",
    [
        # sources.section.name — compact sources.name inserted AFTER sources.section
        (
            ("sources", "chess_com", "chess"),
            (),
            None,
            [
                ("sources", "chess_com", "chess"),
                ("sources", "chess_com"),
                ("sources", "chess"),
                ("sources",),
                (),
            ],
        ),
        # compact with config_section (embedded credentials)
        (
            ("sources", "chess_com", "chess"),
            (),
            "credentials",
            [
                ("sources", "chess_com", "chess", "credentials"),
                ("sources", "chess_com", "credentials"),
                ("sources", "chess", "credentials"),
                ("sources", "credentials"),
                ("credentials",),
            ],
        ),
        # compact with embedded sections
        (
            ("sources", "chess_com", "chess"),
            ("inner",),
            None,
            [
                ("sources", "chess_com", "chess", "inner"),
                ("sources", "chess_com", "chess"),
                ("sources", "chess", "inner"),
                ("sources", "chess_com"),
                ("sources",),
                (),
            ],
        ),
        # section == name: no duplicate compact
        (
            ("sources", "chess", "chess"),
            (),
            None,
            [
                ("sources", "chess", "chess"),
                ("sources", "chess"),
                ("sources",),
                (),
            ],
        ),
        # non-sources top section: no compact
        (
            ("destination", "bigquery", "my_dest"),
            (),
            None,
            [
                ("destination", "bigquery", "my_dest"),
                ("destination", "bigquery"),
                ("destination",),
                (),
            ],
        ),
        # only 2 sections under sources: no compact
        (
            ("sources", "chess"),
            (),
            None,
            [("sources", "chess"), ("sources",), ()],
        ),
    ],
    ids=[
        "compact-basic",
        "compact-credentials",
        "compact-embedded",
        "section-eq-name",
        "non-sources",
        "sources-2-sections",
    ],
)
def test_compact_sources_layout(
    explicit: Tuple[str, ...],
    embedded: Tuple[str, ...],
    config_section: str,
    expected: List[Tuple[str, ...]],
) -> None:
    assert _build_section_lookup_paths(explicit, embedded, config_section, True) == expected


def test_non_section_provider_paths() -> None:
    """Non-section providers always get a single empty-tuple path."""
    assert _build_section_lookup_paths(("a", "b"), (), None, False) == [()]
    assert _build_section_lookup_paths((), (), "cfg", False) == [()]


@pytest.mark.parametrize(
    "return_on,config_section",
    [
        (("sources", "chess_com", "chess"), None),
        (("sources", "chess"), None),
        (("sources", "chess", "credentials"), "credentials"),
    ],
    ids=["full-path", "compact-path", "compact-credentials"],
)
def test_sources_compact_resolution(return_on: Tuple[str, ...], config_section: str) -> None:
    provider = SecretMockProvider()
    provider.value = "found"
    provider.return_value_on = return_on
    value, _ = resolve_single_provider_value(
        provider,
        key="api_key",
        hint=TSecretValue,
        config_section=config_section,
        explicit_sections=("sources", "chess_com", "chess"),
    )
    assert value == "found"


def test_pipeline_name_passed_to_provider() -> None:
    """Pipeline name is passed directly to provider.get_value, not baked into sections."""
    provider = SecretMockProvider()
    provider.value = "pipeline_val"
    provider.return_value_on = ("my_pipe", "sources", "chess_com", "chess")
    value, _ = resolve_single_provider_value(
        provider,
        key="api_key",
        hint=TSecretValue,
        pipeline_name="my_pipe",
        explicit_sections=("sources", "chess_com", "chess"),
    )
    assert value == "pipeline_val"

    # all calls get the same pipeline_name
    received: List[Optional[str]] = []

    class Tracker(SecretMockProvider):
        def get_value(self, key, hint, pipeline_name, *sections):
            received.append(pipeline_name)
            return None, key

    resolve_single_provider_value(
        Tracker(),
        key="k",
        hint=TSecretValue,
        pipeline_name="my_pipe",
        explicit_sections=("sources", "mod", "src"),
    )
    # all calls receive the same pipeline_name
    assert all(pn == "my_pipe" for pn in received)


def test_non_section_provider_skipped_with_pipeline_name() -> None:
    """Non-section providers are skipped when pipeline_name is set."""
    call_log: List[Tuple[Optional[str], Tuple[str, ...]]] = []

    class Tracker(MockProvider):
        @property
        def supports_sections(self):
            return False

        def get_value(self, key, hint, pipeline_name, *sections):
            call_log.append((pipeline_name, sections))
            return None, key

    # with pipeline_name: provider is skipped entirely
    value, traces = resolve_single_provider_value(
        Tracker(),
        key="k",
        hint=str,
        pipeline_name="pipe",
        explicit_sections=("sources", "mod", "src"),
    )
    assert value is None
    assert call_log == []

    # without pipeline_name: provider is queried with bare key
    resolve_single_provider_value(
        Tracker(),
        key="k",
        hint=str,
        explicit_sections=("sources", "mod", "src"),
    )
    assert call_log == [(None, ())]
