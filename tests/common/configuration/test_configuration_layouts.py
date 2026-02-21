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
                (None, "destination", "bigquery", "my_dest"),
                (None, "destination", "bigquery"),
                (None, "destination"),
                (None,),
            ],
        ),
        (
            ("sources", "chess"),
            (),
            None,
            [(None, "sources", "chess"), (None, "sources"), (None,)],
        ),
        ((), (), None, [(None,)]),
        # config_section (eg. credentials) appended to each path
        (
            ("sources", "chess"),
            (),
            "credentials",
            [
                (None, "sources", "chess", "credentials"),
                (None, "sources", "credentials"),
                (None, "credentials"),
            ],
        ),
        # embedded sections (nested config fields) extend the path
        (
            ("sources", "chess"),
            ("api_config", "retry"),
            None,
            [
                (None, "sources", "chess", "api_config", "retry"),
                (None, "sources", "chess", "api_config"),
                (None, "sources", "chess"),
                (None, "sources"),
                (None,),
            ],
        ),
        # embedded + config_section
        (
            ("sources", "chess"),
            ("api_config",),
            "credentials",
            [
                (None, "sources", "chess", "api_config", "credentials"),
                (None, "sources", "chess", "credentials"),
                (None, "sources", "credentials"),
                (None, "credentials"),
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
    expected: List[Tuple[Optional[str], ...]],
) -> None:
    assert _build_section_lookup_paths(explicit, embedded, config_section, True) == expected


@pytest.mark.parametrize(
    "explicit,embedded,config_section,expected",
    [
        # sources.section.name inserts compact sources.name path
        (
            ("sources", "chess_com", "chess"),
            (),
            None,
            [
                (None, "sources", "chess_com", "chess"),
                (None, "sources", "chess"),
                (None, "sources", "chess_com"),
                (None, "sources"),
                (None,),
            ],
        ),
        # compact with config_section (embedded credentials)
        (
            ("sources", "chess_com", "chess"),
            (),
            "credentials",
            [
                (None, "sources", "chess_com", "chess", "credentials"),
                (None, "sources", "chess", "credentials"),
                (None, "sources", "chess_com", "credentials"),
                (None, "sources", "credentials"),
                (None, "credentials"),
            ],
        ),
        # compact with embedded sections
        (
            ("sources", "chess_com", "chess"),
            ("inner",),
            None,
            [
                (None, "sources", "chess_com", "chess", "inner"),
                (None, "sources", "chess", "inner"),
                (None, "sources", "chess_com", "chess"),
                (None, "sources", "chess_com"),
                (None, "sources"),
                (None,),
            ],
        ),
        # section == name: no duplicate compact
        (
            ("sources", "chess", "chess"),
            (),
            None,
            [
                (None, "sources", "chess", "chess"),
                (None, "sources", "chess"),
                (None, "sources"),
                (None,),
            ],
        ),
        # non-sources top section: no compact
        (
            ("destination", "bigquery", "my_dest"),
            (),
            None,
            [
                (None, "destination", "bigquery", "my_dest"),
                (None, "destination", "bigquery"),
                (None, "destination"),
                (None,),
            ],
        ),
        # only 2 sections under sources: no compact
        (
            ("sources", "chess"),
            (),
            None,
            [(None, "sources", "chess"), (None, "sources"), (None,)],
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
    expected: List[Tuple[Optional[str], ...]],
) -> None:
    assert _build_section_lookup_paths(explicit, embedded, config_section, True) == expected


@pytest.mark.parametrize(
    "pipeline_name,expected",
    [
        # pipeline-prefixed paths come first, then non-prefixed
        (
            "my_pipe",
            [
                ("my_pipe", "sources", "mod", "src"),
                ("my_pipe", "sources", "src"),
                ("my_pipe", "sources", "mod"),
                ("my_pipe", "sources"),
                ("my_pipe",),
                (None, "sources", "mod", "src"),
                (None, "sources", "src"),
                (None, "sources", "mod"),
                (None, "sources"),
                (None,),
            ],
        ),
        # no pipeline: all paths tagged with None
        (
            None,
            [
                (None, "sources", "mod", "src"),
                (None, "sources", "src"),
                (None, "sources", "mod"),
                (None, "sources"),
                (None,),
            ],
        ),
    ],
    ids=["with-pipeline", "no-pipeline"],
)
def test_pipeline_name_prefix(
    pipeline_name: str,
    expected: List[Tuple[Optional[str], ...]],
) -> None:
    assert (
        _build_section_lookup_paths(("sources", "mod", "src"), (), None, True, pipeline_name)
        == expected
    )


def test_non_section_provider_paths() -> None:
    """Non-section providers always get a single (None,) path."""
    assert _build_section_lookup_paths(("a", "b"), (), None, False) == [(None,)]
    assert _build_section_lookup_paths(("a", "b"), (), None, False, "pipe") == [(None,)]
    assert _build_section_lookup_paths((), (), "cfg", False) == [(None,)]


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
    """Pipeline name is passed to provider as first arg, not baked into sections."""
    # pipeline-scoped value is found via provider's own pipeline_name handling
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

    # provider receives actual pipeline_name for prefixed paths, None for non-prefixed
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
    # first batch has pipeline_name, second batch has None
    assert "my_pipe" in received
    assert None in received


def test_non_section_provider_bare_key_only() -> None:
    """Non-section providers try bare key with pipeline_name=None."""
    call_log: List[Tuple[Optional[str], Tuple[str, ...]]] = []

    class Tracker(MockProvider):
        @property
        def supports_sections(self):
            return False

        def get_value(self, key, hint, pipeline_name, *sections):
            call_log.append((pipeline_name, sections))
            return None, key

    resolve_single_provider_value(
        Tracker(),
        key="k",
        hint=str,
        pipeline_name="pipe",
        explicit_sections=("sources", "mod", "src"),
    )
    assert call_log == [(None, ())]
