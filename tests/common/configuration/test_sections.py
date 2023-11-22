import pytest
from typing import Any, Optional
from dlt.common.configuration.container import Container

from dlt.common.configuration import (
    configspec,
    ConfigFieldMissingException,
    resolve,
    inject_section,
)
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs import BaseConfiguration, ConfigSectionContext
from dlt.common.configuration.exceptions import LookupTrace
from dlt.common.typing import AnyType

from tests.utils import preserve_environ
from tests.common.configuration.utils import (
    MockProvider,
    SectionedConfiguration,
    environment,
    mock_provider,
    env_provider,
)


@configspec
class SingleValConfiguration(BaseConfiguration):
    sv: str


@configspec
class EmbeddedConfiguration(BaseConfiguration):
    sv_config: Optional[SingleValConfiguration]


@configspec
class EmbeddedWithSectionedConfiguration(BaseConfiguration):
    embedded: SectionedConfiguration


@configspec
class EmbeddedIgnoredConfiguration(BaseConfiguration):
    # underscore prevents the field name to be added to embedded sections
    _sv_config: Optional[SingleValConfiguration]


@configspec
class EmbeddedIgnoredWithSectionedConfiguration(BaseConfiguration):
    _embedded: SectionedConfiguration


@configspec
class EmbeddedWithIgnoredEmbeddedConfiguration(BaseConfiguration):
    ignored_embedded: EmbeddedIgnoredWithSectionedConfiguration


def test_sectioned_configuration(environment: Any, env_provider: ConfigProvider) -> None:
    with pytest.raises(ConfigFieldMissingException) as exc_val:
        resolve.resolve_configuration(SectionedConfiguration())

    assert list(exc_val.value.traces.keys()) == ["password"]
    assert exc_val.value.spec_name == "SectionedConfiguration"
    # check trace
    traces = exc_val.value.traces["password"]
    # only one provider and section was tried
    assert len(traces) == 1
    assert traces[0] == LookupTrace(
        "Environment Variables", ["DLT_TEST"], "DLT_TEST__PASSWORD", None
    )
    # assert traces[1] == LookupTrace("secrets.toml", ["DLT_TEST"], "DLT_TEST.password", None)
    # assert traces[2] == LookupTrace("config.toml", ["DLT_TEST"], "DLT_TEST.password", None)

    # init vars work without section
    C = resolve.resolve_configuration(SectionedConfiguration(), explicit_value={"password": "PASS"})
    assert C.password == "PASS"

    # env var must be prefixed
    environment["PASSWORD"] = "PASS"
    with pytest.raises(ConfigFieldMissingException) as exc_val:
        resolve.resolve_configuration(SectionedConfiguration())
    environment["DLT_TEST__PASSWORD"] = "PASS"
    C = resolve.resolve_configuration(SectionedConfiguration())
    assert C.password == "PASS"


def test_explicit_sections(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"
    # mock providers separates sections with | and key with -
    _, k = mock_provider.get_value("key", AnyType, None)
    assert k == "-key"
    _, k = mock_provider.get_value("key", AnyType, None, "ns1")
    assert k == "ns1-key"
    _, k = mock_provider.get_value("key", AnyType, None, "ns1", "ns2")
    assert k == "ns1|ns2-key"

    # via make configuration
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration())
    assert mock_provider.last_section == ()
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration(), sections=("ns1",))
    # value is returned only on empty section
    assert mock_provider.last_section == ()
    # always start with more precise section
    assert mock_provider.last_sections == [("ns1",), ()]
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration(), sections=("ns1", "ns2"))
    assert mock_provider.last_sections == [("ns1", "ns2"), ("ns1",), ()]


def test_explicit_sections_with_sectioned_config(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"
    # with sectiond config
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(SectionedConfiguration())
    assert mock_provider.last_section == ("DLT_TEST",)
    # first the native representation of SectionedConfiguration is queried with (), and then the fields in SectionedConfiguration are queried only in DLT_TEST
    assert mock_provider.last_sections == [(), ("DLT_TEST",)]
    # sectioned config is always innermost
    mock_provider.reset_stats()
    resolve.resolve_configuration(SectionedConfiguration(), sections=("ns1",))
    assert mock_provider.last_sections == [("ns1",), (), ("ns1", "DLT_TEST"), ("DLT_TEST",)]
    mock_provider.reset_stats()
    resolve.resolve_configuration(SectionedConfiguration(), sections=("ns1", "ns2"))
    assert mock_provider.last_sections == [
        ("ns1", "ns2"),
        ("ns1",),
        (),
        ("ns1", "ns2", "DLT_TEST"),
        ("ns1", "DLT_TEST"),
        ("DLT_TEST",),
    ]


def test_overwrite_config_section_from_embedded(mock_provider: MockProvider) -> None:
    mock_provider.value = {}
    mock_provider.return_value_on = ("embedded",)
    resolve.resolve_configuration(EmbeddedWithSectionedConfiguration())
    # when resolving the config section DLT_TEST was removed and the embedded section was used instead
    assert mock_provider.last_section == ("embedded",)
    # lookup in order: () - parent config when looking for "embedded", then from "embedded" config
    assert mock_provider.last_sections == [(), ("embedded",)]


def test_explicit_sections_from_embedded_config(mock_provider: MockProvider) -> None:
    mock_provider.value = {"sv": "A"}
    mock_provider.return_value_on = ("sv_config",)
    c = resolve.resolve_configuration(EmbeddedConfiguration())
    # we mock the dictionary below as the value for all requests
    assert c.sv_config.sv == '{"sv":"A"}'
    # following sections were used when resolving EmbeddedConfig:
    # - initial value for the whole embedded sv_config skipped because it does not have section
    # - then ("sv_config",) resolve sv in sv_config
    assert mock_provider.last_sections == [("sv_config",)]
    # embedded section inner of explicit
    mock_provider.reset_stats()
    resolve.resolve_configuration(EmbeddedConfiguration(), sections=("ns1",))
    assert mock_provider.last_sections == [
        (
            "ns1",
            "sv_config",
        ),
        ("sv_config",),
    ]


def test_ignore_embedded_section_by_field_name(mock_provider: MockProvider) -> None:
    mock_provider.value = {"sv": "A"}
    resolve.resolve_configuration(EmbeddedIgnoredConfiguration())
    # _sv_config will not be added to embedded sections and looked up
    assert mock_provider.last_sections == [()]
    mock_provider.reset_stats()
    resolve.resolve_configuration(EmbeddedIgnoredConfiguration(), sections=("ns1",))
    assert mock_provider.last_sections == [("ns1",), ()]
    # if section config exist, it won't be replaced by embedded section
    mock_provider.reset_stats()
    mock_provider.value = {}
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(EmbeddedIgnoredWithSectionedConfiguration())
    assert mock_provider.last_sections == [(), ("DLT_TEST",)]
    # embedded configuration of depth 2: first normal, second - ignored
    mock_provider.reset_stats()
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(EmbeddedWithIgnoredEmbeddedConfiguration())
    assert mock_provider.last_sections == [
        ("ignored_embedded",),
        ("ignored_embedded", "DLT_TEST"),
        ("DLT_TEST",),
    ]


def test_injected_sections(mock_provider: MockProvider) -> None:
    container = Container()
    mock_provider.value = "value"

    with container.injectable_context(ConfigSectionContext(sections=("inj-ns1",))):
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_sections == [("inj-ns1",), ()]
        mock_provider.reset_stats()
        # explicit section preempts injected section
        resolve.resolve_configuration(SingleValConfiguration(), sections=("ns1",))
        assert mock_provider.last_sections == [("ns1",), ()]
        # sectiond config inner of injected
        mock_provider.reset_stats()
        mock_provider.return_value_on = ("DLT_TEST",)
        resolve.resolve_configuration(SectionedConfiguration())
        assert mock_provider.last_sections == [
            ("inj-ns1",),
            (),
            ("inj-ns1", "DLT_TEST"),
            ("DLT_TEST",),
        ]
        # injected section inner of ns coming from embedded config
        mock_provider.reset_stats()
        mock_provider.return_value_on = ()
        mock_provider.value = {"sv": "A"}
        resolve.resolve_configuration(EmbeddedConfiguration())
        assert mock_provider.last_sections == [("inj-ns1", "sv_config"), ("sv_config",)]

    # multiple injected sections
    with container.injectable_context(ConfigSectionContext(sections=("inj-ns1", "inj-ns2"))):
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_sections == [("inj-ns1", "inj-ns2"), ("inj-ns1",), ()]
        mock_provider.reset_stats()


def test_section_context() -> None:
    with pytest.raises(ValueError):
        ConfigSectionContext().source_name()
    with pytest.raises(ValueError):
        ConfigSectionContext(sections=()).source_name()
    with pytest.raises(ValueError):
        ConfigSectionContext(sections=("sources",)).source_name()
    with pytest.raises(ValueError):
        ConfigSectionContext(sections=("sources", "modules")).source_name()

    assert ConfigSectionContext(sections=("sources", "modules", "func")).source_name() == "func"

    # TODO: test merge functions


def test_section_with_pipeline_name(mock_provider: MockProvider) -> None:
    # if pipeline name is present, keys will be looked up twice: with pipeline as top level section and without it

    container = Container()
    mock_provider.value = "value"

    with container.injectable_context(ConfigSectionContext(pipeline_name="PIPE")):
        mock_provider.return_value_on = ()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_sections == [("PIPE",), ()]

        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration(), sections=("ns1",))
        # PIPE section is exhausted then another lookup without PIPE
        assert mock_provider.last_sections == [("PIPE", "ns1"), ("PIPE",), ("ns1",), ()]

        mock_provider.return_value_on = ("PIPE",)
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration(), sections=("ns1",))
        assert mock_provider.last_sections == [("PIPE", "ns1"), ("PIPE",)]

        # with both pipe and config sections are always present in lookup
        # "PIPE", "DLT_TEST"
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        # () will never be searched
        with pytest.raises(ConfigFieldMissingException):
            resolve.resolve_configuration(SectionedConfiguration())
        mock_provider.return_value_on = ("DLT_TEST",)
        mock_provider.reset_stats()
        resolve.resolve_configuration(SectionedConfiguration())
        # first the whole SectionedConfiguration is looked under key DLT_TEST (sections: ('PIPE',), ()), then fields of SectionedConfiguration
        assert mock_provider.last_sections == [("PIPE",), (), ("PIPE", "DLT_TEST"), ("DLT_TEST",)]

    # with pipeline and injected sections
    with container.injectable_context(
        ConfigSectionContext(pipeline_name="PIPE", sections=("inj-ns1",))
    ):
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_sections == [("PIPE", "inj-ns1"), ("PIPE",), ("inj-ns1",), ()]


# def test_sections_with_duplicate(mock_provider: MockProvider) -> None:
#     container = Container()
#     mock_provider.value = "value"

#     with container.injectable_context(ConfigNamespacesContext(pipeline_name="DLT_TEST", sections=("DLT_TEST", "DLT_TEST"))):
#         mock_provider.return_value_on = ("DLT_TEST",)
#         resolve.resolve_configuration(SectionedConfiguration(), sections=("DLT_TEST", "DLT_TEST"))
#         # no duplicates are removed, duplicates are misconfiguration
#         # note: use dict.fromkeys to create ordered sets from lists if we ever want to remove duplicates
#         # the lookup tuples are create as follows:
#         # 1. (pipeline name, deduplicated sections, config section)
#         # 2. (deduplicated sections, config section)
#         # 3. (pipeline name, config section)
#         # 4. (config section)
#         assert mock_provider.last_sections == [("DLT_TEST", "DLT_TEST", "DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST"), ("DLT_TEST",)]


def test_inject_section(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"

    with inject_section(ConfigSectionContext(pipeline_name="PIPE", sections=("inj-ns1",))):
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_sections == [("PIPE", "inj-ns1"), ("PIPE",), ("inj-ns1",), ()]

        # inject with merge previous
        with inject_section(ConfigSectionContext(sections=("inj-ns2",))):
            mock_provider.reset_stats()
            resolve.resolve_configuration(SingleValConfiguration())
            assert mock_provider.last_sections == [("PIPE", "inj-ns2"), ("PIPE",), ("inj-ns2",), ()]

            # inject without merge
            mock_provider.reset_stats()
            with inject_section(ConfigSectionContext(), merge_existing=False):
                resolve.resolve_configuration(SingleValConfiguration())
                assert mock_provider.last_sections == [()]
