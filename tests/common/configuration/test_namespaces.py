import pytest
from typing import Any, Optional
from dlt.common.configuration.container import Container

from dlt.common.configuration import configspec, ConfigFieldMissingException, resolve, inject_namespace
from dlt.common.configuration.specs import BaseConfiguration, ConfigNamespacesContext
from dlt.common.configuration.exceptions import LookupTrace

from tests.utils import preserve_environ
from tests.common.configuration.utils import MockProvider, WrongConfiguration, SecretConfiguration, NamespacedConfiguration, environment, mock_provider


@configspec
class SingleValConfiguration(BaseConfiguration):
    sv: str


@configspec
class EmbeddedConfiguration(BaseConfiguration):
    sv_config: Optional[SingleValConfiguration]


@configspec
class EmbeddedWithNamespacedConfiguration(BaseConfiguration):
    embedded: NamespacedConfiguration


@configspec
class EmbeddedIgnoredConfiguration(BaseConfiguration):
    # underscore prevents the field name to be added to embedded namespaces
    _sv_config: Optional[SingleValConfiguration]


@configspec
class EmbeddedIgnoredWithNamespacedConfiguration(BaseConfiguration):
    _embedded: NamespacedConfiguration


@configspec
class EmbeddedWithIgnoredEmbeddedConfiguration(BaseConfiguration):
    ignored_embedded: EmbeddedIgnoredWithNamespacedConfiguration


def test_namespaced_configuration(environment: Any) -> None:
    with pytest.raises(ConfigFieldMissingException) as exc_val:
        resolve.resolve_configuration(NamespacedConfiguration())

    assert list(exc_val.value.traces.keys()) == ["password"]
    assert exc_val.value.spec_name == "NamespacedConfiguration"
    # check trace
    traces = exc_val.value.traces["password"]
    # only one provider and namespace was tried
    assert len(traces) == 3
    assert traces[0] == LookupTrace("Environment Variables", ["DLT_TEST"], "DLT_TEST__PASSWORD", None)
    assert traces[1] == LookupTrace("Pipeline secrets.toml", ["DLT_TEST"], "DLT_TEST.password", None)
    assert traces[2] == LookupTrace("Pipeline config.toml", ["DLT_TEST"], "DLT_TEST.password", None)

    # init vars work without namespace
    C = resolve.resolve_configuration(NamespacedConfiguration(), initial_value={"password": "PASS"})
    assert C.password == "PASS"

    # env var must be prefixed
    environment["PASSWORD"] = "PASS"
    with pytest.raises(ConfigFieldMissingException) as exc_val:
        resolve.resolve_configuration(NamespacedConfiguration())
    environment["DLT_TEST__PASSWORD"] = "PASS"
    C = resolve.resolve_configuration(NamespacedConfiguration())
    assert C.password == "PASS"


def test_explicit_namespaces(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"
    # mock providers separates namespaces with | and key with -
    _, k = mock_provider.get_value("key", Any)
    assert k == "-key"
    _, k = mock_provider.get_value("key", Any, "ns1")
    assert k == "ns1-key"
    _, k = mock_provider.get_value("key", Any, "ns1", "ns2")
    assert k == "ns1|ns2-key"

    # via make configuration
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration())
    assert mock_provider.last_namespace == ()
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration(), namespaces=("ns1",))
    # value is returned only on empty namespace
    assert mock_provider.last_namespace == ()
    # always start with more precise namespace
    assert mock_provider.last_namespaces == [("ns1",), ()]
    mock_provider.reset_stats()
    resolve.resolve_configuration(SingleValConfiguration(), namespaces=("ns1", "ns2"))
    assert mock_provider.last_namespaces == [("ns1", "ns2"), ("ns1",), ()]


def test_explicit_namespaces_with_namespaced_config(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"
    # with namespaced config
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(NamespacedConfiguration())
    assert mock_provider.last_namespace == ("DLT_TEST",)
    # first the native representation of NamespacedConfiguration is queried with (), and then the fields in NamespacedConfiguration are queried only in DLT_TEST
    assert mock_provider.last_namespaces == [(), ("DLT_TEST",)]
    # namespaced config is always innermost
    mock_provider.reset_stats()
    resolve.resolve_configuration(NamespacedConfiguration(), namespaces=("ns1",))
    assert mock_provider.last_namespaces == [("ns1",), (), ("ns1", "DLT_TEST"), ("DLT_TEST",)]
    mock_provider.reset_stats()
    resolve.resolve_configuration(NamespacedConfiguration(), namespaces=("ns1", "ns2"))
    assert mock_provider.last_namespaces == [("ns1", "ns2"), ("ns1",), (), ("ns1", "ns2", "DLT_TEST"), ("ns1", "DLT_TEST"), ("DLT_TEST",)]


def test_overwrite_config_namespace_from_embedded(mock_provider: MockProvider) -> None:
    mock_provider.value = {}
    mock_provider.return_value_on = ("embedded",)
    resolve.resolve_configuration(EmbeddedWithNamespacedConfiguration())
    # when resolving the config namespace DLT_TEST was removed and the embedded namespace was used instead
    assert mock_provider.last_namespace == ("embedded",)
    # lookup in order: () - parent config when looking for "embedded", then from "embedded" config
    assert mock_provider.last_namespaces == [(), ("embedded",)]


def test_explicit_namespaces_from_embedded_config(mock_provider: MockProvider) -> None:
    mock_provider.value = {"sv": "A"}
    mock_provider.return_value_on = ("sv_config",)
    c = resolve.resolve_configuration(EmbeddedConfiguration())
    # we mock the dictionary below as the value for all requests
    assert c.sv_config.sv == '{"sv": "A"}'
    # following namespaces were used when resolving EmbeddedConfig: () trying to get initial value for the whole embedded sv_config, then ("sv_config",), () to resolve sv in sv_config
    assert mock_provider.last_namespaces == [(), ("sv_config",)]
    # embedded namespace inner of explicit
    mock_provider.reset_stats()
    resolve.resolve_configuration(EmbeddedConfiguration(), namespaces=("ns1",))
    assert mock_provider.last_namespaces == [("ns1",), (), ("ns1", "sv_config",), ("sv_config",)]


def test_ignore_embedded_namespace_by_field_name(mock_provider: MockProvider) -> None:
    mock_provider.value = {"sv": "A"}
    resolve.resolve_configuration(EmbeddedIgnoredConfiguration())
    # _sv_config will not be added to embedded namespaces and looked up
    assert mock_provider.last_namespaces == [()]
    mock_provider.reset_stats()
    resolve.resolve_configuration(EmbeddedIgnoredConfiguration(), namespaces=("ns1",))
    assert mock_provider.last_namespaces == [("ns1",), ()]
    # if namespace config exist, it won't be replaced by embedded namespace
    mock_provider.reset_stats()
    mock_provider.value = {}
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(EmbeddedIgnoredWithNamespacedConfiguration())
    assert mock_provider.last_namespaces == [(), ("DLT_TEST",)]
    # embedded configuration of depth 2: first normal, second - ignored
    mock_provider.reset_stats()
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.resolve_configuration(EmbeddedWithIgnoredEmbeddedConfiguration())
    assert mock_provider.last_namespaces == [(), ('ignored_embedded',), ('ignored_embedded', 'DLT_TEST'), ('DLT_TEST',)]


def test_injected_namespaces(mock_provider: MockProvider) -> None:
    container = Container()
    mock_provider.value = "value"

    with container.injectable_context(ConfigNamespacesContext(namespaces=("inj-ns1",))):
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("inj-ns1",), ()]
        mock_provider.reset_stats()
        # explicit namespace preempts injected namespace
        resolve.resolve_configuration(SingleValConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("ns1",), ()]
        # namespaced config inner of injected
        mock_provider.reset_stats()
        mock_provider.return_value_on = ("DLT_TEST",)
        resolve.resolve_configuration(NamespacedConfiguration())
        assert mock_provider.last_namespaces == [("inj-ns1",), (), ("inj-ns1", "DLT_TEST"), ("DLT_TEST",)]
        # injected namespace inner of ns coming from embedded config
        mock_provider.reset_stats()
        mock_provider.return_value_on = ()
        mock_provider.value = {"sv": "A"}
        resolve.resolve_configuration(EmbeddedConfiguration())
        # first we look for sv_config -> ("inj-ns1",), () then we look for sv
        assert mock_provider.last_namespaces == [("inj-ns1",), (), ("inj-ns1", "sv_config"), ("sv_config",)]

    # multiple injected namespaces
    with container.injectable_context(ConfigNamespacesContext(namespaces=("inj-ns1", "inj-ns2"))):
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("inj-ns1", "inj-ns2"), ("inj-ns1",), ()]
        mock_provider.reset_stats()


def test_namespace_with_pipeline_name(mock_provider: MockProvider) -> None:
    # if pipeline name is present, keys will be looked up twice: with pipeline as top level namespace and without it

    container = Container()
    mock_provider.value = "value"

    with container.injectable_context(ConfigNamespacesContext(pipeline_name="PIPE")):
        mock_provider.return_value_on = ()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("PIPE",), ()]

        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration(), namespaces=("ns1",))
        # PIPE namespace is exhausted then another lookup without PIPE
        assert mock_provider.last_namespaces == [("PIPE", "ns1"), ("PIPE",), ("ns1",), ()]

        mock_provider.return_value_on = ("PIPE", )
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("PIPE", "ns1"), ("PIPE",)]

        # with both pipe and config namespaces are always present in lookup
        # "PIPE", "DLT_TEST"
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        # () will never be searched
        with pytest.raises(ConfigFieldMissingException):
            resolve.resolve_configuration(NamespacedConfiguration())
        mock_provider.return_value_on = ("DLT_TEST",)
        mock_provider.reset_stats()
        resolve.resolve_configuration(NamespacedConfiguration())
        # first the whole NamespacedConfiguration is looked under key DLT_TEST (namespaces: ('PIPE',), ()), then fields of NamespacedConfiguration
        assert mock_provider.last_namespaces == [('PIPE',), (), ("PIPE", "DLT_TEST"), ("DLT_TEST",)]

    # with pipeline and injected namespaces
    with container.injectable_context(ConfigNamespacesContext(pipeline_name="PIPE", namespaces=("inj-ns1",))):
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("PIPE", "inj-ns1"), ("PIPE",), ("inj-ns1",), ()]


# def test_namespaces_with_duplicate(mock_provider: MockProvider) -> None:
#     container = Container()
#     mock_provider.value = "value"

#     with container.injectable_context(ConfigNamespacesContext(pipeline_name="DLT_TEST", namespaces=("DLT_TEST", "DLT_TEST"))):
#         mock_provider.return_value_on = ("DLT_TEST",)
#         resolve.resolve_configuration(NamespacedConfiguration(), namespaces=("DLT_TEST", "DLT_TEST"))
#         # no duplicates are removed, duplicates are misconfiguration
#         # note: use dict.fromkeys to create ordered sets from lists if we ever want to remove duplicates
#         # the lookup tuples are create as follows:
#         # 1. (pipeline name, deduplicated namespaces, config namespace)
#         # 2. (deduplicated namespaces, config namespace)
#         # 3. (pipeline name, config namespace)
#         # 4. (config namespace)
#         assert mock_provider.last_namespaces == [("DLT_TEST", "DLT_TEST", "DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST"), ("DLT_TEST", "DLT_TEST"), ("DLT_TEST",)]


def test_inject_namespace(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"

    with inject_namespace(ConfigNamespacesContext(pipeline_name="PIPE", namespaces=("inj-ns1",))):
        resolve.resolve_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("PIPE", "inj-ns1"), ("PIPE",), ("inj-ns1",), ()]

        # inject with merge previous
        with inject_namespace(ConfigNamespacesContext(namespaces=("inj-ns2",))):
            mock_provider.reset_stats()
            resolve.resolve_configuration(SingleValConfiguration())
            assert mock_provider.last_namespaces == [("PIPE", "inj-ns2"), ("PIPE",), ("inj-ns2",), ()]

            # inject without merge
            mock_provider.reset_stats()
            with inject_namespace(ConfigNamespacesContext(), merge_existing=False):
                resolve.resolve_configuration(SingleValConfiguration())
                assert mock_provider.last_namespaces == [()]
