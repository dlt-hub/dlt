from unittest import mock
import pytest
from typing import Any, Optional
from dlt.common.configuration.container import Container

from dlt.common.typing import TSecretValue
from dlt.common.configuration import configspec, ConfigEntryMissingException, ConfigFileNotFoundException, resolve
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration.providers import environ as environ_provider
from dlt.common.configuration.exceptions import LookupTrace

from tests.utils import preserve_environ
from tests.common.configuration.utils import MockProvider, WrongConfiguration, SecretConfiguration, NamespacedConfiguration, environment, mock_provider


@configspec
class SingleValConfiguration(BaseConfiguration):
    sv: str


@configspec
class EmbeddedConfiguration(BaseConfiguration):
    sv_config: Optional[SingleValConfiguration]


def test_namespaced_configuration(environment: Any) -> None:
    with pytest.raises(ConfigEntryMissingException) as exc_val:
        resolve.make_configuration(NamespacedConfiguration())
    assert list(exc_val.value.traces.keys()) == ["password"]
    assert exc_val.value.spec_name == "NamespacedConfiguration"
    # check trace
    traces = exc_val.value.traces["password"]
    # only one provider and namespace was tried
    assert len(traces) == 1
    assert traces[0] == LookupTrace("Environment Variables", ["DLT_TEST"], "DLT_TEST__PASSWORD", None)

    # init vars work without namespace
    C = resolve.make_configuration(NamespacedConfiguration(), initial_value={"password": "PASS"})
    assert C.password == "PASS"

    # env var must be prefixed
    environment["PASSWORD"] = "PASS"
    with pytest.raises(ConfigEntryMissingException) as exc_val:
        resolve.make_configuration(NamespacedConfiguration())
    environment["DLT_TEST__PASSWORD"] = "PASS"
    C = resolve.make_configuration(NamespacedConfiguration())
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
    resolve.make_configuration(SingleValConfiguration())
    assert mock_provider.last_namespace == ()
    mock_provider.reset_stats()
    resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1",))
    # value is returned only on empty namespace
    assert mock_provider.last_namespace == ()
    # always start with more precise namespace
    assert mock_provider.last_namespaces == [("ns1",), ()]
    mock_provider.reset_stats()
    resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1", "ns2"))
    assert mock_provider.last_namespaces == [("ns1", "ns2"), ("ns1",), ()]


def test_explicit_namespaces_with_namespaced_config(mock_provider: MockProvider) -> None:
    mock_provider.value = "value"
    # with namespaced config
    mock_provider.return_value_on = ("DLT_TEST",)
    resolve.make_configuration(NamespacedConfiguration())
    assert mock_provider.last_namespace == ("DLT_TEST",)
    # namespace from config is mandatory, provider will not be queried with ()
    assert mock_provider.last_namespaces == [("DLT_TEST",)]
    # namespaced config is always innermost
    mock_provider.reset_stats()
    resolve.make_configuration(NamespacedConfiguration(), namespaces=("ns1",))
    assert mock_provider.last_namespaces == [("ns1", "DLT_TEST"), ("DLT_TEST",)]
    mock_provider.reset_stats()
    resolve.make_configuration(NamespacedConfiguration(), namespaces=("ns1", "ns2"))
    assert mock_provider.last_namespaces == [("ns1", "ns2", "DLT_TEST"), ("ns1", "DLT_TEST"), ("DLT_TEST",)]


def test_explicit_namespaces_from_embedded_config(mock_provider: MockProvider) -> None:
    mock_provider.value = {"sv": "A"}
    C = resolve.make_configuration(EmbeddedConfiguration())
    # we mock the dictionary below as the value for all requests
    assert C.sv_config.sv == '{"sv": "A"}'
    # following namespaces were used when resolving EmbeddedConfig: () - to resolve sv_config and then: ("sv_config",), () to resolve sv in sv_config
    assert mock_provider.last_namespaces == [(), ("sv_config",), ()]
    # embedded namespace inner of explicit
    mock_provider.reset_stats()
    C = resolve.make_configuration(EmbeddedConfiguration(), namespaces=("ns1",))
    assert mock_provider.last_namespaces == [("ns1",), (), ("ns1", "sv_config",), ("ns1",), ()]


def test_injected_namespaces(mock_provider: MockProvider) -> None:
    container = Container()
    mock_provider.value = "value"

    with container.injectable_configuration(resolve.ConfigNamespacesConfiguration(namespaces=("inj-ns1",))):
        resolve.make_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("inj-ns1",), ()]
        mock_provider.reset_stats()
        # explicit namespace inner of injected
        resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("inj-ns1", "ns1"), ("inj-ns1",), ()]
        # namespaced config inner of injected
        mock_provider.reset_stats()
        mock_provider.return_value_on = ("DLT_TEST",)
        resolve.make_configuration(NamespacedConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("inj-ns1", "ns1", "DLT_TEST"), ("inj-ns1", "DLT_TEST"), ("DLT_TEST",)]
        # explicit namespace inner of ns coming from embedded config
        mock_provider.reset_stats()
        mock_provider.return_value_on = ()
        mock_provider.value = {"sv": "A"}
        resolve.make_configuration(EmbeddedConfiguration(), namespaces=("ns1",))
        # first we look for sv_config -> ("inj-ns1", "ns1"), ("inj-ns1",), () then we look for sv
        assert mock_provider.last_namespaces == [("inj-ns1", "ns1"), ("inj-ns1",), (), ("inj-ns1", "ns1", "sv_config"), ("inj-ns1", "ns1"), ("inj-ns1",), ()]

    # multiple injected namespaces
    with container.injectable_configuration(resolve.ConfigNamespacesConfiguration(namespaces=("inj-ns1", "inj-ns2"))):
        mock_provider.reset_stats()
        resolve.make_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("inj-ns1", "inj-ns2"), ("inj-ns1",), ()]
        mock_provider.reset_stats()
        # explicit namespace inner of injected
        resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("inj-ns1", "inj-ns2", "ns1"), ("inj-ns1", "inj-ns2"), ("inj-ns1",), ()]


def test_namespace_from_pipeline_name(mock_provider: MockProvider) -> None:
    # AXIES__DESTINATION__STORAGE_CREDENTIALS__PRIVATE_KEY, DESTINATION__STORAGE_CREDENTIALS__PRIVATE_KEY, DESTINATION__PRIVATE_KEY, GCP__PRIVATE_KEY
    # if pipeline name is present, keys will be looked up twice: with pipeline as top level namespace and without it

    container = Container()
    mock_provider.value = "value"

    with container.injectable_configuration(resolve.ConfigNamespacesConfiguration(pipeline_name="PIPE")):
        mock_provider.return_value_on = ()
        resolve.make_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("PIPE",), ()]

        mock_provider.reset_stats()
        resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1",))
        # PIPE namespace is exhausted then another lookup without PIPE
        assert mock_provider.last_namespaces == [("PIPE", "ns1"), ("PIPE",), ("ns1",), ()]

        mock_provider.return_value_on = ("PIPE", )
        mock_provider.reset_stats()
        resolve.make_configuration(SingleValConfiguration(), namespaces=("ns1",))
        assert mock_provider.last_namespaces == [("PIPE", "ns1"), ("PIPE",)]

        # with both pipe and config namespaces are always present in lookup
        # "PIPE", "DLT_TEST"
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        # () will never be searched
        with pytest.raises(ConfigEntryMissingException):
            resolve.make_configuration(NamespacedConfiguration())
        mock_provider.return_value_on = ("DLT_TEST",)
        mock_provider.reset_stats()
        resolve.make_configuration(NamespacedConfiguration())
        assert mock_provider.last_namespaces == [("PIPE", "DLT_TEST"), ("DLT_TEST",)]

    # with pipeline and injected namespaces
    with container.injectable_configuration(resolve.ConfigNamespacesConfiguration(pipeline_name="PIPE", namespaces=("inj-ns1",))):
        mock_provider.return_value_on = ()
        mock_provider.reset_stats()
        resolve.make_configuration(SingleValConfiguration())
        assert mock_provider.last_namespaces == [("PIPE", "inj-ns1"), ("PIPE",), ("inj-ns1",), ()]
