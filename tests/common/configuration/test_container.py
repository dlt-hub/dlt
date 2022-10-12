from typing import Any
import pytest

from dlt.common.configuration import configspec
from dlt.common.configuration.providers.container import ContainerProvider
from dlt.common.configuration.resolve import make_configuration
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration.container import Container, ContainerInjectableConfiguration
from dlt.common.configuration.exceptions import ContainerInjectableConfigurationMangled, InvalidInitialValue
from dlt.common.configuration.specs.config_providers_configuration import ConfigProvidersListConfiguration

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


@configspec(init=True)
class InjectableTestConfiguration(ContainerInjectableConfiguration):
    current_value: str


@configspec
class EmbeddedWithInjectableConfiguration(BaseConfiguration):
    injected: InjectableTestConfiguration


@pytest.fixture()
def container() -> Container:
    # erase singleton
    Container._INSTANCE = None
    return Container()


def test_singleton(container: Container) -> None:
    # keep the old configurations list
    container_configurations = container.configurations

    singleton = Container()
    # make sure it is the same object
    assert container is singleton
    # that holds the same configurations dictionary
    assert container_configurations is singleton.configurations


def test_get_default_injectable_config(container: Container) -> None:
    pass


def test_container_injectable_context(container: Container) -> None:
    with container.injectable_configuration(InjectableTestConfiguration()) as current_config:
        assert current_config.current_value is None
        current_config.current_value = "TEST"
        assert container[InjectableTestConfiguration].current_value == "TEST"
        assert container[InjectableTestConfiguration] is current_config

    assert InjectableTestConfiguration not in container


def test_container_injectable_context_restore(container: Container) -> None:
    # this will create InjectableTestConfiguration
    original = container[InjectableTestConfiguration]
    original.current_value = "ORIGINAL"
    with container.injectable_configuration(InjectableTestConfiguration()) as current_config:
        current_config.current_value = "TEST"
        # nested context is supported
        with container.injectable_configuration(InjectableTestConfiguration()) as inner_config:
            assert inner_config.current_value is None
            assert container[InjectableTestConfiguration] is inner_config
        assert container[InjectableTestConfiguration] is current_config

    assert container[InjectableTestConfiguration] is original
    assert container[InjectableTestConfiguration].current_value == "ORIGINAL"


def test_container_injectable_context_mangled(container: Container) -> None:
    original = container[InjectableTestConfiguration]
    original.current_value = "ORIGINAL"

    injectable = InjectableTestConfiguration()
    with pytest.raises(ContainerInjectableConfigurationMangled) as py_ex:
        with container.injectable_configuration(injectable) as current_config:
            current_config.current_value = "TEST"
            # overwrite the config in container
            container.configurations[InjectableTestConfiguration] = InjectableTestConfiguration()
    assert py_ex.value.spec == InjectableTestConfiguration
    assert py_ex.value.expected_config == injectable


def test_container_provider(container: Container) -> None:
    provider = ContainerProvider()
    v, k = provider.get_value("n/a", InjectableTestConfiguration)
    # provider does not create default value in Container
    assert v is None
    assert k == str(InjectableTestConfiguration)
    assert InjectableTestConfiguration not in container

    original = container[InjectableTestConfiguration]
    original.current_value = "ORIGINAL"
    v, _ = provider.get_value("n/a", InjectableTestConfiguration)
    assert v is original

    # must assert if namespaces are provided
    with pytest.raises(AssertionError):
        provider.get_value("n/a", InjectableTestConfiguration, ("ns1",))


def test_container_provider_embedded_inject(container: Container, environment: Any) -> None:
    environment["INJECTED"] = "unparsable"
    with container.injectable_configuration(InjectableTestConfiguration(current_value="Embed")) as injected:
        # must have top precedence - over the environ provider. environ provider is returning a value that will cannot be parsed
        # but the container provider has a precedence and the lookup in environ provider will never happen
        C = make_configuration(EmbeddedWithInjectableConfiguration())
        assert C.injected.current_value == "Embed"
        assert C.injected is injected
        # remove first provider
        container[ConfigProvidersListConfiguration].providers.pop(0)
        # now environment will provide unparsable value
        with pytest.raises(InvalidInitialValue):
            C = make_configuration(EmbeddedWithInjectableConfiguration())
