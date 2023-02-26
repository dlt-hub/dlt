import pytest
from typing import Any, ClassVar, Literal, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.providers.context import ContextProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import BaseConfiguration, ContainerInjectableContext
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ConfigFieldMissingException, ContainerInjectableContextMangled, ContextDefaultCannotBeCreated

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


@configspec(init=True)
class InjectableTestContext(ContainerInjectableContext):
    current_value: str

    def parse_native_representation(self, native_value: Any) -> None:
        raise ValueError(native_value)


@configspec
class EmbeddedWithInjectableContext(BaseConfiguration):
    injected: InjectableTestContext


@configspec
class NoDefaultInjectableContext(ContainerInjectableContext):

    can_create_default: ClassVar[bool] = False


@configspec
class EmbeddedWithNoDefaultInjectableContext(BaseConfiguration):
    injected: NoDefaultInjectableContext


@configspec
class EmbeddedWithNoDefaultInjectableOptionalContext(BaseConfiguration):
    injected: Optional[NoDefaultInjectableContext]


@pytest.fixture()
def container() -> Container:
    container = Container._INSTANCE
    # erase singleton
    Container._INSTANCE = None
    yield Container()
    # restore the old container
    Container._INSTANCE = container


def test_singleton(container: Container) -> None:
    # keep the old configurations list
    container_configurations = container.contexts

    singleton = Container()
    # make sure it is the same object
    assert container is singleton
    # that holds the same configurations dictionary
    assert container_configurations is singleton.contexts


def test_container_items(container: Container) -> None:
    # will add InjectableTestContext instance to container
    container[InjectableTestContext]
    assert InjectableTestContext in container
    del container[InjectableTestContext]
    assert InjectableTestContext not in container
    container[InjectableTestContext] = InjectableTestContext(current_value="S")
    assert container[InjectableTestContext].current_value == "S"
    container[InjectableTestContext] = InjectableTestContext(current_value="SS")
    assert container[InjectableTestContext].current_value == "SS"


def test_get_default_injectable_config(container: Container) -> None:
    injectable = container[InjectableTestContext]
    assert injectable.current_value is None
    assert isinstance(injectable, InjectableTestContext)


def test_raise_on_no_default_value(container: Container) -> None:
    with pytest.raises(ContextDefaultCannotBeCreated):
        container[NoDefaultInjectableContext]

    # ok when injected
    with container.injectable_context(NoDefaultInjectableContext()) as injected:
        assert container[NoDefaultInjectableContext] is injected


def test_container_injectable_context(container: Container) -> None:
    with container.injectable_context(InjectableTestContext()) as current_config:
        assert current_config.current_value is None
        current_config.current_value = "TEST"
        assert container[InjectableTestContext].current_value == "TEST"
        assert container[InjectableTestContext] is current_config

    assert InjectableTestContext not in container


def test_container_injectable_context_restore(container: Container) -> None:
    # this will create InjectableTestConfiguration
    original = container[InjectableTestContext]
    original.current_value = "ORIGINAL"
    with container.injectable_context(InjectableTestContext()) as current_config:
        current_config.current_value = "TEST"
        # nested context is supported
        with container.injectable_context(InjectableTestContext()) as inner_config:
            assert inner_config.current_value is None
            assert container[InjectableTestContext] is inner_config
        assert container[InjectableTestContext] is current_config

    assert container[InjectableTestContext] is original
    assert container[InjectableTestContext].current_value == "ORIGINAL"


def test_container_injectable_context_mangled(container: Container) -> None:
    original = container[InjectableTestContext]
    original.current_value = "ORIGINAL"

    context = InjectableTestContext()
    with pytest.raises(ContainerInjectableContextMangled) as py_ex:
        with container.injectable_context(context) as current_config:
            current_config.current_value = "TEST"
            # overwrite the config in container
            container[InjectableTestContext] = InjectableTestContext()
    assert py_ex.value.spec == InjectableTestContext
    assert py_ex.value.expected_config == context


def test_container_provider(container: Container) -> None:
    provider = ContextProvider()
    # default value will be created
    v, k = provider.get_value("n/a", InjectableTestContext)
    assert isinstance(v, InjectableTestContext)
    assert k == "InjectableTestContext"
    assert InjectableTestContext in container

    # provider does not create default value in Container
    v, k = provider.get_value("n/a", NoDefaultInjectableContext)
    assert v is None
    assert NoDefaultInjectableContext not in container

    # explicitly create value
    original = NoDefaultInjectableContext()
    container[NoDefaultInjectableContext] = original
    v, _ = provider.get_value("n/a", NoDefaultInjectableContext)
    assert v is original

    # must assert if sections are provided
    with pytest.raises(AssertionError):
        provider.get_value("n/a", InjectableTestContext, ("ns1",))

    # type hints that are not classes
    literal = Literal["a"]
    v, k = provider.get_value("n/a", literal)
    assert v is None
    assert k == "typing.Literal['a']"


def test_container_provider_embedded_inject(container: Container, environment: Any) -> None:
    environment["INJECTED"] = "unparsable"
    with container.injectable_context(InjectableTestContext(current_value="Embed")) as injected:
        # must have top precedence - over the environ provider. environ provider is returning a value that will cannot be parsed
        # but the container provider has a precedence and the lookup in environ provider will never happen
        C = resolve_configuration(EmbeddedWithInjectableContext())
        assert C.injected.current_value == "Embed"
        assert C.injected is injected


def test_container_provider_embedded_no_default(container: Container) -> None:
    with container.injectable_context(NoDefaultInjectableContext()):
        resolve_configuration(EmbeddedWithNoDefaultInjectableContext())
    # default cannot be created so fails
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(EmbeddedWithNoDefaultInjectableContext())
    assert py_ex.value.fields == ["injected"]
    # optional returns none
    c = resolve_configuration(EmbeddedWithNoDefaultInjectableOptionalContext())
    assert c.injected is None
