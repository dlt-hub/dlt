from concurrent.futures import ThreadPoolExecutor
import pytest
import threading
from typing import Any, ClassVar, Literal, Optional, Iterator, Type, TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.configuration.providers.context import ContextProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import BaseConfiguration, ContainerInjectableContext
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingException,
    ContainerInjectableContextMangled,
    ContextDefaultCannotBeCreated,
)

from tests.utils import preserve_environ
from tests.common.configuration.utils import environment


@configspec
class InjectableTestContext(ContainerInjectableContext):
    current_value: str = None

    def parse_native_representation(self, native_value: Any) -> None:
        raise ValueError(native_value)


@configspec
class EmbeddedWithInjectableContext(BaseConfiguration):
    injected: InjectableTestContext = None


@configspec
class NoDefaultInjectableContext(ContainerInjectableContext):
    can_create_default: ClassVar[bool] = False


@configspec
class GlobalTestContext(InjectableTestContext):
    global_affinity: ClassVar[bool] = True


@configspec
class EmbeddedWithNoDefaultInjectableContext(BaseConfiguration):
    injected: NoDefaultInjectableContext = None


@configspec
class EmbeddedWithNoDefaultInjectableOptionalContext(BaseConfiguration):
    injected: Optional[NoDefaultInjectableContext] = None


@pytest.fixture()
def container() -> Iterator[Container]:
    container = Container._INSTANCE
    # erase singleton
    Container._INSTANCE = None
    yield Container()
    # restore the old container
    Container._INSTANCE = container


def test_singleton(container: Container) -> None:
    # keep the old configurations list
    container_configurations = container.thread_contexts

    singleton = Container()
    # make sure it is the same object
    assert container is singleton
    # that holds the same configurations dictionary
    assert container_configurations is singleton.thread_contexts


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_items(container: Container, spec: Type[InjectableTestContext]) -> None:
    # will add InjectableTestContext instance to container
    container[spec]
    assert spec in container
    del container[spec]
    assert spec not in container

    inst_s = spec(current_value="S")
    # make sure that spec knows it is in the container
    assert inst_s.in_container is False
    container[spec] = inst_s
    assert inst_s.in_container is True
    assert container[spec].current_value == "S"

    inst_ss = spec(current_value="SS")
    container[spec] = inst_ss
    assert container[spec].current_value == "SS"

    # inst_s out of container
    assert inst_s.in_container is False
    assert inst_ss.in_container is True
    del container[spec]
    assert inst_s.in_container is False
    assert inst_ss.in_container is False


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


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_injectable_context(
    container: Container, spec: Type[InjectableTestContext]
) -> None:
    with container.injectable_context(InjectableTestContext()) as current_config:
        assert current_config.current_value is None
        current_config.current_value = "TEST"
        assert container[InjectableTestContext].current_value == "TEST"
        assert container[InjectableTestContext] is current_config

    assert InjectableTestContext not in container


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_injectable_context_restore(
    container: Container, spec: Type[InjectableTestContext]
) -> None:
    # this will create InjectableTestConfiguration
    original = container[spec]
    original.current_value = "ORIGINAL"
    with container.injectable_context(spec()) as current_config:
        current_config.current_value = "TEST"
        # nested context is supported
        with container.injectable_context(spec()) as inner_config:
            assert inner_config.current_value is None
            assert container[spec] is inner_config
        assert container[spec] is current_config

    assert container[spec] is original
    assert container[spec].current_value == "ORIGINAL"


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_injectable_context_mangled(
    container: Container, spec: Type[InjectableTestContext]
) -> None:
    original = container[spec]
    original.current_value = "ORIGINAL"

    context = spec()
    with pytest.raises(ContainerInjectableContextMangled) as py_ex:
        with container.injectable_context(context) as current_config:
            current_config.current_value = "TEST"
            # overwrite the config in container
            container[spec] = spec()
    assert py_ex.value.spec == spec
    assert py_ex.value.expected_config == context


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_thread_affinity(container: Container, spec: Type[InjectableTestContext]) -> None:
    event = threading.Semaphore(0)
    thread_item: InjectableTestContext = None

    def _thread() -> None:
        container[spec] = spec(current_value="THREAD")
        event.release()
        event.acquire()
        nonlocal thread_item
        thread_item = container[spec]
        event.release()

    threading.Thread(target=_thread, daemon=True).start()
    event.acquire()
    # it may be or separate copy (InjectableTestContext) or single copy (GlobalTestContext)
    main_item = container[spec]
    main_item.current_value = "MAIN"
    event.release()
    main_item = container[spec]
    event.release()
    if spec is GlobalTestContext:
        # just one context is kept globally
        assert main_item is thread_item
        # MAIN was set after thread
        assert thread_item.current_value == "MAIN"
    else:
        assert main_item is not thread_item
        assert main_item.current_value == "MAIN"
        assert thread_item.current_value == "THREAD"


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_pool_affinity(container: Container, spec: Type[InjectableTestContext]) -> None:
    event = threading.Semaphore(0)
    thread_item: InjectableTestContext = None

    def _thread() -> None:
        container[spec] = spec(current_value="THREAD")
        event.release()
        event.acquire()
        nonlocal thread_item
        thread_item = container[spec]
        event.release()

    threading.Thread(target=_thread, daemon=True, name=Container.thread_pool_prefix()).start()
    event.acquire()
    # it may be or separate copy (InjectableTestContext) or single copy (GlobalTestContext)
    main_item = container[spec]
    main_item.current_value = "MAIN"
    event.release()
    main_item = container[spec]
    event.release()

    # just one context is kept globally - Container user pool thread name to get the starting thread id
    # and uses it to retrieve context
    assert main_item is thread_item
    # MAIN was set after thread
    assert thread_item.current_value == "MAIN"


def test_thread_pool_affinity(container: Container) -> None:
    def _context() -> InjectableTestContext:
        return container[InjectableTestContext]

    main_item = container[InjectableTestContext] = InjectableTestContext(current_value="MAIN")

    with ThreadPoolExecutor(thread_name_prefix=container.thread_pool_prefix()) as p:
        future = p.submit(_context)
        item = future.result()

    assert item is main_item

    # create non affine pool
    with ThreadPoolExecutor() as p:
        future = p.submit(_context)
        item = future.result()

    assert item is not main_item


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_provider(container: Container, spec: Type[InjectableTestContext]) -> None:
    provider = ContextProvider()
    # default value will be created
    v, k = provider.get_value("n/a", spec, None)
    assert isinstance(v, spec)
    assert k == spec.__name__
    assert spec in container

    # provider does not create default value in Container
    v, k = provider.get_value("n/a", NoDefaultInjectableContext, None)
    assert v is None
    assert NoDefaultInjectableContext not in container

    # explicitly create value
    original = NoDefaultInjectableContext()
    container[NoDefaultInjectableContext] = original
    v, _ = provider.get_value("n/a", NoDefaultInjectableContext, None)
    assert v is original

    # must assert if sections are provided
    with pytest.raises(AssertionError):
        provider.get_value("n/a", spec, None, "ns1")

    # type hints that are not classes
    literal = Literal["a"]
    v, k = provider.get_value("n/a", literal, None)  # type: ignore[arg-type]
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


@pytest.mark.parametrize("spec", (InjectableTestContext, GlobalTestContext))
def test_container_provider_embedded_no_default(
    container: Container, spec: Type[InjectableTestContext]
) -> None:
    with container.injectable_context(NoDefaultInjectableContext()):
        resolve_configuration(EmbeddedWithNoDefaultInjectableContext())
    # default cannot be created so fails
    with pytest.raises(ConfigFieldMissingException) as py_ex:
        resolve_configuration(EmbeddedWithNoDefaultInjectableContext())
    assert py_ex.value.fields == ["injected"]
    # optional returns none
    c = resolve_configuration(EmbeddedWithNoDefaultInjectableOptionalContext())
    assert c.injected is None
