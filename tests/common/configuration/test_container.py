from concurrent.futures import ThreadPoolExecutor
import pytest
import threading
from typing import Any, ClassVar, Dict, Literal, Optional, Iterator, Type

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
class WorkerAffinityContext(ContainerInjectableContext):
    """A custom context marked for worker affinity, global."""

    worker_affinity: ClassVar[bool] = True
    global_affinity: ClassVar[bool] = True

    value: str = None


@configspec
class ThreadLocalWorkerContext(ContainerInjectableContext):
    """A custom context marked for worker affinity, thread-local."""

    worker_affinity: ClassVar[bool] = True
    global_affinity: ClassVar[bool] = False

    value: str = None


@configspec
class FailingAddContext(ContainerInjectableContext):
    """Context whose after_add raises to test exception safety."""

    can_create_default: ClassVar[bool] = False
    global_affinity: ClassVar[bool] = True

    def after_add(self) -> None:
        raise RuntimeError("after_add failed")


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
    try:
        yield Container()
    finally:
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


def test_get_worker_contexts_empty(container: Container) -> None:
    """Empty container returns empty dict."""
    contexts = container.get_worker_contexts()
    assert contexts == {}


def test_get_worker_contexts_no_worker_affinity(container: Container) -> None:
    """Contexts without worker_affinity are not included."""
    container[InjectableTestContext] = InjectableTestContext(current_value="test")
    container[GlobalTestContext] = GlobalTestContext(current_value="global")

    contexts = container.get_worker_contexts()
    assert contexts == {}


def test_get_worker_contexts_with_worker_affinity(container: Container) -> None:
    """Contexts with worker_affinity=True are included."""
    worker_ctx = WorkerAffinityContext(value="worker")
    container[WorkerAffinityContext] = worker_ctx

    contexts = container.get_worker_contexts()
    assert WorkerAffinityContext in contexts
    assert contexts[WorkerAffinityContext] is worker_ctx


def test_get_worker_contexts_mixed(container: Container) -> None:
    """Only worker_affinity contexts are returned when mixed with others."""
    container[InjectableTestContext] = InjectableTestContext(current_value="test")
    worker_ctx = WorkerAffinityContext(value="worker")
    container[WorkerAffinityContext] = worker_ctx

    contexts = container.get_worker_contexts()
    assert len(contexts) == 1
    assert WorkerAffinityContext in contexts
    assert InjectableTestContext not in contexts


def test_worker_context_restore_in_simulated_worker(container: Container) -> None:
    """Simulate restoring worker contexts in a 'worker' process."""
    from dlt.common.runtime.init import restore_run_context
    from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

    # set up worker context in "main" process
    worker_ctx = WorkerAffinityContext(value="from_main")
    container[WorkerAffinityContext] = worker_ctx

    # get contexts to pass to worker
    worker_contexts = container.get_worker_contexts()
    assert WorkerAffinityContext in worker_contexts

    # get run context
    run_ctx = container[PluggableRunContext]

    # simulate worker: create fresh container
    worker_container = Container()

    # restore contexts in worker
    restore_run_context(run_ctx.context, worker_contexts)

    # verify worker has the context
    restored = worker_container[WorkerAffinityContext]
    assert restored.value == "from_main"


def test_get_worker_contexts_thread_local(container: Container) -> None:
    """Each thread gets its own thread-local worker contexts."""
    results: Dict[str, Dict[Type[ContainerInjectableContext], ContainerInjectableContext]] = {}
    event1 = threading.Semaphore(0)
    event2 = threading.Semaphore(0)

    def thread_a() -> None:
        container[ThreadLocalWorkerContext] = ThreadLocalWorkerContext(value="thread_a")
        event1.release()  # signal: thread_a context set
        event2.acquire()  # wait for thread_b to set its context
        results["thread_a"] = container.get_worker_contexts()
        event1.release()  # signal: thread_a done

    def thread_b() -> None:
        event1.acquire()  # wait for thread_a to set its context
        container[ThreadLocalWorkerContext] = ThreadLocalWorkerContext(value="thread_b")
        event2.release()  # signal: thread_b context set
        results["thread_b"] = container.get_worker_contexts()

    t_a = threading.Thread(target=thread_a, daemon=True)
    t_b = threading.Thread(target=thread_b, daemon=True)
    t_a.start()
    t_b.start()

    t_b.join(timeout=5)
    event1.acquire()  # wait for thread_a to finish
    t_a.join(timeout=5)

    # each thread should get its own context
    assert ThreadLocalWorkerContext in results["thread_a"]
    assert ThreadLocalWorkerContext in results["thread_b"]
    ctx_a = results["thread_a"][ThreadLocalWorkerContext]
    ctx_b = results["thread_b"][ThreadLocalWorkerContext]
    assert isinstance(ctx_a, ThreadLocalWorkerContext)
    assert isinstance(ctx_b, ThreadLocalWorkerContext)
    assert ctx_a.value == "thread_a"
    assert ctx_b.value == "thread_b"


def test_get_worker_contexts_from_pool_thread(container: Container) -> None:
    """Pool threads (with dlt-pool-{id}- prefix) get main thread's context."""
    # set thread-local context in main thread
    container[ThreadLocalWorkerContext] = ThreadLocalWorkerContext(value="main_thread")

    def get_contexts_in_pool() -> (
        Dict[Type[ContainerInjectableContext], ContainerInjectableContext]
    ):
        return container.get_worker_contexts()

    # pool thread should get main thread's context due to thread name prefix
    with ThreadPoolExecutor(thread_name_prefix=container.thread_pool_prefix()) as pool:
        future = pool.submit(get_contexts_in_pool)
        result = future.result()

    assert ThreadLocalWorkerContext in result
    ctx = result[ThreadLocalWorkerContext]
    assert isinstance(ctx, ThreadLocalWorkerContext)
    assert ctx.value == "main_thread"


def test_get_worker_contexts_two_pools_different_contexts(container: Container) -> None:
    """Two separate thread pools with different contexts get their own contexts."""
    from concurrent.futures import ThreadPoolExecutor
    import time

    results: Dict[str, Dict[Type[ContainerInjectableContext], ContainerInjectableContext]] = {}

    def run_in_pool_a() -> None:
        # set context for this "pipeline"
        container[ThreadLocalWorkerContext] = ThreadLocalWorkerContext(value="pool_a")
        # create a pool that will inherit this thread's context
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=container.thread_pool_prefix()
        ) as pool:
            future = pool.submit(container.get_worker_contexts)
            results["pool_a"] = future.result()

    def run_in_pool_b() -> None:
        # set different context for this "pipeline"
        container[ThreadLocalWorkerContext] = ThreadLocalWorkerContext(value="pool_b")
        # create a pool that will inherit this thread's context
        with ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=container.thread_pool_prefix()
        ) as pool:
            future = pool.submit(container.get_worker_contexts)
            results["pool_b"] = future.result()

    # run both in separate threads (simulating two pipelines)
    t_a = threading.Thread(target=run_in_pool_a, daemon=True)
    t_b = threading.Thread(target=run_in_pool_b, daemon=True)
    t_a.start()
    t_b.start()
    t_a.join(timeout=5)
    t_b.join(timeout=5)

    # each pool should have gotten its parent thread's context
    assert ThreadLocalWorkerContext in results["pool_a"]
    assert ThreadLocalWorkerContext in results["pool_b"]
    ctx_a = results["pool_a"][ThreadLocalWorkerContext]
    ctx_b = results["pool_b"][ThreadLocalWorkerContext]
    assert isinstance(ctx_a, ThreadLocalWorkerContext)
    assert isinstance(ctx_b, ThreadLocalWorkerContext)
    assert ctx_a.value == "pool_a"
    assert ctx_b.value == "pool_b"


@pytest.mark.parametrize("lock_context_on_yield", [True, False])
def test_injectable_context_nested_reentrance(
    container: Container, lock_context_on_yield: bool
) -> None:
    """Nested injectable_context works via RLock reentrance."""
    outer = InjectableTestContext(current_value="OUTER")
    inner = InjectableTestContext(current_value="INNER")

    with container.injectable_context(outer, lock_context_on_yield=lock_context_on_yield):
        assert container[InjectableTestContext].current_value == "OUTER"
        with container.injectable_context(inner, lock_context_on_yield=lock_context_on_yield):
            assert container[InjectableTestContext].current_value == "INNER"
        assert container[InjectableTestContext].current_value == "OUTER"
    assert InjectableTestContext not in container


@pytest.mark.parametrize("lock_context_on_yield", [True, False])
def test_injectable_context_lock_controls_concurrent_write(
    container: Container, lock_context_on_yield: bool
) -> None:
    """lock_context_on_yield=True blocks concurrent writes; False allows them."""
    barrier = threading.Barrier(2, timeout=5)
    write_completed = threading.Event()

    def writer_thread() -> None:
        barrier.wait()
        container[GlobalTestContext] = GlobalTestContext(current_value="THREAD")
        write_completed.set()

    t = threading.Thread(target=writer_thread, daemon=True)
    try:
        with container.injectable_context(
            GlobalTestContext(current_value="MAIN"),
            lock_context_on_yield=lock_context_on_yield,
        ):
            t.start()
            # barrier will unblock when both threads wait
            barrier.wait()
            # wait for writer to complete, but it may wait on yield lock
            completed = write_completed.wait(timeout=1)
            if lock_context_on_yield:
                assert not completed, "writer must be blocked by held lock"
            else:
                assert completed, "writer must succeed when lock is released"
    except ContainerInjectableContextMangled:
        assert not lock_context_on_yield, "mangled only expected when lock not held"
    finally:
        write_completed.wait(timeout=5)
        t.join(timeout=5)


def test_injectable_context_lock_no_deadlock_on_new_thread(container: Container) -> None:
    """Per-context lock does not block new threads from creating their own contexts."""
    thread_done = threading.Event()

    def thread_func() -> None:
        container[InjectableTestContext] = InjectableTestContext(current_value="THREAD")
        thread_done.set()

    with container.injectable_context(
        GlobalTestContext(current_value="LOCKED"), lock_context_on_yield=True
    ):
        t = threading.Thread(target=thread_func, daemon=True)
        t.start()
        assert thread_done.wait(timeout=5), "thread must not deadlock"
        t.join(timeout=5)


def test_concurrent_default_creation(container: Container) -> None:
    """Multiple threads racing __getitem__ create exactly one default instance."""
    num_threads = 4
    barrier = threading.Barrier(num_threads, timeout=5)
    results = [None] * num_threads

    def reader(idx: int) -> None:
        barrier.wait()
        results[idx] = container[GlobalTestContext]

    threads = [threading.Thread(target=reader, args=(i,), daemon=True) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert all(r is not None for r in results)
    # double-checked locking must produce exactly one instance
    assert all(r is results[0] for r in results)


def test_injectable_context_setitem_exception_releases_lock(container: Container) -> None:
    """If after_add raises during setitem, the per-context lock is released."""
    with pytest.raises(RuntimeError, match="after_add failed"):
        with container.injectable_context(FailingAddContext()):
            pass  # never reached

    # verify lock is released: write from another thread using the same global lock
    acquired = threading.Event()

    def try_write() -> None:
        container[GlobalTestContext] = GlobalTestContext(current_value="AFTER_FAIL")
        acquired.set()

    t = threading.Thread(target=try_write, daemon=True)
    t.start()
    assert acquired.wait(timeout=5), "lock must be released after setitem exception"
    t.join(timeout=5)
    assert container[GlobalTestContext].current_value == "AFTER_FAIL"
