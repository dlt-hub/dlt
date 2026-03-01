from contextlib import contextmanager
import re
import threading
from typing import ClassVar, Dict, Iterator, Optional, Tuple, Type, TypeVar

from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    TInjectableContext,
)
from dlt.common.configuration.exceptions import (
    ContainerInjectableContextMangled,
    ContextDefaultCannotBeCreated,
)
from dlt.common.typing import is_subclass


class Container:
    """A singleton injection container holding several injection contexts. Implements basic dictionary interface.

    Injection context is identified by its type and available via dict indexer. The common pattern is to instantiate default context value
    if it is not yet present in container.

    By default, the context is thread-affine so it can be injected only n the thread that originally set it. This behavior may be changed
    in particular context type (spec).

    The indexer is settable and allows to explicitly set the value. This is required by in any context that needs to be explicitly instantiated.

    The `injectable_context` allows to set a context with a `with` keyword and then restore the previous one after it gets out of scope.

    """

    _INSTANCE: ClassVar[Optional["Container"]] = None
    _LOCK: ClassVar[threading.RLock] = threading.RLock()
    _MAIN_THREAD_ID: ClassVar[int] = threading.get_ident()
    """A main thread id to which get item will fallback for contexts without default"""

    _TContext = Dict[Type[ContainerInjectableContext], ContainerInjectableContext]

    thread_contexts: Dict[int, Tuple["Container._TContext", threading.RLock]]
    """Thread-id → (context dict, per-context RLock). Every thread gets its own lock."""

    def __new__(cls: Type["Container"]) -> "Container":
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
            cls._INSTANCE.thread_contexts = {Container._MAIN_THREAD_ID: ({}, threading.RLock())}

        return cls._INSTANCE

    def __init__(self) -> None:
        pass

    def __getitem__(self, spec: Type[TInjectableContext]) -> TInjectableContext:
        # return existing config object or create it from spec
        if not is_subclass(spec, ContainerInjectableContext):
            raise KeyError(f"`{spec.__name__}` is not a context")

        context, lock = self._thread_context(spec)
        item = context.get(spec)
        if item is None:
            if spec.can_create_default:
                with lock:
                    # re-check after acquiring lock (double-checked locking)
                    item = context.get(spec)
                    if item is None:
                        # create and set inside lock - RLock allows reentrant access
                        # if spec() or callbacks access container
                        item = spec()
                        self._thread_setitem(context, spec, item)
            else:
                raise ContextDefaultCannotBeCreated(spec)

        return item  # type: ignore[return-value]

    def __setitem__(self, spec: Type[TInjectableContext], value: TInjectableContext) -> None:
        # value passed to container must be final
        value.resolve()
        context, lock = self._thread_context(spec)
        with lock:
            self._thread_setitem(context, spec, value)

    def __delitem__(self, spec: Type[TInjectableContext]) -> None:
        context, lock = self._thread_context(spec)
        with lock:
            self._thread_delitem(context, spec)

    def __contains__(self, spec: Type[TInjectableContext]) -> bool:
        context, _ = self._thread_context(spec)
        return spec in context

    def _effective_thread_id(self) -> int:
        """Return the effective thread id for context lookup.

        Thread in a dlt pool (named `dlt-pool-{id}-*`) see the same context
        to support sharing context in isolated extract pipelines.
        """
        if m := re.match(r"dlt-pool-(\d+)-", threading.current_thread().name):
            return int(m.group(1))
        return threading.get_ident()

    def _thread_context(
        self, spec: Type[TInjectableContext]
    ) -> Tuple["Container._TContext", threading.RLock]:
        """Return (context dict, per-context lock) for the given spec."""
        if spec.global_affinity:
            return self.thread_contexts[Container._MAIN_THREAD_ID]

        thread_id = self._effective_thread_id()

        # fast path: context already exists (dict.get is GIL-atomic)
        ctx_and_lock = self.thread_contexts.get(thread_id)
        if ctx_and_lock is not None:
            return ctx_and_lock

        # create new thread context under structural lock
        with Container._LOCK:
            if (ctx_and_lock := self.thread_contexts.get(thread_id)) is None:
                ctx_and_lock = ({}, threading.RLock())
                self.thread_contexts[thread_id] = ctx_and_lock
            return ctx_and_lock

    def _thread_setitem(
        self,
        context: "Container._TContext",
        spec: Type[ContainerInjectableContext],
        value: TInjectableContext,
    ) -> None:
        """Set context item and run callbacks. Caller must hold the context lock."""
        old_ctx = context.get(spec)
        if old_ctx:
            old_ctx.before_remove()
            old_ctx.in_container = False
        context[spec] = value
        value.in_container = True
        value.after_add()
        if not value.extras_added:
            value.add_extras()
            value.extras_added = True

    def _thread_delitem(
        self,
        context: "Container._TContext",
        spec: Type[ContainerInjectableContext],
    ) -> None:
        """Delete context item and run callbacks. Caller must hold the context lock."""
        old_ctx = context.get(spec)
        if old_ctx is None:
            raise KeyError(spec)
        old_ctx.before_remove()
        old_ctx.in_container = False
        del context[spec]

    @contextmanager
    def injectable_context(
        self, config: TInjectableContext, lock_context_on_yield: bool = False
    ) -> Iterator[TInjectableContext]:
        """Insert ``config`` into the container and restore the previous value on exit.

        Args:
            config (TInjectableContext): The context instance to inject.
            lock_context_on_yield (bool): When False (default), the per-context lock
                is released before yield and re-acquired for restore, allowing other
                threads to write to the same spec. When True, the lock is held for
                the entire lifetime including yield.

        Yields:
            TInjectableContext: The injected ``config`` instance.

        Raises:
            ContainerInjectableContextMangled: If another writer overwrote the
                context during yield (only possible when ``lock_context_on_yield``
                is False).
        """

        config.resolve()
        spec = type(config)
        context, ctx_lock = self._thread_context(spec)

        # always lock for setitem
        ctx_lock.acquire()
        try:
            previous_config: ContainerInjectableContext = context.get(spec)
            self._thread_setitem(context, spec, config)
        except BaseException:
            ctx_lock.release()
            raise

        if not lock_context_on_yield:
            ctx_lock.release()

        try:
            yield config
        finally:
            # re-acquire for restore when lock_context is False
            if not lock_context_on_yield:
                ctx_lock.acquire()
            try:
                current_config = context.get(spec)
                if current_config is config:
                    if previous_config is None:
                        self._thread_delitem(context, spec)
                    else:
                        self._thread_setitem(context, spec, previous_config)
                else:
                    raise ContainerInjectableContextMangled(spec, context[spec], config)
            finally:
                ctx_lock.release()

    def get(self, spec: Type[TInjectableContext]) -> Optional[TInjectableContext]:
        try:
            return self[spec]
        except KeyError:
            return None

    def get_worker_contexts(
        self,
    ) -> Dict[Type[ContainerInjectableContext], ContainerInjectableContext]:
        """Collect all contexts marked with `worker_affinity=True` for the current thread.

        Returns a dict mapping context type to instance for all contexts that should
        be passed to worker processes.
        """
        result: Dict[Type[ContainerInjectableContext], ContainerInjectableContext] = {}

        with Container._LOCK:
            # collect global_affinity contexts from main thread
            main_ctx, _ = self.thread_contexts[Container._MAIN_THREAD_ID]
            for spec, ctx in list(main_ctx.items()):
                if spec.worker_affinity:
                    result[spec] = ctx

            # collect thread-local contexts from the effective thread's context
            thread_id = self._effective_thread_id()
            if thread_id != Container._MAIN_THREAD_ID:
                ctx_and_lock = self.thread_contexts.get(thread_id)
                if ctx_and_lock is not None:
                    thread_ctx, _ = ctx_and_lock
                    for spec, ctx in list(thread_ctx.items()):
                        if spec.worker_affinity:
                            result[spec] = ctx

        return result

    @staticmethod
    def thread_pool_prefix() -> str:
        """Creates a container friendly pool prefix that contains starting thread id. Container implementation will automatically use it
        for any thread-affine contexts instead of using id of the pool thread
        """
        return f"dlt-pool-{threading.get_ident()}-"
