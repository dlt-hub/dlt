from contextlib import contextmanager, nullcontext, AbstractContextManager
import re
import threading
from typing import ClassVar, Dict, Iterator, Optional, Tuple, Type, TypeVar, Any

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

    _INSTANCE: ClassVar["Container"] = None
    _LOCK: ClassVar[threading.Lock] = threading.Lock()
    _MAIN_THREAD_ID: ClassVar[int] = threading.get_ident()
    """A main thread id to which get item will fallback for contexts without default"""

    thread_contexts: Dict[int, Dict[Type[ContainerInjectableContext], ContainerInjectableContext]]
    """A thread aware mapping of injection context """
    _context_container_locks: Dict[str, threading.RLock]
    """Locks for container types on threads."""

    main_context: Dict[Type[ContainerInjectableContext], ContainerInjectableContext]
    """Injection context for the main thread"""

    def __new__(cls: Type["Container"]) -> "Container":
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
            cls._INSTANCE.thread_contexts = {}
            cls._INSTANCE._context_container_locks = {}
            cls._INSTANCE.main_context = cls._INSTANCE.thread_contexts[
                Container._MAIN_THREAD_ID
            ] = {}

        return cls._INSTANCE

    def __init__(self) -> None:
        pass

    def __getitem__(self, spec: Type[TInjectableContext]) -> TInjectableContext:
        # return existing config object or create it from spec
        if not is_subclass(spec, ContainerInjectableContext):
            raise KeyError(f"`{spec.__name__}` is not a context")

        context, item = self._thread_getitem(spec)
        if item is None:
            if spec.can_create_default:
                item = spec()
                self._thread_setitem(context, spec, item)
            else:
                raise ContextDefaultCannotBeCreated(spec)

        return item  # type: ignore[return-value]

    def __setitem__(self, spec: Type[TInjectableContext], value: TInjectableContext) -> None:
        # value passed to container must be final
        value.resolve()
        # put it into context
        self._thread_setitem(self._thread_context(spec), spec, value)

    def __delitem__(self, spec: Type[TInjectableContext]) -> None:
        context = self._thread_context(spec)
        self._thread_delitem(context, spec)

    def __contains__(self, spec: Type[TInjectableContext]) -> bool:
        context = self._thread_context(spec)
        return spec in context

    def _thread_context(
        self, spec: Type[TInjectableContext]
    ) -> Dict[Type[ContainerInjectableContext], ContainerInjectableContext]:
        if spec.global_affinity:
            return self.main_context
        else:
            # thread pool names used in dlt contain originating thread id. use this id over pool id
            if m := re.match(r"dlt-pool-(\d+)-", threading.current_thread().name):
                thread_id = int(m.group(1))
            else:
                thread_id = threading.get_ident()

            # return main context for main thread
            if thread_id == Container._MAIN_THREAD_ID:
                return self.main_context
            # we may add a new empty thread context so lock here
            with Container._LOCK:
                if (context := self.thread_contexts.get(thread_id)) is None:
                    context = self.thread_contexts[thread_id] = {}
                return context

    def _thread_getitem(
        self, spec: Type[TInjectableContext]
    ) -> Tuple[
        Dict[Type[ContainerInjectableContext], ContainerInjectableContext],
        ContainerInjectableContext,
    ]:
        context = self._thread_context(spec)
        item = context.get(spec)
        return context, item

    def _thread_setitem(
        self,
        context: Dict[Type[ContainerInjectableContext], ContainerInjectableContext],
        spec: Type[ContainerInjectableContext],
        value: TInjectableContext,
    ) -> None:
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
        context: Dict[Type[ContainerInjectableContext], ContainerInjectableContext],
        spec: Type[ContainerInjectableContext],
    ) -> None:
        old_ctx = context[spec]
        old_ctx.before_remove()
        del context[spec]
        old_ctx.in_container = False

    @contextmanager
    def injectable_context(
        self, config: TInjectableContext, lock_context: bool = False
    ) -> Iterator[TInjectableContext]:
        """A context manager that will insert `config` into the container and restore the previous value when it gets out of scope."""

        config.resolve()
        spec = type(config)
        previous_config: ContainerInjectableContext = None
        context = self._thread_context(spec)
        lock: AbstractContextManager[Any]

        # if there is a lock_id, we need a lock for the lock_id in the scope of the current context
        if lock_context:
            lock_key = f"{id(context)}"
            if (lock := self._context_container_locks.get(lock_key)) is None:
                # use multi-entrant locks so same thread can acquire this context several times
                with Container._LOCK:
                    self._context_container_locks[lock_key] = lock = threading.RLock()
        else:
            lock = nullcontext()

        with lock:
            # remember context and set item
            previous_config = context.get(spec)
            self._thread_setitem(context, spec, config)
            try:
                yield config
            finally:
                # before setting the previous config for given spec, check if there was no overlapping modification
                context, current_config = self._thread_getitem(spec)
                if current_config is config:
                    # config is injected for spec so restore previous
                    if previous_config is None:
                        self._thread_delitem(context, spec)
                    else:
                        self._thread_setitem(context, spec, previous_config)
                else:
                    # value was modified in the meantime and not restored
                    raise ContainerInjectableContextMangled(spec, context[spec], config)

    def get(self, spec: Type[TInjectableContext]) -> Optional[TInjectableContext]:
        try:
            return self[spec]
        except KeyError:
            return None

    @staticmethod
    def thread_pool_prefix() -> str:
        """Creates a container friendly pool prefix that contains starting thread id. Container implementation will automatically use it
        for any thread-affine contexts instead of using id of the pool thread
        """
        return f"dlt-pool-{threading.get_ident()}-"
