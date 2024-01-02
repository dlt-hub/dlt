import inspect
import types
import asyncio
import makefun
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from threading import Thread
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Optional,
    Sequence,
    Union,
    Callable,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Awaitable,
    Tuple,
    Type,
    TYPE_CHECKING,
    Literal,
)

from dlt.common import sleep
from dlt.common.configuration import configspec
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import BaseConfiguration, ContainerInjectableContext
from dlt.common.configuration.container import Container
from dlt.common.exceptions import PipelineException
from dlt.common.source import unset_current_pipe_name, set_current_pipe_name
from dlt.common.typing import AnyFun, AnyType, TDataItems
from dlt.common.utils import get_callable_name

from dlt.extract.exceptions import (
    CreatePipeException,
    DltSourceException,
    ExtractorException,
    InvalidStepFunctionArguments,
    InvalidResourceDataTypeFunctionNotAGenerator,
    InvalidTransformerGeneratorFunction,
    ParametrizedResourceUnbound,
    PipeException,
    PipeGenInvalid,
    PipeItemProcessingError,
    PipeNotBoundToData,
    ResourceExtractionError,
)
from dlt.extract.typing import DataItemWithMeta, ItemTransform, SupportsPipe, TPipedDataItems
from dlt.extract.utils import (
    check_compat_transformer,
    simulate_func_call,
    wrap_compat_transformer,
    wrap_resource_gen,
)

if TYPE_CHECKING:
    TItemFuture = Future[Union[TDataItems, DataItemWithMeta]]
else:
    TItemFuture = Future


class PipeItem(NamedTuple):
    item: TDataItems
    step: int
    pipe: "Pipe"
    meta: Any


class ResolvablePipeItem(NamedTuple):
    # mypy unable to handle recursive types, ResolvablePipeItem should take itself in "item"
    item: Union[TPipedDataItems, Iterator[TPipedDataItems]]
    step: int
    pipe: "Pipe"
    meta: Any


class FuturePipeItem(NamedTuple):
    item: TItemFuture
    step: int
    pipe: "Pipe"
    meta: Any


class SourcePipeItem(NamedTuple):
    item: Union[Iterator[TPipedDataItems], Iterator[ResolvablePipeItem]]
    step: int
    pipe: "Pipe"
    meta: Any


# pipeline step may be iterator of data items or mapping function that returns data item or another iterator
from dlt.common.typing import TDataItem

TPipeStep = Union[
    Iterable[TPipedDataItems],
    Iterator[TPipedDataItems],
    # Callable with meta
    Callable[[TDataItems, Optional[Any]], TPipedDataItems],
    Callable[[TDataItems, Optional[Any]], Iterator[TPipedDataItems]],
    Callable[[TDataItems, Optional[Any]], Iterator[ResolvablePipeItem]],
    # Callable without meta
    Callable[[TDataItems], TPipedDataItems],
    Callable[[TDataItems], Iterator[TPipedDataItems]],
    Callable[[TDataItems], Iterator[ResolvablePipeItem]],
]

TPipeNextItemMode = Union[Literal["fifo"], Literal["round_robin"]]


class ForkPipe:
    def __init__(self, pipe: "Pipe", step: int = -1, copy_on_fork: bool = False) -> None:
        """A transformer that forks the `pipe` and sends the data items to forks added via `add_pipe` method."""
        self._pipes: List[Tuple["Pipe", int]] = []
        self.copy_on_fork = copy_on_fork
        """If true, the data items going to a forked pipe will be copied"""
        self.add_pipe(pipe, step)

    def add_pipe(self, pipe: "Pipe", step: int = -1) -> None:
        if pipe not in self._pipes:
            self._pipes.append((pipe, step))

    def has_pipe(self, pipe: "Pipe") -> bool:
        return pipe in [p[0] for p in self._pipes]

    def __call__(self, item: TDataItems, meta: Any) -> Iterator[ResolvablePipeItem]:
        for i, (pipe, step) in enumerate(self._pipes):
            if i == 0 or not self.copy_on_fork:
                _it = item
            else:
                # shallow copy the item
                _it = copy(item)
            # always start at the beginning
            yield ResolvablePipeItem(_it, step, pipe, meta)


class Pipe(SupportsPipe):
    def __init__(self, name: str, steps: List[TPipeStep] = None, parent: "Pipe" = None) -> None:
        self.name = name
        self._gen_idx = 0
        self._steps: List[TPipeStep] = []
        self.parent = parent
        # add the steps, this will check and mod transformations
        if steps:
            for step in steps:
                self.append_step(step)

    @classmethod
    def from_data(
        cls,
        name: str,
        gen: Union[Iterable[TPipedDataItems], Iterator[TPipedDataItems], AnyFun],
        parent: "Pipe" = None,
    ) -> "Pipe":
        return cls(name, [gen], parent=parent)

    @property
    def is_empty(self) -> bool:
        """Checks if pipe contains any steps"""
        return len(self._steps) == 0

    @property
    def has_parent(self) -> bool:
        return self.parent is not None

    @property
    def is_data_bound(self) -> bool:
        """Checks if pipe is bound to data and can be iterated. Pipe is bound if has a parent that is bound xor is not empty."""
        if self.has_parent:
            return self.parent.is_data_bound
        else:
            return not self.is_empty

    @property
    def gen(self) -> TPipeStep:
        """A data generating step"""
        return self._steps[self._gen_idx]

    @property
    def tail(self) -> TPipeStep:
        return self._steps[-1]

    @property
    def steps(self) -> List[TPipeStep]:
        return self._steps

    def find(self, *step_type: AnyType) -> int:
        """Finds a step with object of type `step_type`"""
        return next((i for i, v in enumerate(self._steps) if isinstance(v, step_type)), -1)

    def __getitem__(self, i: int) -> TPipeStep:
        return self._steps[i]

    def __len__(self) -> int:
        return len(self._steps)

    def fork(self, child_pipe: "Pipe", child_step: int = -1, copy_on_fork: bool = False) -> "Pipe":
        if len(self._steps) == 0:
            raise CreatePipeException(self.name, f"Cannot fork to empty pipe {child_pipe}")
        fork_step = self.tail
        if not isinstance(fork_step, ForkPipe):
            fork_step = ForkPipe(child_pipe, child_step, copy_on_fork)
            self.append_step(fork_step)
        else:
            if not fork_step.has_pipe(child_pipe):
                fork_step.add_pipe(child_pipe, child_step)
        return self

    def append_step(self, step: TPipeStep) -> "Pipe":
        """Appends pipeline step. On first added step performs additional verification if step is a valid data generator"""
        step_no = len(self._steps)
        if step_no == 0 and not self.has_parent:
            self._verify_head_step(step)
        else:
            step = self._wrap_transform_step_meta(step_no, step)

        self._steps.append(step)
        return self

    def insert_step(self, step: TPipeStep, index: int) -> "Pipe":
        """Inserts step at a given index in the pipeline. Allows prepending only for transformers"""
        step_no = len(self._steps)
        if step_no == 0:
            return self.append_step(step)
        if index == 0:
            if not self.has_parent:
                raise CreatePipeException(
                    self.name,
                    "You cannot insert a step before head of the resource that is not a"
                    " transformer",
                )
        step = self._wrap_transform_step_meta(index, step)
        # actually insert in the list
        self._steps.insert(index, step)
        # increase the _gen_idx if added before generator
        if index <= self._gen_idx:
            self._gen_idx += 1
        return self

    def remove_step(self, index: int) -> None:
        """Removes steps at a given index. Gen step cannot be removed"""
        if index == self._gen_idx:
            raise CreatePipeException(
                self.name,
                f"Step at index {index} holds a data generator for this pipe and cannot be removed",
            )
        self._steps.pop(index)
        if index < self._gen_idx:
            self._gen_idx -= 1

    def replace_gen(self, gen: TPipeStep) -> None:
        """Replaces data generating step. Assumes that you know what are you doing"""
        assert not self.is_empty
        self._steps[self._gen_idx] = gen

    def full_pipe(self) -> "Pipe":
        """Creates a pipe that from the current and all the parent pipes."""
        # prevent creating full pipe with unbound heads
        if self.has_parent:
            self._ensure_transform_step(self._gen_idx, self.gen)
        else:
            self.ensure_gen_bound()

        if self.has_parent:
            steps = self.parent.full_pipe().steps
        else:
            steps = []

        steps.extend(self._steps)
        p = Pipe(self.name, [])
        # set the steps so they are not evaluated again
        p._steps = steps
        # return pipe with resolved dependencies
        return p

    def ensure_gen_bound(self) -> None:
        """Verifies that gen step is bound to data"""
        head = self.gen
        if not callable(head):
            return
        sig = inspect.signature(head)
        try:
            # must bind without arguments
            sig.bind()
        except TypeError as ex:
            callable_name = get_callable_name(head)
            raise ParametrizedResourceUnbound(
                self.name,
                callable_name,
                sig.replace(parameters=list(sig.parameters.values())[1:]),
                "resource",
                str(ex),
            )

    def evaluate_gen(self) -> None:
        """Lazily evaluate gen of the pipe when creating PipeIterator. Allows creating multiple use pipes from generator functions and lists"""
        if not self.is_data_bound:
            raise PipeNotBoundToData(self.name, self.has_parent)

        gen = self.gen
        if not self.has_parent:
            if callable(gen):
                try:
                    # must be parameter-less callable or parameters must have defaults
                    self.replace_gen(gen())  # type: ignore
                except TypeError as ex:
                    raise ParametrizedResourceUnbound(
                        self.name,
                        get_callable_name(gen),
                        inspect.signature(gen),
                        "resource",
                        str(ex),
                    )
            # otherwise it must be an iterator
            if isinstance(gen, Iterable):
                self.replace_gen(iter(gen))
        else:
            # verify if transformer can be called
            self._ensure_transform_step(self._gen_idx, gen)

        # evaluate transforms
        for step_no, step in enumerate(self._steps):
            # print(f"pipe {self.name} step no {step_no} step({step})")
            if isinstance(step, ItemTransform):
                self._steps[step_no] = step.bind(self)

    def bind_gen(self, *args: Any, **kwargs: Any) -> Any:
        """Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe and then replaces the pipe gen with the wrapped one"""
        try:
            gen = self._wrap_gen(*args, **kwargs)
            self.replace_gen(gen)
            return gen
        except InvalidResourceDataTypeFunctionNotAGenerator:
            try:
                # call regular function to check what is inside
                _data = self.gen(*args, **kwargs)  # type: ignore
            except Exception as ev_ex:
                # break chaining
                raise ev_ex from None
            # accept if pipe or object holding pipe is returned
            # TODO: use a protocol (but protocols are slow)
            if isinstance(_data, Pipe) or hasattr(_data, "_pipe"):
                return _data
            raise

    def _wrap_gen(self, *args: Any, **kwargs: Any) -> Any:
        """Finds and wraps with `args` + `kwargs` the callable generating step in the resource pipe."""
        head = self.gen
        _data: Any = None

        # skip the data item argument for transformers
        args_to_skip = 1 if self.has_parent else 0
        # simulate function call
        sig, _, _ = simulate_func_call(head, args_to_skip, *args, **kwargs)
        assert callable(head)

        # create wrappers with partial
        if self.has_parent:
            _data = wrap_compat_transformer(self.name, head, sig, *args, **kwargs)
        else:
            _data = wrap_resource_gen(self.name, head, sig, *args, **kwargs)
        return _data

    def _verify_head_step(self, step: TPipeStep) -> None:
        # first element must be Iterable, Iterator or Callable in resource pipe
        if not isinstance(step, (Iterable, Iterator)) and not callable(step):
            raise CreatePipeException(
                self.name, "A head of a resource pipe must be Iterable, Iterator or a Callable"
            )

    def _wrap_transform_step_meta(self, step_no: int, step: TPipeStep) -> TPipeStep:
        # step must be a callable: a transformer or a transformation
        if isinstance(step, (Iterable, Iterator)) and not callable(step):
            if self.has_parent:
                raise CreatePipeException(
                    self.name, "Iterable or Iterator cannot be a step in transformer pipe"
                )
            else:
                raise CreatePipeException(
                    self.name, "Iterable or Iterator can only be a first step in resource pipe"
                )

        if not callable(step):
            raise CreatePipeException(
                self.name,
                "Pipe step must be a callable taking one data item as argument and optional second"
                " meta argument",
            )
        else:
            # check the signature
            sig = inspect.signature(step)
            meta_arg = check_compat_transformer(self.name, step, sig)
            # sig_arg_count = len(sig.parameters)
            # callable_name = get_callable_name(step)
            # if sig_arg_count == 0:
            #     raise InvalidStepFunctionArguments(self.name, callable_name, sig, "Function takes no arguments")
            # # see if meta is present in kwargs
            # meta_arg = next((p for p in sig.parameters.values() if p.name == "meta"), None)
            # if meta_arg is not None:
            #     if meta_arg.kind not in (meta_arg.KEYWORD_ONLY, meta_arg.POSITIONAL_OR_KEYWORD):
            #         raise InvalidStepFunctionArguments(self.name, callable_name, sig, "'meta' cannot be pos only argument '")
            if meta_arg is None:
                # add meta parameter when not present
                orig_step = step

                def _partial(*args: Any, **kwargs: Any) -> Any:
                    # orig step does not have meta
                    kwargs.pop("meta", None)
                    # del kwargs["meta"]
                    return orig_step(*args, **kwargs)

                meta_arg = inspect.Parameter(
                    "meta", inspect._ParameterKind.KEYWORD_ONLY, default=None
                )
                kwargs_arg = next(
                    (p for p in sig.parameters.values() if p.kind == inspect.Parameter.VAR_KEYWORD),
                    None,
                )
                if kwargs_arg:
                    # pass meta in variadic
                    new_sig = sig
                else:
                    new_sig = makefun.add_signature_parameters(sig, last=(meta_arg,))
                step = makefun.wraps(step, new_sig=new_sig)(_partial)

            # verify the step callable, gen may be parametrized and will be evaluated at run time
            if not self.is_empty:
                self._ensure_transform_step(step_no, step)
        return step

    def _ensure_transform_step(self, step_no: int, step: TPipeStep) -> None:
        """Verifies that `step` is a valid callable to be a transform step of the pipeline"""
        assert callable(step), f"{step} must be callable"

        sig = inspect.signature(step)
        try:
            # get eventually modified sig
            sig.bind("item", meta="meta")
        except TypeError as ty_ex:
            callable_name = get_callable_name(step)
            if step_no == self._gen_idx:
                # error for gen step
                if len(sig.parameters) == 0:
                    raise InvalidTransformerGeneratorFunction(self.name, callable_name, sig, code=1)
                else:
                    # show the sig without first argument
                    raise ParametrizedResourceUnbound(
                        self.name,
                        callable_name,
                        sig.replace(parameters=list(sig.parameters.values())[1:]),
                        "transformer",
                        str(ty_ex),
                    )
            else:
                raise InvalidStepFunctionArguments(self.name, callable_name, sig, str(ty_ex))

    def _clone(self, new_name: str = None, with_parent: bool = False) -> "Pipe":
        """Clones the pipe steps, optionally renaming the pipe. Used internally to clone a list of connected pipes."""
        new_parent = self.parent
        if with_parent and self.parent and not self.parent.is_empty:
            parent_new_name = new_name
            if new_name:
                # if we are renaming the pipe, then also rename the parent
                if self.name in self.parent.name:
                    parent_new_name = self.parent.name.replace(self.name, new_name)
                else:
                    parent_new_name = f"{self.parent.name}_{new_name}"
            new_parent = self.parent._clone(parent_new_name, with_parent)

        p = Pipe(new_name or self.name, [], new_parent)
        p._steps = self._steps.copy()
        return p

    def __repr__(self) -> str:
        if self.has_parent:
            bound_str = " data bound to " + repr(self.parent)
        else:
            bound_str = ""
        return f"Pipe {self.name} [steps: {len(self._steps)}] at {id(self)}{bound_str}"


class PipeIterator(Iterator[PipeItem]):
    @configspec
    class PipeIteratorConfiguration(BaseConfiguration):
        max_parallel_items: int = 20
        workers: int = 5
        futures_poll_interval: float = 0.01
        copy_on_fork: bool = False
        next_item_mode: str = "fifo"

        __section__ = "extract"

    def __init__(
        self,
        max_parallel_items: int,
        workers: int,
        futures_poll_interval: float,
        next_item_mode: TPipeNextItemMode,
    ) -> None:
        self.max_parallel_items = max_parallel_items
        self.workers = workers
        self.futures_poll_interval = futures_poll_interval

        self._round_robin_index: int = -1
        self._initial_sources_count: int = 0
        self._async_pool: asyncio.AbstractEventLoop = None
        self._async_pool_thread: Thread = None
        self._thread_pool: ThreadPoolExecutor = None
        self._sources: List[SourcePipeItem] = []
        self._futures: List[FuturePipeItem] = []
        self._next_item_mode = next_item_mode

    @classmethod
    @with_config(spec=PipeIteratorConfiguration)
    def from_pipe(
        cls,
        pipe: Pipe,
        *,
        max_parallel_items: int = 20,
        workers: int = 5,
        futures_poll_interval: float = 0.01,
        next_item_mode: TPipeNextItemMode = "fifo",
    ) -> "PipeIterator":
        # join all dependent pipes
        if pipe.parent:
            pipe = pipe.full_pipe()
        # clone pipe to allow multiple iterations on pipe based on iterables/callables
        pipe = pipe._clone()
        # head must be iterator
        pipe.evaluate_gen()
        if not isinstance(pipe.gen, Iterator):
            raise PipeGenInvalid(pipe.name, pipe.gen)
        # create extractor
        extract = cls(max_parallel_items, workers, futures_poll_interval, next_item_mode)
        # add as first source
        extract._sources.append(SourcePipeItem(pipe.gen, 0, pipe, None))
        cls._initial_sources_count = 1
        return extract

    @classmethod
    @with_config(spec=PipeIteratorConfiguration)
    def from_pipes(
        cls,
        pipes: Sequence[Pipe],
        yield_parents: bool = True,
        *,
        max_parallel_items: int = 20,
        workers: int = 5,
        futures_poll_interval: float = 0.01,
        copy_on_fork: bool = False,
        next_item_mode: TPipeNextItemMode = "fifo",
    ) -> "PipeIterator":
        # print(f"max_parallel_items: {max_parallel_items} workers: {workers}")
        extract = cls(max_parallel_items, workers, futures_poll_interval, next_item_mode)
        # clone all pipes before iterating (recursively) as we will fork them (this add steps) and evaluate gens
        pipes, _ = PipeIterator.clone_pipes(pipes)

        def _fork_pipeline(pipe: Pipe) -> None:
            if pipe.parent:
                # fork the parent pipe
                pipe.evaluate_gen()
                pipe.parent.fork(pipe, copy_on_fork=copy_on_fork)
                # make the parent yield by sending a clone of item to itself with position at the end
                if yield_parents and pipe.parent in pipes:
                    # fork is last step of the pipe so it will yield
                    pipe.parent.fork(pipe.parent, len(pipe.parent) - 1, copy_on_fork=copy_on_fork)
                _fork_pipeline(pipe.parent)
            else:
                # head of independent pipe must be iterator
                pipe.evaluate_gen()
                if not isinstance(pipe.gen, Iterator):
                    raise PipeGenInvalid(pipe.name, pipe.gen)
                # add every head as source only once
                if not any(i.pipe == pipe for i in extract._sources):
                    extract._sources.append(SourcePipeItem(pipe.gen, 0, pipe, None))

        # reverse pipes for current mode, as we start processing from the back
        if next_item_mode == "fifo":
            pipes.reverse()
        for pipe in pipes:
            _fork_pipeline(pipe)

        extract._initial_sources_count = len(extract._sources)

        return extract

    def __next__(self) -> PipeItem:
        pipe_item: Union[ResolvablePipeItem, SourcePipeItem] = None
        # __next__ should call itself to remove the `while` loop and continue clauses but that may lead to stack overflows: there's no tail recursion opt in python
        # https://stackoverflow.com/questions/13591970/does-python-optimize-tail-recursion (see Y combinator on how it could be emulated)
        while True:
            # do we need new item?
            if pipe_item is None:
                # process element from the futures
                if len(self._futures) > 0:
                    pipe_item = self._resolve_futures()
                # if none then take element from the newest source
                if pipe_item is None:
                    pipe_item = self._get_source_item()

                if pipe_item is None:
                    if len(self._futures) == 0 and len(self._sources) == 0:
                        # no more elements in futures or sources
                        raise StopIteration()
                    else:
                        sleep(self.futures_poll_interval)
                    continue

            item = pipe_item.item
            # if item is iterator, then add it as a new source
            if isinstance(item, Iterator):
                # print(f"adding iterable {item}")
                self._sources.append(
                    SourcePipeItem(item, pipe_item.step, pipe_item.pipe, pipe_item.meta)
                )
                pipe_item = None
                continue

            if isinstance(item, Awaitable) or callable(item):
                # do we have a free slot or one of the slots is done?
                if len(self._futures) < self.max_parallel_items or self._next_future() >= 0:
                    # check if Awaitable first - awaitable can also be a callable
                    if isinstance(item, Awaitable):
                        future = asyncio.run_coroutine_threadsafe(item, self._ensure_async_pool())
                    elif callable(item):
                        future = self._ensure_thread_pool().submit(item)
                    # print(future)
                    self._futures.append(FuturePipeItem(future, pipe_item.step, pipe_item.pipe, pipe_item.meta))  # type: ignore
                    # pipe item consumed for now, request a new one
                    pipe_item = None
                    continue
                else:
                    # print("maximum futures exceeded, waiting")
                    sleep(self.futures_poll_interval)
                # try same item later
                continue

            # if we are at the end of the pipe then yield element
            if pipe_item.step == len(pipe_item.pipe) - 1:
                # must be resolved
                if isinstance(item, (Iterator, Awaitable, AsyncIterator)) or callable(item):
                    raise PipeItemProcessingError(
                        pipe_item.pipe.name,
                        f"Pipe item at step {pipe_item.step} was not fully evaluated and is of type"
                        f" {type(pipe_item.item).__name__}. This is internal error or you are"
                        " yielding something weird from resources ie. functions or awaitables.",
                    )
                # mypy not able to figure out that item was resolved
                return pipe_item  # type: ignore

            # advance to next step
            step = pipe_item.pipe[pipe_item.step + 1]
            try:
                set_current_pipe_name(pipe_item.pipe.name)
                next_meta = pipe_item.meta
                next_item = step(item, meta=pipe_item.meta)  # type: ignore
                if isinstance(next_item, DataItemWithMeta):
                    next_meta = next_item.meta
                    next_item = next_item.data
            except TypeError as ty_ex:
                assert callable(step)
                raise InvalidStepFunctionArguments(
                    pipe_item.pipe.name,
                    get_callable_name(step),
                    inspect.signature(step),
                    str(ty_ex),
                )
            except (PipelineException, ExtractorException, DltSourceException, PipeException):
                raise
            except Exception as ex:
                raise ResourceExtractionError(
                    pipe_item.pipe.name, step, str(ex), "transform"
                ) from ex
            # create next pipe item if a value was returned. A None means that item was consumed/filtered out and should not be further processed
            if next_item is not None:
                pipe_item = ResolvablePipeItem(
                    next_item, pipe_item.step + 1, pipe_item.pipe, next_meta
                )
            else:
                pipe_item = None

    def close(self) -> None:
        # unregister the pipe name right after execution of gen stopped
        unset_current_pipe_name()

        def stop_background_loop(loop: asyncio.AbstractEventLoop) -> None:
            loop.stop()

        # stop all futures
        for f, _, _, _ in self._futures:
            if not f.done():
                f.cancel()
        self._futures.clear()

        # close all generators
        for gen, _, _, _ in self._sources:
            if inspect.isgenerator(gen):
                gen.close()
        self._sources.clear()

        # print("stopping loop")
        if self._async_pool:
            self._async_pool.call_soon_threadsafe(stop_background_loop, self._async_pool)
            # print("joining thread")
            self._async_pool_thread.join()
            self._async_pool = None
            self._async_pool_thread = None
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

    def _ensure_async_pool(self) -> asyncio.AbstractEventLoop:
        # lazily create async pool is separate thread
        if self._async_pool:
            return self._async_pool

        def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._async_pool = asyncio.new_event_loop()
        self._async_pool_thread = Thread(
            target=start_background_loop,
            args=(self._async_pool,),
            daemon=True,
            name=Container.thread_pool_prefix() + "futures",
        )
        self._async_pool_thread.start()

        # start or return async pool
        return self._async_pool

    def _ensure_thread_pool(self) -> ThreadPoolExecutor:
        # lazily start or return thread pool
        if self._thread_pool:
            return self._thread_pool

        self._thread_pool = ThreadPoolExecutor(
            self.workers, thread_name_prefix=Container.thread_pool_prefix() + "threads"
        )
        return self._thread_pool

    def __enter__(self) -> "PipeIterator":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: types.TracebackType
    ) -> None:
        self.close()

    def _next_future(self) -> int:
        return next((i for i, val in enumerate(self._futures) if val.item.done()), -1)

    def _resolve_futures(self) -> ResolvablePipeItem:
        # no futures at all
        if len(self._futures) == 0:
            return None

        # anything done?
        idx = self._next_future()
        if idx == -1:
            # nothing done
            return None

        future, step, pipe, meta = self._futures.pop(idx)

        if future.cancelled():
            # get next future
            return self._resolve_futures()

        if future.exception():
            ex = future.exception()
            if isinstance(
                ex, (PipelineException, ExtractorException, DltSourceException, PipeException)
            ):
                raise ex
            raise ResourceExtractionError(pipe.name, future, str(ex), "future") from ex

        item = future.result()
        if isinstance(item, DataItemWithMeta):
            return ResolvablePipeItem(item.data, step, pipe, item.meta)
        else:
            return ResolvablePipeItem(item, step, pipe, meta)

    def _get_source_item(self) -> ResolvablePipeItem:
        if self._next_item_mode == "fifo":
            return self._get_source_item_current()
        elif self._next_item_mode == "round_robin":
            return self._get_source_item_round_robin()

    def _get_source_item_current(self) -> ResolvablePipeItem:
        # no more sources to iterate
        if len(self._sources) == 0:
            return None
        try:
            # get items from last added iterator, this makes the overall Pipe as close to FIFO as possible
            gen, step, pipe, meta = self._sources[-1]
            # print(f"got {pipe.name}")
            # register current pipe name during the execution of gen
            set_current_pipe_name(pipe.name)
            item = None
            while item is None:
                item = next(gen)
            # full pipe item may be returned, this is used by ForkPipe step
            # to redirect execution of an item to another pipe
            if isinstance(item, ResolvablePipeItem):
                return item
            else:
                # keep the item assigned step and pipe when creating resolvable item
                if isinstance(item, DataItemWithMeta):
                    return ResolvablePipeItem(item.data, step, pipe, item.meta)
                else:
                    return ResolvablePipeItem(item, step, pipe, meta)
        except StopIteration:
            # remove empty iterator and try another source
            self._sources.pop()
            return self._get_source_item()
        except (PipelineException, ExtractorException, DltSourceException, PipeException):
            raise
        except Exception as ex:
            raise ResourceExtractionError(pipe.name, gen, str(ex), "generator") from ex

    def _get_source_item_round_robin(self) -> ResolvablePipeItem:
        sources_count = len(self._sources)
        # no more sources to iterate
        if sources_count == 0:
            return None
        # if there are currently more sources than added initially, we need to process the new ones first
        if sources_count > self._initial_sources_count:
            return self._get_source_item_current()
        try:
            # print(f"got {pipe.name}")
            # register current pipe name during the execution of gen
            item = None
            while item is None:
                self._round_robin_index = (self._round_robin_index + 1) % sources_count
                gen, step, pipe, meta = self._sources[self._round_robin_index]
                set_current_pipe_name(pipe.name)
                item = next(gen)
            # full pipe item may be returned, this is used by ForkPipe step
            # to redirect execution of an item to another pipe
            if isinstance(item, ResolvablePipeItem):
                return item
            else:
                # keep the item assigned step and pipe when creating resolvable item
                if isinstance(item, DataItemWithMeta):
                    return ResolvablePipeItem(item.data, step, pipe, item.meta)
                else:
                    return ResolvablePipeItem(item, step, pipe, meta)
        except StopIteration:
            # remove empty iterator and try another source
            self._sources.pop(self._round_robin_index)
            # we need to decrease the index to keep the round robin order
            self._round_robin_index -= 1
            # since in this case we have popped an initial source, we need to decrease the initial sources count
            self._initial_sources_count -= 1
            return self._get_source_item_round_robin()
        except (PipelineException, ExtractorException, DltSourceException, PipeException):
            raise
        except Exception as ex:
            raise ResourceExtractionError(pipe.name, gen, str(ex), "generator") from ex

    @staticmethod
    def clone_pipes(
        pipes: Sequence[Pipe], existing_cloned_pairs: Dict[int, Pipe] = None
    ) -> Tuple[List[Pipe], Dict[int, Pipe]]:
        """This will clone pipes and fix the parent/dependent references"""
        cloned_pipes = [p._clone() for p in pipes if id(p) not in (existing_cloned_pairs or {})]
        cloned_pairs = {id(p): c for p, c in zip(pipes, cloned_pipes)}
        if existing_cloned_pairs:
            cloned_pairs.update(existing_cloned_pairs)

        for clone in cloned_pipes:
            while True:
                if not clone.parent:
                    break
                # if already a clone
                if clone.parent in cloned_pairs.values():
                    break
                # clone if parent pipe not yet cloned
                parent_id = id(clone.parent)
                if parent_id not in cloned_pairs:
                    # print("cloning:" + clone.parent.name)
                    cloned_pairs[parent_id] = clone.parent._clone()
                # replace with clone
                # print(f"replace depends on {clone.name} to {clone.parent.name}")
                clone.parent = cloned_pairs[parent_id]
                # recur with clone
                clone = clone.parent

        return cloned_pipes, cloned_pairs


class ManagedPipeIterator(PipeIterator):
    """A version of the pipe iterator that gets closed automatically on an exception in _next_"""

    _ctx: List[ContainerInjectableContext] = None
    _container: Container = None

    def set_context(self, ctx: List[ContainerInjectableContext]) -> None:
        """Sets list of injectable contexts that will be injected into Container for each call to __next__"""
        self._ctx = ctx
        self._container = Container()

    def __next__(self) -> PipeItem:
        if self._ctx:
            managers = [self._container.injectable_context(ctx) for ctx in self._ctx]
            for manager in managers:
                manager.__enter__()
        try:
            item = super().__next__()
        except Exception as ex:
            # release context manager
            if self._ctx:
                if isinstance(ex, StopIteration):
                    for manager in managers:
                        manager.__exit__(None, None, None)
                else:
                    for manager in managers:
                        manager.__exit__(type(ex), ex, None)
            # crash in next
            self.close()
            raise
        if self._ctx:
            for manager in managers:
                manager.__exit__(None, None, None)
        return item
