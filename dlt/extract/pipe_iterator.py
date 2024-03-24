import inspect
import types
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Sequence,
    Union,
    Iterator,
    List,
    Awaitable,
    Tuple,
    Type,
    Literal,
)
from concurrent.futures import TimeoutError as FutureTimeoutError

from dlt.common.configuration import configspec
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import (
    BaseConfiguration,
    ContainerInjectableContext,
    known_sections,
)
from dlt.common.configuration.container import Container
from dlt.common.exceptions import PipelineException
from dlt.common.source import unset_current_pipe_name, set_current_pipe_name
from dlt.common.utils import get_callable_name

from dlt.extract.exceptions import (
    DltSourceException,
    ExtractorException,
    InvalidStepFunctionArguments,
    PipeException,
    PipeGenInvalid,
    PipeItemProcessingError,
    ResourceExtractionError,
)
from dlt.extract.pipe import Pipe
from dlt.extract.items import DataItemWithMeta, PipeItem, ResolvablePipeItem, SourcePipeItem
from dlt.extract.utils import wrap_async_iterator
from dlt.extract.concurrency import FuturesPool

TPipeNextItemMode = Literal["fifo", "round_robin"]


class PipeIterator(Iterator[PipeItem]):
    @configspec
    class PipeIteratorConfiguration(BaseConfiguration):
        max_parallel_items: int = 20
        workers: int = 5
        futures_poll_interval: float = 0.01
        copy_on_fork: bool = False
        next_item_mode: str = "fifo"

        __section__: ClassVar[str] = known_sections.EXTRACT

    def __init__(
        self,
        max_parallel_items: int,
        workers: int,
        futures_poll_interval: float,
        sources: List[SourcePipeItem],
        next_item_mode: TPipeNextItemMode,
    ) -> None:
        self._sources = sources
        self._next_item_mode: TPipeNextItemMode = next_item_mode
        self._initial_sources_count = len(sources)
        self._current_source_index: int = 0
        self._futures_pool = FuturesPool(
            workers=workers,
            poll_interval=futures_poll_interval,
            max_parallel_items=max_parallel_items,
        )

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
        sources = [SourcePipeItem(pipe.gen, 0, pipe, None)]
        return cls(max_parallel_items, workers, futures_poll_interval, sources, next_item_mode)

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
        sources: List[SourcePipeItem] = []

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
                if not any(i.pipe == pipe for i in sources):
                    sources.append(SourcePipeItem(pipe.gen, 0, pipe, None))

        # reverse pipes for current mode, as we start processing from the back
        pipes.reverse()
        for pipe in pipes:
            _fork_pipeline(pipe)

        # create extractor
        return cls(max_parallel_items, workers, futures_poll_interval, sources, next_item_mode)

    def __next__(self) -> PipeItem:
        pipe_item: Union[ResolvablePipeItem, SourcePipeItem] = None
        # __next__ should call itself to remove the `while` loop and continue clauses but that may lead to stack overflows: there's no tail recursion opt in python
        # https://stackoverflow.com/questions/13591970/does-python-optimize-tail-recursion (see Y combinator on how it could be emulated)
        while True:
            # do we need new item?
            if pipe_item is None:
                # Always check for done futures to avoid starving the pool
                pipe_item = self._futures_pool.resolve_next_future_no_wait()

                if pipe_item is None:
                    # if none then take element from the newest source
                    pipe_item = self._get_source_item()

                if pipe_item is None:
                    # Wait for some time for futures to resolve
                    try:
                        pipe_item = self._futures_pool.resolve_next_future(
                            use_configured_timeout=True
                        )
                    except FutureTimeoutError:
                        pass
                    else:
                        if pipe_item is None:
                            # pool was empty - then do a regular poll sleep
                            self._futures_pool.sleep()

                if pipe_item is None:
                    if self._futures_pool.empty and len(self._sources) == 0:
                        # no more elements in futures or sources
                        raise StopIteration()
                    else:
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

            # handle async iterator items as new source
            if isinstance(item, AsyncIterator):
                self._sources.append(
                    SourcePipeItem(
                        wrap_async_iterator(item), pipe_item.step, pipe_item.pipe, pipe_item.meta
                    ),
                )
                pipe_item = None
                continue

            if isinstance(item, Awaitable) or callable(item):
                # Callables are submitted to the pool to be executed in the background
                self._futures_pool.submit(pipe_item)  # type: ignore[arg-type]
                pipe_item = None
                # Future will be resolved later, move on to the next item
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

    def _get_source_item(self) -> ResolvablePipeItem:
        sources_count = len(self._sources)
        # no more sources to iterate
        if sources_count == 0:
            return None
        try:
            first_evaluated_index: int = None
            # always reset to end of list for fifo mode, also take into account that new sources can be added
            # if too many new sources is added we switch to fifo not to exhaust them
            if self._next_item_mode == "fifo" or (
                sources_count - self._initial_sources_count >= self._futures_pool.max_parallel_items
            ):
                self._current_source_index = sources_count - 1
            else:
                self._current_source_index = (self._current_source_index - 1) % sources_count
            while True:
                # if we have checked all sources once and all returned None, return and poll/resolve some futures
                if self._current_source_index == first_evaluated_index:
                    return None
                # get next item from the current source
                gen, step, pipe, meta = self._sources[self._current_source_index]
                set_current_pipe_name(pipe.name)

                pipe_item = next(gen)
                if pipe_item is not None:
                    # full pipe item may be returned, this is used by ForkPipe step
                    # to redirect execution of an item to another pipe
                    # else
                    if not isinstance(pipe_item, ResolvablePipeItem):
                        # keep the item assigned step and pipe when creating resolvable item
                        if isinstance(pipe_item, DataItemWithMeta):
                            return ResolvablePipeItem(pipe_item.data, step, pipe, pipe_item.meta)
                        else:
                            return ResolvablePipeItem(pipe_item, step, pipe, meta)

                if pipe_item is not None:
                    return pipe_item

                # remember the first evaluated index
                if first_evaluated_index is None:
                    first_evaluated_index = self._current_source_index
                # always go round robin if None was returned or item is to be run as future
                self._current_source_index = (self._current_source_index - 1) % sources_count

        except StopIteration:
            # remove empty iterator and try another source
            self._sources.pop(self._current_source_index)
            # decrease initial source count if we popped an initial source
            if self._current_source_index < self._initial_sources_count:
                self._initial_sources_count -= 1
            return self._get_source_item()
        except (PipelineException, ExtractorException, DltSourceException, PipeException):
            raise
        except Exception as ex:
            raise ResourceExtractionError(pipe.name, gen, str(ex), "generator") from ex

    def close(self) -> None:
        # unregister the pipe name right after execution of gen stopped
        unset_current_pipe_name()

        # Close the futures pool and cancel all tasks
        # It's important to do this before closing generators as we can't close a running generator
        self._futures_pool.close()

        # close all generators
        for gen, _, _, _ in self._sources:
            if inspect.isgenerator(gen):
                gen.close()

        self._sources.clear()

    def __enter__(self) -> "PipeIterator":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: types.TracebackType
    ) -> None:
        self.close()

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
