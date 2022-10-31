import types
import asyncio
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Thread
from typing import Optional, Sequence, Union, Callable, Iterable, Iterator, List, NamedTuple, Awaitable, Tuple, Type, TYPE_CHECKING

from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.typing import TDataItem, TDataItems

from dlt.extract.exceptions import CreatePipeException, PipeItemProcessingError
from dlt.extract.typing import TPipedDataItems

if TYPE_CHECKING:
    TItemFuture = Future[TDataItems]
else:
    TItemFuture = Future

from dlt.common.time import sleep


class PipeItem(NamedTuple):
    item: TDataItems
    step: int
    pipe: "Pipe"


class ResolvablePipeItem(NamedTuple):
    # mypy unable to handle recursive types, ResolvablePipeItem should take itself in "item"
    item: Union[TPipedDataItems, Iterator[TPipedDataItems]]
    step: int
    pipe: "Pipe"


class FuturePipeItem(NamedTuple):
    item: TItemFuture
    step: int
    pipe: "Pipe"


class SourcePipeItem(NamedTuple):
    item: Union[Iterator[TPipedDataItems], Iterator[ResolvablePipeItem]]
    step: int
    pipe: "Pipe"


# pipeline step may be iterator of data items or mapping function that returns data item or another iterator
TPipeStep = Union[
    Iterable[TPipedDataItems],
    Iterator[TPipedDataItems],
    Callable[[TDataItems], TPipedDataItems],
    Callable[[TDataItems], Iterator[TPipedDataItems]],
    Callable[[TDataItems], Iterator[ResolvablePipeItem]]
]


class ForkPipe:
    def __init__(self, pipe: "Pipe", step: int = -1) -> None:
        self._pipes: List[Tuple["Pipe", int]] = []
        self.add_pipe(pipe, step)

    def add_pipe(self, pipe: "Pipe", step: int = -1) -> None:
        if pipe not in self._pipes:
            self._pipes.append((pipe, step))

    def has_pipe(self, pipe: "Pipe") -> bool:
        return pipe in [p[0] for p in self._pipes]

    def __call__(self, item: TDataItems) -> Iterator[ResolvablePipeItem]:
        for i, (pipe, step) in enumerate(self._pipes):
            _it = item if i == 0 else deepcopy(item)
            # always start at the beginning
            yield ResolvablePipeItem(_it, step, pipe)


class FilterItem:
    def __init__(self, filter_f: Callable[[TDataItem], bool]) -> None:
        self._filter_f = filter_f

    def __call__(self, item: TDataItems) -> Optional[TDataItems]:
        # item may be a list TDataItem or a single TDataItem
        if isinstance(item, list):
            item = [i for i in item if self._filter_f(i)]
            if not item:
                # item was fully consumed by the filter
                return None
            return item
        else:
            return item if self._filter_f(item) else None


class Pipe:
    def __init__(self, name: str, steps: List[TPipeStep] = None, parent: "Pipe" = None) -> None:
        self.name = name
        self._steps: List[TPipeStep] = steps or []
        self._backup_steps: List[TPipeStep] = None
        self._pipe_id = f"{name}_{id(self)}"
        self.parent = parent

    @classmethod
    def from_iterable(cls, name: str, gen: Union[Iterable[TPipedDataItems], Iterator[TPipedDataItems]], parent: "Pipe" = None) -> "Pipe":
        if isinstance(gen, Iterable):
            gen = iter(gen)
        return cls(name, [gen], parent=parent)

    @property
    def head(self) -> TPipeStep:
        return self._steps[0]

    @property
    def tail(self) -> TPipeStep:
        return self._steps[-1]

    @property
    def steps(self) -> List[TPipeStep]:
        return self._steps

    def __getitem__(self, i: int) -> TPipeStep:
        return self._steps[i]

    def __len__(self) -> int:
        return len(self._steps)

    def fork(self, child_pipe: "Pipe", child_step: int = -1) -> "Pipe":
        if len(self._steps) == 0:
            raise CreatePipeException("Cannot fork to empty pipe")
        fork_step = self.tail
        if not isinstance(fork_step, ForkPipe):
            fork_step = ForkPipe(child_pipe, child_step)
            self.add_step(fork_step)
        else:
            if not fork_step.has_pipe(child_pipe):
                fork_step.add_pipe(child_pipe, child_step)
        return self

    def clone(self) -> "Pipe":
        p = Pipe(self.name, self._steps.copy(), self.parent)
        # clone shares the id with the original
        p._pipe_id = self._pipe_id
        return p

    # def backup(self) -> None:
    #     if self.has_backup:
    #         raise PipeBackupException("Pipe backup already exists, restore pipe first")
    #     self._backup_steps = self._steps.copy()

    # @property
    # def has_backup(self) -> bool:
    #     return self._backup_steps is not None


    # def restore(self) -> None:
    #     if not self.has_backup:
    #         raise PipeBackupException("No pipe backup to restore")
    #     self._steps = self._backup_steps
    #     self._backup_steps = None

    def add_step(self, step: TPipeStep) -> "Pipe":
        if len(self._steps) == 0 and self.parent is None:
            # first element must be iterable or iterator
            if not isinstance(step, (Iterable, Iterator)):
                raise CreatePipeException("First step of independent pipe must be Iterable or Iterator")
            else:
                if isinstance(step, Iterable):
                    step = iter(step)
                self._steps.append(step)
        else:
            if isinstance(step, (Iterable, Iterator)):
                if self.parent is not None:
                    raise CreatePipeException("Iterable or Iterator cannot be a step in dependent pipe")
                else:
                    raise CreatePipeException("Iterable or Iterator can only be a first step in independent pipe")
            if not callable(step):
                raise CreatePipeException("Pipe step must be a callable taking exactly one data item as input")
            self._steps.append(step)
        return self

    def full_pipe(self) -> "Pipe":
        if self.parent:
            pipe = self.parent.full_pipe().steps
        else:
            pipe = []

        # return pipe with resolved dependencies
        pipe.extend(self._steps)
        return Pipe(self.name, pipe)

    def evaluate_head(self) -> None:
        # if pipe head is callable then call it
        if self.parent is None:
            if callable(self.head):
                self._steps[0] = self.head()  # type: ignore

    def __repr__(self) -> str:
        return f"Pipe {self.name} ({self._pipe_id}) at {id(self)}"


class PipeIterator(Iterator[PipeItem]):

    @configspec
    class PipeIteratorConfiguration(BaseConfiguration):
        max_parallel_items: int = 100
        workers: int = 5
        futures_poll_interval: float = 0.01

    def __init__(self, max_parallel_items: int, workers: int, futures_poll_interval: float) -> None:
        self.max_parallel_items = max_parallel_items
        self.workers = workers
        self.futures_poll_interval = futures_poll_interval

        self._async_pool: asyncio.AbstractEventLoop = None
        self._async_pool_thread: Thread = None
        self._thread_pool: ThreadPoolExecutor = None
        self._sources: List[SourcePipeItem] = []
        self._futures: List[FuturePipeItem] = []

    @classmethod
    @with_config(spec=PipeIteratorConfiguration)
    def from_pipe(cls, pipe: Pipe, *, max_parallel_items: int = 100, workers: int = 5, futures_poll_interval: float = 0.01) -> "PipeIterator":
        if pipe.parent:
            pipe = pipe.full_pipe()
        # head must be iterator
        pipe.evaluate_head()
        assert isinstance(pipe.head, Iterator)
        # create extractor
        extract = cls(max_parallel_items, workers, futures_poll_interval)
        # add as first source
        extract._sources.append(SourcePipeItem(pipe.head, 0, pipe))
        return extract

    @classmethod
    @with_config(spec=PipeIteratorConfiguration)
    def from_pipes(cls, pipes: Sequence[Pipe], yield_parents: bool = True, *, max_parallel_items: int = 100, workers: int = 5, futures_poll_interval: float = 0.01) -> "PipeIterator":
        extract = cls(max_parallel_items, workers, futures_poll_interval)
        # TODO: consider removing cloning. pipe are single use and may be iterated only once, here we modify an immediately run
        # clone all pipes before iterating (recursively) as we will fork them and this add steps
        pipes = PipeIterator.clone_pipes(pipes)

        def _fork_pipeline(pipe: Pipe) -> None:
            print(f"forking: {pipe.name}")
            if pipe.parent:
                # fork the parent pipe
                pipe.parent.fork(pipe)
                # make the parent yield by sending a clone of item to itself with position at the end
                if yield_parents and pipe.parent in pipes:
                    # fork is last step of the pipe so it will yield
                    pipe.parent.fork(pipe.parent, len(pipe.parent) - 1)
                _fork_pipeline(pipe.parent)
            else:
                # head of independent pipe must be iterator
                pipe.evaluate_head()
                assert isinstance(pipe.head, Iterator)
                # add every head as source only once
                if not any(i.pipe == pipe for i in extract._sources):
                    print("add to sources: " + pipe.name)
                    extract._sources.append(SourcePipeItem(pipe.head, 0, pipe))

        for pipe in reversed(pipes):
            _fork_pipeline(pipe)

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
                        # if len(_sources
                        # print("waiting")
                        sleep(self.futures_poll_interval)
                    continue


            item = pipe_item.item
            # if item is iterator, then add it as a new source
            if isinstance(item, Iterator):
                # print(f"adding iterable {item}")
                self._sources.append(SourcePipeItem(item, pipe_item.step, pipe_item.pipe))
                pipe_item = None
                continue

            if isinstance(item, Awaitable) or callable(item):
                # do we have a free slot or one of the slots is done?
                if len(self._futures) < self.max_parallel_items or self._next_future() >= 0:
                    if isinstance(item, Awaitable):
                        future = asyncio.run_coroutine_threadsafe(item, self._ensure_async_pool())
                    elif callable(item):
                        future = self._ensure_thread_pool().submit(item)
                    # print(future)
                    self._futures.append(FuturePipeItem(future, pipe_item.step, pipe_item.pipe))  # type: ignore
                    # pipe item consumed for now, request a new one
                    pipe_item = None
                    continue
                else:
                    # print("maximum futures exceeded, waiting")
                    sleep(self.futures_poll_interval)
                # try same item later
                continue

            # if we are at the end of the pipe then yield element
            # print(pipe_item)
            if pipe_item.step == len(pipe_item.pipe) - 1:
                # must be resolved
                if isinstance(item, (Iterator, Awaitable)) or callable(pipe_item.pipe):
                    raise PipeItemProcessingError("Pipe item not processed", pipe_item)
                # mypy not able to figure out that item was resolved
                return pipe_item  # type: ignore

            # advance to next step
            step = pipe_item.pipe[pipe_item.step + 1]
            assert callable(step)
            next_item = step(item)
            pipe_item = ResolvablePipeItem(next_item, pipe_item.step + 1, pipe_item.pipe)


    def _ensure_async_pool(self) -> asyncio.AbstractEventLoop:
        # lazily create async pool is separate thread
        if self._async_pool:
            return self._async_pool

        def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._async_pool = asyncio.new_event_loop()
        self._async_pool_thread = Thread(target=start_background_loop, args=(self._async_pool,), daemon=True)
        self._async_pool_thread.start()

        # start or return async pool
        return self._async_pool

    def _ensure_thread_pool(self) -> ThreadPoolExecutor:
        # lazily start or return thread pool
        if self._thread_pool:
            return self._thread_pool

        self._thread_pool = ThreadPoolExecutor(self.workers)
        return self._thread_pool

    def __enter__(self) -> "PipeIterator":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: types.TracebackType) -> None:

        def stop_background_loop(loop: asyncio.AbstractEventLoop) -> None:
            loop.stop()

        for f, _, _ in self._futures:
            if not f.done():
                f.cancel()
        print("stopping loop")
        if self._async_pool:
            self._async_pool.call_soon_threadsafe(stop_background_loop, self._async_pool)
            print("joining thread")
            self._async_pool_thread.join()
            self._async_pool = None
            self._async_pool_thread = None
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

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

        future, step, pipe = self._futures.pop(idx)

        if future.cancelled():
            # get next future
            return self._resolve_futures()

        if future.exception():
            raise future.exception()

        return ResolvablePipeItem(future.result(), step, pipe)

    def _get_source_item(self) -> ResolvablePipeItem:
        # no more sources to iterate
        if len(self._sources) == 0:
            return None

        # get items from last added iterator, this makes the overall Pipe as close to FIFO as possible
        gen, step, pipe = self._sources[-1]
        try:
            item = next(gen)
            # full pipe item may be returned, this is used by ForkPipe step
            # to redirect execution of an item to another pipe
            if isinstance(item, ResolvablePipeItem):
                return item
            else:
                # keep the item assigned step and pipe
                return ResolvablePipeItem(item, step, pipe)
        except StopIteration:
            # remove empty iterator and try another source
            self._sources.pop()
            return self._get_source_item()

    @staticmethod
    def clone_pipes(pipes: Sequence[Pipe]) -> Sequence[Pipe]:
        # will clone the pipes including the dependent ones
        cloned_pipes = [p.clone() for p in pipes]
        cloned_pairs = {id(p): c for p, c in zip(pipes, cloned_pipes)}

        for clone in cloned_pipes:
            while True:
                if not clone.parent:
                    break
                # if already a clone
                if clone.parent in cloned_pairs.values():
                    break
                # clone if parent pipe not yet cloned
                if id(clone.parent) not in cloned_pairs:
                    print("cloning:" + clone.parent.name)
                    cloned_pairs[id(clone.parent)] = clone.parent.clone()
                # replace with clone
                print(f"replace depends on {clone.name} to {clone.parent.name}")
                clone.parent = cloned_pairs[id(clone.parent)]
                # recurr with clone
                clone = clone.parent

        return cloned_pipes
