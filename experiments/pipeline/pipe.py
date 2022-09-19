import types
import asyncio
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Thread
from typing import Optional, Sequence, Union, Callable, Iterable, Iterator, List, NamedTuple, Awaitable, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    TItemFuture = Future[TDirectDataItem]
else:
    TItemFuture = Future

from dlt.common.exceptions import DltException
from dlt.common.time import sleep
from dlt.common.sources import TDirectDataItem, TResolvableDataItem


class PipeItem(NamedTuple):
    item: TDirectDataItem
    step: int
    pipe: "Pipe"


class ResolvablePipeItem(NamedTuple):
    # mypy unable to handle recursive types, ResolvablePipeItem should take itself in "item"
    item: Union[TResolvableDataItem, Iterator[TResolvableDataItem]]
    step: int
    pipe: "Pipe"


class FuturePipeItem(NamedTuple):
    item: TItemFuture
    step: int
    pipe: "Pipe"


class SourcePipeItem(NamedTuple):
    item: Union[Iterator[TResolvableDataItem], Iterator[ResolvablePipeItem]]
    step: int
    pipe: "Pipe"


# pipeline step may be iterator of data items or mapping function that returns data item or another iterator
TPipeStep = Union[
    Iterable[TResolvableDataItem],
    Iterator[TResolvableDataItem],
    Callable[[TDirectDataItem], TResolvableDataItem],
    Callable[[TDirectDataItem], Iterator[TResolvableDataItem]],
    Callable[[TDirectDataItem], Iterator[ResolvablePipeItem]]
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

    def __call__(self, item: TDirectDataItem) -> Iterator[ResolvablePipeItem]:
        for i, (pipe, step) in enumerate(self._pipes):
            _it = item if i == 0 else deepcopy(item)
            # always start at the beginning
            yield ResolvablePipeItem(_it, step, pipe)


class FilterItem:
    def __init__(self, filter_f: Callable[[TDirectDataItem], bool]) -> None:
        self._filter_f = filter_f

    def __call__(self, item: TDirectDataItem) -> Optional[TDirectDataItem]:
        # item may be a list TDataItem or a single TDataItem
        if isinstance(item, list):
            item = [i for i in item if self._filter_f(i)]
            if not item:
                # item was fully consumed by the filer
                return None
            return item
        else:
            return item if self._filter_f(item) else None


class Pipe:
    def __init__(self, name: str, steps: List[TPipeStep] = None, depends_on: "Pipe" = None) -> None:
        self.name = name
        self._steps: List[TPipeStep] = steps or []
        self.depends_on = depends_on

    @classmethod
    def from_iterable(cls, name: str, gen: Union[Iterable[TResolvableDataItem], Iterator[TResolvableDataItem]]) -> "Pipe":
        if isinstance(gen, Iterable):
            gen = iter(gen)
        return cls(name, [gen])

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
        return Pipe(self.name, self._steps.copy(), self.depends_on)

    def add_step(self, step: TPipeStep) -> "Pipe":
        if len(self._steps) == 0 and self.depends_on is None:
            # first element must be iterable or iterator
            if not isinstance(step, (Iterable, Iterator)):
                raise CreatePipeException("First step of independent pipe must be Iterable or Iterator")
            else:
                if isinstance(step, Iterable):
                    step = iter(step)
                self._steps.append(step)
        else:
            if isinstance(step, (Iterable, Iterator)):
                if self.depends_on is not None:
                    raise CreatePipeException("Iterable or Iterator cannot be a step in dependent pipe")
                else:
                    raise CreatePipeException("Iterable or Iterator can only be a first step in independent pipe")
            if not callable(step):
                raise CreatePipeException("Pipe step must be a callable taking exactly one data item as input")
            self._steps.append(step)
        return self

    def full_pipe(self) -> "Pipe":
        if self.depends_on:
            pipe = self.depends_on.full_pipe().steps
        else:
            pipe = []

        # return pipe with resolved dependencies
        pipe.extend(self._steps)
        return Pipe(self.name, pipe)


class PipeIterator(Iterator[PipeItem]):

    def __init__(self, max_parallelism: int = 100, worker_threads: int = 5, futures_poll_interval: float = 0.01) -> None:
        self.max_parallelism = max_parallelism
        self.worker_threads = worker_threads
        self.futures_poll_interval = futures_poll_interval

        self._async_pool: asyncio.AbstractEventLoop = None
        self._async_pool_thread: Thread = None
        self._thread_pool: ThreadPoolExecutor = None
        self._sources: List[SourcePipeItem] = []
        self._futures: List[FuturePipeItem] = []

    @classmethod
    def from_pipe(cls, pipe: Pipe, max_parallelism: int = 100, worker_threads: int = 5, futures_poll_interval: float = 0.01) -> "PipeIterator":
        if pipe.depends_on:
            pipe = pipe.full_pipe()
        # head must be iterator
        assert isinstance(pipe.head, Iterator)
        # create extractor
        extract = cls(max_parallelism, worker_threads, futures_poll_interval)
        # add as first source
        extract._sources.append(SourcePipeItem(pipe.head, 0, pipe))
        return extract

    @classmethod
    def from_pipes(cls, pipes: Sequence[Pipe], yield_parents: bool = True, max_parallelism: int = 100, worker_threads: int = 5, futures_poll_interval: float = 0.01) -> "PipeIterator":
        # as we add fork steps below, pipes are cloned before use
        pipes = [p.clone() for p in pipes]
        extract = cls(max_parallelism, worker_threads, futures_poll_interval)
        for pipe in reversed(pipes):
            if pipe.depends_on:
                # fork the parent pipe
                pipe.depends_on.fork(pipe)
                # make the parent yield by sending a clone of item to itself with position at the end
                if yield_parents:
                    pipe.depends_on.fork(pipe.depends_on, len(pipe.depends_on) - 1)
            else:
                # head of independent pipe must be iterator
                assert isinstance(pipe.head, Iterator)
                # add every head as source
                extract._sources.append(SourcePipeItem(pipe.head, 0, pipe))
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

            # if item is iterator, then add it as a new source
            if isinstance(pipe_item.item, Iterator):
                # print(f"adding iterable {item}")
                self._sources.append(SourcePipeItem(pipe_item.item, pipe_item.step, pipe_item.pipe))
                pipe_item = None
                continue

            if isinstance(pipe_item.item, Awaitable) or callable(pipe_item.item):
                # do we have a free slot or one of the slots is done?
                if len(self._futures) < self.max_parallelism or self._next_future() >= 0:
                    if isinstance(pipe_item.item, Awaitable):
                        future = asyncio.run_coroutine_threadsafe(pipe_item.item, self._ensure_async_pool())
                    else:
                        future = self._ensure_thread_pool().submit(pipe_item.item)
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
                if isinstance(pipe_item.item, (Iterator, Awaitable)) or callable(pipe_item.pipe):
                    raise PipeItemProcessingError("Pipe item not processed", pipe_item)
                # mypy not able to figure out that item was resolved
                return pipe_item  # type: ignore

            # advance to next step
            step = pipe_item.pipe[pipe_item.step + 1]
            assert callable(step)
            item = step(pipe_item.item)
            pipe_item = ResolvablePipeItem(item, pipe_item.step + 1, pipe_item.pipe)  # type: ignore


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

        self._thread_pool = ThreadPoolExecutor(self.worker_threads)
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


class PipeException(DltException):
    pass


class CreatePipeException(PipeException):
    pass


class PipeItemProcessingError(PipeException):
    pass

