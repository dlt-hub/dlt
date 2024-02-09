import asyncio
from uuid import uuid4
from asyncio import Future
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    wait as wait_for_futures,
    FIRST_COMPLETED,
)
from threading import Thread
from typing import List, Awaitable, Callable, Any, Dict, Set, Optional

from dlt.common.exceptions import PipelineException
from dlt.common.configuration.container import Container
from dlt.common.runtime.signals import sleep
from dlt.extract.typing import DataItemWithMeta, TItemFuture
from dlt.extract.items import ResolvablePipeItem, FuturePipeItem

from dlt.extract.exceptions import (
    DltSourceException,
    ExtractorException,
    PipeException,
    ResourceExtractionError,
)


class WorkerPool:
    """Worker pool for pipe items that can be resolved asynchronously.

    Items can be either asyncio coroutines or regular callables which will be executed in a thread pool.
    """

    def __init__(
        self, workers: int = 5, poll_interval: float = 0.01, max_parallel_items: int = 20
    ) -> None:
        self.futures: Dict[TItemFuture, FuturePipeItem] = {}
        self._thread_pool: ThreadPoolExecutor = None
        self._async_pool: asyncio.AbstractEventLoop = None
        self._async_pool_thread: Thread = None
        self.workers = workers
        self.poll_interval = poll_interval
        self.max_parallel_items = max_parallel_items
        self.used_slots: int = 0

    def __len__(self) -> int:
        return len(self.futures)

    @property
    def free_slots(self) -> int:
        # Done futures don't count as slots, so we can still add futures
        return self.max_parallel_items - self.used_slots

    @property
    def empty(self) -> bool:
        return len(self.futures) == 0

    def _ensure_thread_pool(self) -> ThreadPoolExecutor:
        # lazily start or return thread pool
        if self._thread_pool:
            return self._thread_pool

        self._thread_pool = ThreadPoolExecutor(
            self.workers, thread_name_prefix=Container.thread_pool_prefix() + "threads"
        )
        return self._thread_pool

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

    def _vacate_slot(self, _: TItemFuture) -> None:
        # Used as callback to free up slot when future is done
        self.used_slots -= 1

    def submit(self, pipe_item: ResolvablePipeItem) -> Optional[TItemFuture]:
        """Submit an item to the pool.

        Args:
            pipe_item: The item to submit.

        Returns:
            The future if the item was successfully submitted, otherwise None.
        """
        if self.free_slots == 0:
            return None

        if self.free_slots < 0:  # TODO: Sanity check during dev, should never happen
            raise RuntimeError("Used more slots than max_parallel_items")

        # submit to thread pool or async pool
        item = pipe_item.item
        if isinstance(item, Awaitable):
            future = asyncio.run_coroutine_threadsafe(item, self._ensure_async_pool())
        elif callable(item):
            future = self._ensure_thread_pool().submit(item)
        else:
            raise ValueError(f"Unsupported item type: {type(item)}")

        # Future is not removed from self.futures until it's been consumed by the
        # pipe iterator. But we always want to vacate a slot so new jobs can be submitted
        future.add_done_callback(self._vacate_slot)
        self.used_slots += 1

        self.futures[future] = FuturePipeItem(
            future, pipe_item.step, pipe_item.pipe, pipe_item.meta
        )
        return future

    def poll(self) -> None:
        sleep(self.poll_interval)

    def _resolve_future(self, future: TItemFuture) -> Optional[ResolvablePipeItem]:
        future, step, pipe, meta = self.futures.pop(future)

        if ex := future.exception():
            if isinstance(ex, StopAsyncIteration):
                return None
            # Raise if any future fails
            if isinstance(
                ex, (PipelineException, ExtractorException, DltSourceException, PipeException)
            ):
                raise ex
            raise ResourceExtractionError(pipe.name, future, str(ex), "future") from ex

        item = future.result()

        if item is None:
            return None
        elif isinstance(item, DataItemWithMeta):
            return ResolvablePipeItem(item.data, step, pipe, item.meta)
        else:
            return ResolvablePipeItem(item, step, pipe, meta)

    def resolve_next_future(self) -> Optional[ResolvablePipeItem]:
        """Wait for the next future to be done. Returns None if no futures done."""
        if not self.futures:
            return None

        if (item := self.resolve_next_future_no_wait()) is not None:
            return item
        for future in as_completed(self.futures):
            if future.cancelled():
                # Get the next not-cancelled future
                continue

            return self._resolve_future(future)

        return None

    def resolve_next_future_no_wait(self) -> Optional[ResolvablePipeItem]:
        """Resolve the first done future in the pool.
        This does not block and returns None if no future is done.
        """
        # Get next done future
        future = next((fut for fut in self.futures if fut.done() and not fut.cancelled()), None)
        if not future:
            return None

        return self._resolve_future(future)

    def wait_for_free_slot(self) -> None:
        """Wait until any future in the pool is completed to ensure there's a free slot."""
        if self.free_slots >= 1:
            self.poll()  # Just sleep a bit to give other threads a chance
            return

        for future in as_completed(self.futures):
            if future.cancelled():
                # Get the next not-cancelled future
                continue
            if self.free_slots == 0:
                # Future was already completed so slot was not freed
                continue
            return  # Return when first future completes

    def close(self) -> None:
        # Cancel all futures
        for f in self.futures:
            if not f.done():
                f.cancel()

        def stop_background_loop(loop: asyncio.AbstractEventLoop) -> None:
            loop.stop()

        if self._async_pool:
            # wait for all async generators to be closed
            future = asyncio.run_coroutine_threadsafe(
                self._async_pool.shutdown_asyncgens(), self._ensure_async_pool()
            )

            wait_for_futures([future])
            self._async_pool.call_soon_threadsafe(stop_background_loop, self._async_pool)

            self._async_pool_thread.join()
            self._async_pool = None
            self._async_pool_thread = None

        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

        self.futures.clear()
