import asyncio
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    wait as wait_for_futures,
)
import contextvars
from threading import Thread
from typing import Awaitable, Dict, Optional

from dlt.common.exceptions import PipelineException
from dlt.common.configuration.container import Container
from dlt.common.runners.pool_runner import TimeoutThreadPoolExecutor
from dlt.common.runtime.signals import sleep
from dlt.extract.items import DataItemWithMeta, TItemFuture, ResolvablePipeItem, FuturePipeItem

from dlt.extract.exceptions import (
    DltSourceException,
    ExtractorException,
    PipeException,
    ResourceExtractionError,
)


class FuturesPool:
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

        self._thread_pool = TimeoutThreadPoolExecutor(
            self.workers,
            timeout=2.0,
            thread_name_prefix=Container.thread_pool_prefix() + "extract-threads",
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
            name=Container.thread_pool_prefix() + "extract-futures",
        )
        self._async_pool_thread.start()

        # start or return async pool
        return self._async_pool

    def _vacate_slot(self, _: TItemFuture) -> None:
        # Used as callback to free up slot when future is done
        self.used_slots -= 1

    def submit(self, pipe_item: ResolvablePipeItem) -> TItemFuture:
        """Submit an item to the pool.

        Args:
            pipe_item: The pipe item to submit. `pipe_item.item` must be either an asyncio coroutine or a callable.

        Returns:
            The resulting future object
        """

        # Sanity check, negative free slots means there's a bug somewhere
        assert self.free_slots >= 0, "Worker pool has negative free slots, this should never happen"

        if self.free_slots == 0:
            # Wait until some future is completed to ensure there's a free slot
            # Note: This is probably not thread safe. If ever multiple threads will be submitting
            # jobs to the pool, we ned to change this whole method to be inside a `threading.Lock`
            self._wait_for_free_slot()

        future: Optional[TItemFuture] = None

        # submit to thread pool or async pool
        item = pipe_item.item
        if isinstance(item, Awaitable):
            future = asyncio.run_coroutine_threadsafe(item, self._ensure_async_pool())
        elif callable(item):
            # pass pipe context to thread pool, happens automatically for coroutines
            ctx = contextvars.copy_context()
            future = self._ensure_thread_pool().submit(ctx.run, item)
        else:
            raise ValueError(f"Unsupported item type: `{type(item)}`")

        # Future is not removed from self.futures until it's been consumed by the
        # pipe iterator. But we always want to vacate a slot so new jobs can be submitted
        future.add_done_callback(self._vacate_slot)
        self.used_slots += 1

        self.futures[future] = FuturePipeItem(
            future, pipe_item.step, pipe_item.pipe, pipe_item.meta
        )
        return future

    def sleep(self) -> None:
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

    def _next_done_future(self) -> Optional[TItemFuture]:
        """Get the done future in the pool (if any). This does not block."""
        return next((fut for fut in self.futures if fut.done() and not fut.cancelled()), None)

    def resolve_next_future(
        self, use_configured_timeout: bool = False
    ) -> Optional[ResolvablePipeItem]:
        """Block until the next future is done and return the result. Returns None if no futures done.

        Args:
            use_configured_timeout: If True, use the value of `self.poll_interval` as the max wait time,
                raises `concurrent.futures.TimeoutError` if no future is done within that time.

        Returns:
            The resolved future item or None if no future is done.
        """
        if not self.futures:
            return None

        if (future := self._next_done_future()) is not None:
            # When there are multiple already done futures from the same pipe we return results in insertion order
            return self._resolve_future(future)
        for future in as_completed(
            self.futures, timeout=self.poll_interval if use_configured_timeout else None
        ):
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
        future = self._next_done_future()
        if not future:
            return None

        return self._resolve_future(future)

    def _wait_for_free_slot(self) -> None:
        """Wait until any future in the pool is completed to ensure there's a free slot."""
        if self.free_slots >= 1:
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
