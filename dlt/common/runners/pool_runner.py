from __future__ import annotations
import time
import multiprocessing
from typing import Any, Callable, Optional, Tuple, Union, cast, TypeVar
from concurrent.futures import Executor, ThreadPoolExecutor, Future
from typing_extensions import ParamSpec

from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.runtime import init
from dlt.common.runners.runnable import Runnable, TExecutor
from dlt.common.runners.configuration import PoolRunnerConfiguration
from dlt.common.runners.typing import TRunMetrics
from dlt.common.runtime import signals
from dlt.common.runtime.signals import sleep
from dlt.common.exceptions import SignalReceivedException
from dlt.common.typing import AnyFun
from dlt.common.runtime.exec_info import platform_supports_threading

T = TypeVar("T")
P = ParamSpec("P")


class NullExecutor(Executor):
    """Dummy executor that runs jobs single-threaded.

    Provides a uniform interface for `None` pool type
    """

    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Run the job and return a Future"""
        fut: Future[T] = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)
        return fut


class TimeoutThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(
        self,
        max_workers: int = None,
        timeout: Optional[float] = None,
        thread_name_prefix: str = "",
        initializer: AnyFun = None,
        initargs: Tuple[Any, ...] = (),
    ):
        """Initializes a new ThreadPoolExecutor instance.

        Args:
            max_workers: The maximum number of threads that can be used to
                execute the given calls.
            timeout: Waits for pool threads to complete, None to wait indefinite
            thread_name_prefix: An optional name prefix to give our threads.
            initializer: A callable used to initialize worker threads.
            initargs: A tuple of arguments to pass to the initializer.
        """
        super().__init__(
            max_workers,
            thread_name_prefix=thread_name_prefix,
            initializer=initializer,
            initargs=initargs,
        )
        self.timeout = timeout
        self._is_alive: bool = None  # set on shutdown

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if not wait or self.timeout is None:
            # behave exactly like the std-lib version
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)

        # initiate shutdown but return immediately
        super().shutdown(wait=False, cancel_futures=cancel_futures)

        deadline = time.monotonic() + self.timeout
        for t in list(self._threads):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            t.join(remaining)

        alive = [t for t in self._threads if t.is_alive()]
        if alive:
            logger.error(
                f"Shutdown of dlt thread pool {self._thread_name_prefix} could not be completed in"
                f" {self.timeout} seconds. All the tasks in the pool are completed, nevertheless"
                " some threads didn't join. This may happen, for example, if thread local storage "
                " is used with object with __del__ method that locks thread when garbage"
                " collecting or if os.fork() is used. NOTE: if you use database clients or other"
                " Python libraries in resource or custom destinations, they can do that without"
                " you knowing. WARNING! This process will still try to join those threads on exit"
                " and most probably will lock indefinitely."
            )
            logger.info(
                f"Shutdown of dlt thread pool {self._thread_name_prefix} - "
                f"{len(alive)} thread(s) still alive: {', '.join(t.name for t in alive)}."
            )
        self._is_alive = len(alive) > 0


def get_default_start_method(method_: str) -> str:
    """Sets method to `spawn` is running in one of orchestrator tasks.

    Called when explicit start method is not set on `PoolRunnerConfiguration`
    """
    if method_ == "fork":
        from dlt.common.runtime.exec_info import (
            is_running_in_airflow_task,
            is_running_in_dagster_task,
            is_running_in_prefect_flow,
        )

        for m_ in [
            is_running_in_airflow_task,
            is_running_in_dagster_task,
            is_running_in_prefect_flow,
        ]:
            if m_():
                logger.info(
                    f"Switching pool start method to `spawn` because `{m_.__name__}` is True"
                )
                return "spawn"
    return method_


def create_pool(config: PoolRunnerConfiguration) -> Executor:
    assert config.pool_type in ["process", "thread", "none", None]

    if not platform_supports_threading():
        logger.info(
            "Platform does not support threading, created single-threaded pool for execution."
        )
        return NullExecutor()

    executor: Executor = None
    if config.pool_type == "process":
        # import process pool only when needed. not all Python envs support process pools
        from concurrent.futures import ProcessPoolExecutor

        # if not fork method, provide initializer for logs and configuration
        start_method = config.start_method or get_default_start_method(
            multiprocessing.get_start_method()
        )
        if start_method != "fork":
            ctx = Container()[PluggableRunContext]
            executor = ProcessPoolExecutor(
                max_workers=config.workers,
                initializer=init.restore_run_context,
                initargs=(ctx.context, ctx.runtime_config),
                mp_context=multiprocessing.get_context(method=start_method),
            )
        else:
            executor = ProcessPoolExecutor(
                max_workers=config.workers, mp_context=multiprocessing.get_context()
            )
    elif config.pool_type == "thread":
        # wait 2 seconds before bailing out from pool shutdown
        executor = TimeoutThreadPoolExecutor(
            max_workers=config.workers,
            timeout=2.0,
            thread_name_prefix=Container.thread_pool_prefix(),
        )
    # no pool - single threaded
    else:
        executor = NullExecutor()

    logger.info(
        f"Created {config.pool_type or 'single-threaded'} pool with"
        f" {config.workers or 'default no.'} workers"
    )
    return executor


def run_pool(
    config: PoolRunnerConfiguration,
    run_f: Union[Runnable[TExecutor], Callable[[TExecutor], TRunMetrics]],
) -> int:
    # validate the run function
    if not isinstance(run_f, Runnable) and not callable(run_f):
        raise ValueError(
            run_f, "Pool runner entry point must be a function `f(pool: TPool)` or `Runnable`"
        )

    # start pool
    pool = create_pool(config)
    runs_count = 1

    def _run_func() -> bool:
        if callable(run_f):
            run_metrics = run_f(cast(TExecutor, pool))
        elif isinstance(run_f, Runnable):
            run_metrics = run_f.run(cast(TExecutor, pool))
        else:
            raise SignalReceivedException(-1)
        return run_metrics.pending_items > 0

    try:
        logger.debug("Running pool")
        while _run_func():
            # for next run
            signals.raise_if_signalled()
            runs_count += 1
            sleep(config.run_sleep)
        return runs_count
    except SignalReceivedException as sigex:
        # sleep this may raise SignalReceivedException
        logger.warning(f"Exiting runner due to signal {sigex.signal_code}")
        raise
    finally:
        if pool:
            logger.info("Closing processing pool")
            pool.shutdown(wait=True)
            pool = None
            logger.info("Processing pool closed")
