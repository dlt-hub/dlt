from __future__ import annotations
import multiprocessing
from typing import Callable, Union, cast, TypeVar
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, Future
from typing_extensions import ParamSpec

from dlt.common import logger, sleep
from dlt.common.configuration.container import Container
from dlt.common.runtime import init
from dlt.common.runners.runnable import Runnable, TExecutor
from dlt.common.runners.configuration import PoolRunnerConfiguration
from dlt.common.runners.typing import TRunMetrics
from dlt.common.runtime import signals
from dlt.common.exceptions import SignalReceivedException


T = TypeVar("T")
P = ParamSpec("P")


class NullExecutor(Executor):
    """Dummy executor that runs jobs single-threaded.

    Provides a uniform interface for `None` pool type
    """

    def submit(self, fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Run the job and return a Future"""
        fut: Future[T] = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)
        return fut


def create_pool(config: PoolRunnerConfiguration) -> Executor:
    if config.pool_type == "process":
        # if not fork method, provide initializer for logs and configuration
        start_method = config.start_method or multiprocessing.get_start_method()
        if start_method != "fork" and init._INITIALIZED:
            return ProcessPoolExecutor(
                max_workers=config.workers,
                initializer=init.initialize_runtime,
                initargs=(init._RUN_CONFIGURATION,),
                mp_context=multiprocessing.get_context(method=start_method),
            )
        else:
            return ProcessPoolExecutor(
                max_workers=config.workers, mp_context=multiprocessing.get_context()
            )
    elif config.pool_type == "thread":
        return ThreadPoolExecutor(
            max_workers=config.workers, thread_name_prefix=Container.thread_pool_prefix()
        )
    # no pool - single threaded
    return NullExecutor()


def run_pool(
    config: PoolRunnerConfiguration,
    run_f: Union[Runnable[TExecutor], Callable[[TExecutor], TRunMetrics]],
) -> int:
    # validate the run function
    if not isinstance(run_f, Runnable) and not callable(run_f):
        raise ValueError(
            run_f, "Pool runner entry point must be a function f(pool: TPool) or Runnable"
        )

    # start pool
    pool = create_pool(config)
    logger.info(f"Created {config.pool_type} pool with {config.workers or 'default no.'} workers")
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
