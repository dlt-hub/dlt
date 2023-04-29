import multiprocessing
from typing import Callable, Union, cast
from multiprocessing.pool import ThreadPool, Pool

from dlt.common import logger, sleep
from dlt.common.runtime import init
from dlt.common.runners.runnable import Runnable, TPool
from dlt.common.runners.configuration import PoolRunnerConfiguration
from dlt.common.runners.typing import TRunMetrics
from dlt.common.runtime import signals
from dlt.common.exceptions import SignalReceivedException


def create_pool(config: PoolRunnerConfiguration) -> Pool:
    if config.pool_type == "process":
        # if not fork method, provide initializer for logs and configuration
        if multiprocessing.get_start_method() != "fork" and init._INITIALIZED:
            return Pool(processes=config.workers, initializer=init.initialize_runtime, initargs=(init._RUN_CONFIGURATION, ))
        else:
            return Pool(processes=config.workers)
    elif config.pool_type == "thread":
        return ThreadPool(processes=config.workers)
    # no pool - single threaded
    return None


def run_pool(config: PoolRunnerConfiguration, run_f: Union[Runnable[TPool], Callable[[TPool], TRunMetrics]]) -> int:
    # validate the run function
    if not isinstance(run_f, Runnable) and not callable(run_f):
        raise ValueError(run_f, "Pool runner entry point must be a function f(pool: TPool) or Runnable")

    # start pool
    pool = create_pool(config)
    logger.info(f"Created {config.pool_type} pool with {config.workers or 'default no.'} workers")
    runs_count = 1

    def _run_func() -> bool:
        if callable(run_f):
            run_metrics = run_f(cast(TPool, pool))
        elif isinstance(run_f, Runnable):
            run_metrics = run_f.run(cast(TPool, pool))
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
            # terminate pool and do not join
            pool.terminate()
            # in very rare cases process hangs here, even with starmap terminating earlier
            # pool.close()
            # pool.join()
            pool = None
            logger.info("Processing pool closed")
