import multiprocessing
from prometheus_client import Counter, Gauge, Summary, CollectorRegistry, REGISTRY
from typing import Callable, Dict, Union, cast
from multiprocessing.pool import ThreadPool, Pool

from dlt.common import logger, signals
from dlt.common.runners.runnable import Runnable, TPool
from dlt.common.time import sleep
from dlt.common.telemetry import TRunHealth, TRunMetrics, get_logging_extras, get_metrics_from_prometheus
from dlt.common.exceptions import SignalReceivedException, TimeRangeExhaustedException, UnsupportedProcessStartMethodException
from dlt.common.configuration.specs import PoolRunnerConfiguration


HEALTH_PROPS_GAUGES: Dict[str, Union[Counter, Gauge]] = None
RUN_DURATION_GAUGE: Gauge = None
RUN_DURATION_SUMMARY: Summary = None

LAST_RUN_METRICS: TRunMetrics = None
LAST_RUN_EXCEPTION: BaseException = None

def create_gauges(registry: CollectorRegistry) -> None:
    global HEALTH_PROPS_GAUGES, RUN_DURATION_GAUGE, RUN_DURATION_SUMMARY

    HEALTH_PROPS_GAUGES = {
        "runs_count": Counter("runs_count", "Count runs", registry=registry),
        "runs_not_idle_count": Counter("runs_not_idle_count", "Count not idle runs", registry=registry),
        "runs_healthy_count": Counter("runs_healthy_count", "Count healthy runs", registry=registry),
        "runs_cs_healthy_gauge": Gauge("runs_cs_healthy_gauge", "Count consecutive healthy runs, reset on failed run", registry=registry),
        "runs_failed_count": Counter("runs_failed_count", "Count failed runs", registry=registry),
        "runs_cs_failed_gauge": Gauge("runs_cs_failed_gauge", "Count consecutive failed runs, reset on healthy run", registry=registry),
        "runs_pending_items_gauge": Gauge("runs_pending_items_gauge", "Number of items pending at the end of the run", registry=registry),
    }

    RUN_DURATION_GAUGE = Gauge("runs_duration_seconds", "Duration of the run", registry=registry)
    RUN_DURATION_SUMMARY = Summary("runs_duration_summary", "Summary of the run duration", registry=registry)


def update_gauges() -> TRunHealth:
    return get_metrics_from_prometheus(HEALTH_PROPS_GAUGES.values())  # type: ignore


def run_pool(C: PoolRunnerConfiguration, run_f: Union[Runnable[TPool], Callable[[TPool], TRunMetrics]]) -> int:
    # validate run function
    if not isinstance(run_f, Runnable) and not callable(run_f):
        raise ValueError(run_f, "Pool runner entry point must be a function f(pool: TPool) or Runnable")

    # create health gauges
    if not HEALTH_PROPS_GAUGES:
        create_gauges(REGISTRY)

    # start pool
    pool: Pool = None
    if C.pool_type == "process":
        # our pool implementation do not work on spawn
        if multiprocessing.get_start_method() != "fork":
            raise UnsupportedProcessStartMethodException(multiprocessing.get_start_method())
        pool = Pool(processes=C.workers)
    elif C.pool_type == "thread":
        pool = ThreadPool(processes=C.workers)
    else:
        pool = None
    logger.info(f"Created {C.pool_type} pool with {C.workers or 'default no.'} workers")
    # track local stats
    runs_count = 0
    runs_not_idle_count = 0

    try:
        while True:
            run_metrics: TRunMetrics = None
            try:
                HEALTH_PROPS_GAUGES["runs_count"].inc()
                runs_count += 1
                # run pool logic
                logger.debug("Running pool")
                with RUN_DURATION_SUMMARY.time(), RUN_DURATION_GAUGE.time():
                    if callable(run_f):
                        run_metrics = run_f(cast(TPool, pool))
                    elif isinstance(run_f, Runnable):
                        run_metrics = run_f.run(cast(TPool, pool))
                    else:
                        raise SignalReceivedException(-1)
            except (Exception, SignalReceivedException) as exc:
                # the run failed
                run_metrics = TRunMetrics(True, True, -1)
                # preserve exception
                # TODO: convert it to callback
                global LAST_RUN_EXCEPTION
                LAST_RUN_EXCEPTION = exc
                if isinstance(exc, (SignalReceivedException, TimeRangeExhaustedException)):
                    # always exit
                    raise
                else:
                    logger.exception("run")
                    # re-raise if EXIT_ON_EXCEPTION is requested
                    if C.exit_on_exception:
                        raise
            finally:
                if run_metrics:
                    logger.debug(f"Pool ran with {run_metrics}")
                    _update_metrics(run_metrics)
                    runs_not_idle_count += int(not run_metrics.was_idle)

            # exit due to signal
            signals.raise_if_signalled()

            # single run may be forced but at least wait_runs must pass
            # and was all the time idle or (was not idle but now pending is 0)
            if C.is_single_run and (runs_count >= C.wait_runs and (runs_not_idle_count == 0 or run_metrics.pending_items == 0)):
                logger.info("Stopping runner due to single run override")
                return 0

            if run_metrics.has_failed:
                sleep(C.run_sleep_when_failed)
            elif run_metrics.pending_items == 0:
                # nothing is pending so we can sleep longer
                sleep(C.run_sleep_idle)
            else:
                # more items are pending, sleep (typically) shorter
                sleep(C.run_sleep)

            # this allows to recycle long living process that get their memory fragmented
            # exit after runners sleeps so we keep the running period
            if runs_count == C.stop_after_runs:
                logger.warning(f"Stopping runner due to max runs {runs_count} exceeded")
                return 0
    except SignalReceivedException as sigex:
        # sleep this may raise SignalReceivedException
        logger.warning(f"Exiting runner due to signal {sigex.signal_code}")
        return sigex.signal_code
    except TimeRangeExhaustedException as tre:
        logger.info(f"{str(tre)}, not further processing will be done")
        return 0
    finally:
        if pool:
            logger.info("Closing processing pool")
            # terminate pool and do not join
            pool.terminate()
            # in very rare cases process hangs here, even with starmap terminating earlier
            # pool.close()
            # pool.join()
            pool = None
            logger.info("Closing processing pool closed")


def _update_metrics(run_metrics: TRunMetrics) -> TRunHealth:
    # gather and emit metrics
    if not run_metrics.was_idle:
        HEALTH_PROPS_GAUGES["runs_not_idle_count"].inc()
    if run_metrics.has_failed:
        HEALTH_PROPS_GAUGES["runs_failed_count"].inc()
        HEALTH_PROPS_GAUGES["runs_cs_failed_gauge"].inc()
        HEALTH_PROPS_GAUGES["runs_cs_healthy_gauge"].set(0)
    else:
        HEALTH_PROPS_GAUGES["runs_healthy_count"].inc()
        HEALTH_PROPS_GAUGES["runs_cs_healthy_gauge"].inc()
        HEALTH_PROPS_GAUGES["runs_cs_failed_gauge"].set(0)
    HEALTH_PROPS_GAUGES["runs_pending_items_gauge"].set(run_metrics.pending_items)
    health_props = update_gauges()
    logger.metrics("progress", "health", extra={"metrics": health_props})
    logger.metrics("progress", "pool runner", extra=get_logging_extras([RUN_DURATION_GAUGE, RUN_DURATION_SUMMARY]))

    # preserve last run metrics
    global LAST_RUN_METRICS
    LAST_RUN_METRICS = run_metrics

    return health_props
