import os
import pytest
import multiprocessing
from time import sleep
from typing import Iterator, Tuple
from multiprocessing.pool import Pool

from dlt.common import logger
from dlt.common.runners.runnable import Runnable, workermethod
from dlt.common.telemetry import TRunMetrics
from dlt.common.utils import uniq_id

ALL_METHODS = multiprocessing.get_all_start_methods()


@pytest.fixture(autouse=True)
def mp_method_auto() -> Iterator[None]:
    method = multiprocessing.get_start_method()
    yield
    multiprocessing.set_start_method(method, force=True)


class _TestRunnableWorkerMethod(Runnable):

    def __init__(self, tasks: int) -> None:
        self.uniq = uniq_id()
        self.tasks = tasks
        self.rv = None

    @staticmethod
    @workermethod
    def worker(self: "_TestRunnableWorkerMethod", v: int) -> Tuple[int, str, int]:
        # sleep a while to force starmap to schedule tasks to separate workers
        sleep(0.3)
        return (v, self.uniq, os.getpid())

    def _run(self, pool: Pool) -> Iterator[Tuple[int, str, int]]:
        rid = id(self)
        assert rid in _TestRunnableWorkerMethod.RUNNING
        self.rv = rv = pool.starmap(_TestRunnableWorkerMethod.worker, [(rid, i) for i in range(self.tasks)])
        assert rid in _TestRunnableWorkerMethod.RUNNING
        return rv

    def run(self, pool: Pool) -> TRunMetrics:
        self._run(pool)
        return TRunMetrics(False, False, 0)


class _TestRunnableWorker(Runnable):

    def __init__(self, tasks: int) -> None:
        self.tasks = tasks
        self.rv = None

    @staticmethod
    def worker(v: int) -> Tuple[int, int]:
        # sleep a while to force starmap to schedule tasks to separate workers
        logger.info(f"_TestRunnableWorker worker {v} pid {os.getpid()}")
        sleep(0.3)
        return (v, os.getpid())

    def _run(self, pool: Pool) -> Iterator[Tuple[int, str, int]]:
        self.rv = rv = pool.starmap(_TestRunnableWorker.worker, [(i, ) for i in range(self.tasks)])
        return rv

    def run(self, pool: Pool) -> TRunMetrics:
        self._run(pool)
        return TRunMetrics(False, False, 0)
