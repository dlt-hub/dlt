import os
from time import sleep
from typing import Iterator, Tuple
from multiprocessing.pool import Pool

from dlt.common.runners.runnable import Runnable, workermethod
from dlt.common.telemetry import TRunMetrics
from dlt.common.utils import uniq_id


class _TestRunnable(Runnable):

    def __init__(self, tasks: int) -> None:
        self.uniq = uniq_id()
        self.tasks = tasks
        self.rv = None

    @staticmethod
    @workermethod
    def worker(self: "_TestRunnable", v: int) -> Tuple[int, str, int]:
        # sleep a while to force starmap to schedule tasks to separate workers
        sleep(0.3)
        return (v, self.uniq, os.getpid())

    def _run(self, pool: Pool) -> Iterator[Tuple[int, str, int]]:
        rid = id(self)
        assert rid in _TestRunnable.RUNNING
        self.rv = rv = pool.starmap(_TestRunnable.worker, [(rid, i) for i in range(self.tasks)])
        assert rid in _TestRunnable.RUNNING
        return rv

    def run(self, pool: Pool) -> TRunMetrics:
        self._run(pool)
        return TRunMetrics(False, False, 0)
