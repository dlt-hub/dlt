from typing import Optional

from concurrent.futures import Executor, ThreadPoolExecutor

from dlt.common.runners.pool_runner import NullExecutor
from dlt.common.runtime.exec_info import platform_supports_threading


class ManagedThreadPool:
    def __init__(self, thread_name_prefix: str, max_workers: int = 1) -> None:
        self.thread_name_prefix = thread_name_prefix
        self._max_workers = max_workers
        self._thread_pool: Optional[Executor] = None

    def _create_thread_pool(self) -> None:
        assert not self._thread_pool, "Thread pool already created"
        # a background pool keeps its single thread, only a platform without threads runs inline
        if platform_supports_threading():
            self._thread_pool = ThreadPoolExecutor(
                self._max_workers, thread_name_prefix=self.thread_name_prefix
            )
        else:
            self._thread_pool = NullExecutor()

    @property
    def thread_pool(self) -> Executor:
        if not self._thread_pool:
            self._create_thread_pool()
        return self._thread_pool

    def stop(self, wait: bool = True) -> None:
        if self._thread_pool:
            self._thread_pool.shutdown(wait=wait)
            self._thread_pool = None
