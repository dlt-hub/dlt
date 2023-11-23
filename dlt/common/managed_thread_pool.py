from typing import Optional

import atexit
from concurrent.futures import ThreadPoolExecutor


class ManagedThreadPool:
    def __init__(self, max_workers: int = 1) -> None:
        self._max_workers = max_workers
        self._thread_pool: Optional[ThreadPoolExecutor] = None

    def _create_thread_pool(self) -> None:
        assert not self._thread_pool, "Thread pool already created"
        self._thread_pool = ThreadPoolExecutor(self._max_workers)
        # flush pool on exit
        atexit.register(self.stop)

    @property
    def thread_pool(self) -> ThreadPoolExecutor:
        if not self._thread_pool:
            self._create_thread_pool()
        return self._thread_pool

    def stop(self, wait: bool = True) -> None:
        if self._thread_pool:
            self._thread_pool.shutdown(wait=wait)
            self._thread_pool = None
