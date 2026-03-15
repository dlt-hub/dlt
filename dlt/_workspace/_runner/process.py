"""Job process management with log prefixing."""

import os
import queue
import signal
import subprocess
import sys
from threading import Thread
from typing import Dict, Iterator, List, Optional, Tuple


class JobProcess:
    """Wraps a subprocess for a single job run with threaded output reading."""

    DEFAULT_GRACE_PERIOD = 30.0

    def __init__(
        self,
        job_ref: str,
        cmd: List[str],
        env: Optional[Dict[str, str]] = None,
        grace_period: float = DEFAULT_GRACE_PERIOD,
    ) -> None:
        self.job_ref = job_ref
        self.cmd = cmd
        self.env = env
        self.grace_period = grace_period
        self._process: Optional[subprocess.Popen[str]] = None
        self._queue: queue.Queue[Optional[Tuple[int, str]]] = queue.Queue()
        self._threads: List[Thread] = []
        self._exit_code: Optional[int] = None

    @property
    def short_name(self) -> str:
        """Last component of job_ref for display."""
        from dlt._workspace.deployment._job_ref import short_name

        return short_name(self.job_ref)

    def start(self) -> None:
        """Start the subprocess and output reader threads."""
        merged_env = dict(os.environ)
        if self.env:
            merged_env.update(self.env)

        self._process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            text=True,
            errors="backslashreplace",
            env=merged_env,
        )

        for stream_no in (1, 2):
            t = Thread(target=self._read_stream, args=(stream_no,), daemon=True)
            t.start()
            self._threads.append(t)

    def _read_stream(self, stream_no: int) -> None:
        """Read lines from stdout (1) or stderr (2) into the queue."""
        stream = self._process.stderr if stream_no == 2 else self._process.stdout
        for line in iter(stream.readline, ""):
            self._queue.put((stream_no, line.rstrip("\n")))
        self._queue.put(None)

    def poll(self) -> Optional[int]:
        """Check if process has terminated. Returns exit code or None."""
        if self._exit_code is not None:
            return self._exit_code
        if self._process is not None:
            self._exit_code = self._process.poll()
        return self._exit_code

    def is_alive(self) -> bool:
        return self.poll() is None

    def drain_output(self) -> List[Tuple[int, str]]:
        """Get all available output lines without blocking."""
        lines: List[Tuple[int, str]] = []
        while True:
            try:
                item = self._queue.get_nowait()
                if item is not None:
                    lines.append(item)
            except queue.Empty:
                break
        return lines

    def terminate(self, grace_period: float = 5.0) -> None:
        """Send SIGTERM, wait grace period, then SIGKILL."""
        if self._process is None or self.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=grace_period)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
        self._exit_code = self._process.returncode

    def wait(self) -> int:
        """Wait for process to complete and return exit code."""
        if self._process is None:
            raise RuntimeError("process not started")
        self._process.wait()
        for t in self._threads:
            t.join()
        self._exit_code = self._process.returncode
        return self._exit_code
