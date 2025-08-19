import os
import sys
import logging
import time
import weakref
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    TextIO,
    Union,
)

from dlt.common import logger as dlt_logger
from dlt.common.configuration import with_config, known_sections, configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.runtime.collector import LogCollector

# Global registry to track active BufferedDataWriter instances
_ACTIVE_WRITERS: Optional[weakref.WeakSet[Any]] = None


def register_buffered_writer(writer: Any) -> None:
    """Register a BufferedDataWriter instance for memory-based flushing"""
    global _ACTIVE_WRITERS
    if _ACTIVE_WRITERS is None:
        _ACTIVE_WRITERS = weakref.WeakSet()
    _ACTIVE_WRITERS.add(writer)


def get_active_writers() -> Set[Any]:
    """Get all currently active BufferedDataWriter instances"""
    global _ACTIVE_WRITERS
    if _ACTIVE_WRITERS is None:
        return set()
    return set(_ACTIVE_WRITERS)


@configspec
class MemoryAwareCollectorConfiguration(BaseConfiguration):
    """Configuration for memory-aware collector"""

    max_memory_mb: Optional[int] = None  # RAM limit in MB (None disables monitoring)
    memory_check_interval: Optional[float] = 2.0  # How often to check memory (seconds)
    flush_threshold_percent: Optional[float] = 0.8  # Flush when this % of limit is reached


class MemoryAwareCollector(LogCollector):
    """A Collector that monitors RAM usage and flushes buffers when memory limits are exceeded"""

    @with_config(spec=MemoryAwareCollectorConfiguration, sections=known_sections.DATA_WRITER)
    def __init__(
        self,
        log_period: float = 1.0,
        logger: Union[logging.Logger, TextIO] = sys.stdout,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
        max_memory_mb: Optional[int] = None,
        memory_check_interval: Optional[float] = 2.0,
        flush_threshold_percent: Optional[float] = 0.8,
    ) -> None:
        """
        Memory-aware collector that extends LogCollector with RAM monitoring.

        Args:
            max_memory_mb: Maximum memory usage in MB before triggering buffer flushes
            memory_check_interval: How often to check memory usage (seconds). Defaults to 5.0
            flush_threshold_percent: Flush buffers when this percentage of memory limit is reached. Defaults to 0.8 (80%)
        """
        super().__init__(log_period, logger, log_level, dump_system_stats)

        # Initialize parent class state (normally done in _start)
        from collections import defaultdict

        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = time.time()
        self.step = "Memory Monitoring"  # Set a default step name

        # Set memory monitoring configuration
        self.max_memory_mb = max_memory_mb
        self.memory_check_interval = memory_check_interval
        self.flush_threshold_percent = flush_threshold_percent
        self.last_memory_check: float = 0

        # Track if we've enabled memory monitoring
        # Check if psutil is available
        try:
            import psutil

            self.memory_monitoring_enabled = self.max_memory_mb is not None

            if self.memory_monitoring_enabled:
                self._log(
                    logging.INFO,
                    f"Memory monitoring enabled with limit: {self.max_memory_mb}MB "
                    f"(flush threshold: {self.flush_threshold_percent*100:.1f}%)",
                )
        except ImportError:
            self.memory_monitoring_enabled = False
            self._log(logging.WARNING, "`psutil` not available. Memory monitoring disabled.")

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        """Update counters and check memory usage if enabled"""
        # Call parent update method
        super().update(name, inc, total, inc_total, message, label)

        # Check memory usage periodically
        if self.memory_monitoring_enabled:
            self._check_memory_usage()

    def _check_memory_usage(self) -> None:
        """Check current memory usage and flush buffers if necessary"""
        current_time = time.time()
        if current_time - self.last_memory_check < self.memory_check_interval:
            return

        self.last_memory_check = current_time

        try:
            import psutil

            process = psutil.Process(os.getpid())
            current_mem_mb = process.memory_info().rss / (1024**2)

            threshold_mb = self.max_memory_mb * self.flush_threshold_percent

            if current_mem_mb > threshold_mb:
                self._log(
                    logging.WARNING,
                    f"Memory usage ({current_mem_mb:.1f}MB) exceeds threshold "
                    f"({threshold_mb:.1f}MB). Flushing buffers...",
                )
                self._flush_all_buffers()

                # Check again after flushing
                new_mem_mb = psutil.Process(os.getpid()).memory_info().rss / (1024**2)
                freed_mb = current_mem_mb - new_mem_mb
                self._log(
                    logging.INFO,
                    f"Buffer flush completed. Memory freed: {freed_mb:.1f}MB. "
                    f"Current usage: {new_mem_mb:.1f}MB",
                )

        except ImportError:
            # psutil not available, disable memory monitoring
            self.memory_monitoring_enabled = False
            self._log(logging.WARNING, "psutil not available. Memory monitoring disabled.")
        except Exception as e:
            self._log(logging.ERROR, f"Error checking memory usage: {e}")

    def _flush_all_buffers(self) -> None:
        """Flush all registered BufferedDataWriter instances"""
        active_writers = get_active_writers()
        flushed_count = 0
        total_items_flushed = 0

        for writer in active_writers:
            try:
                if hasattr(writer, "_flush_items") and hasattr(writer, "_buffered_items"):
                    buffered_count = getattr(writer, "_buffered_items_count", 0)

                    if writer._buffered_items:  # Only flush if there are items
                        writer._flush_items()
                        flushed_count += 1
                        total_items_flushed += buffered_count
            except Exception as e:
                self._log(logging.ERROR, f"Error flushing buffer for writer {writer}: {e}")

        if flushed_count > 0:
            self._log(
                logging.INFO,
                f"Flushed {flushed_count} buffered writers ({total_items_flushed} items) due to"
                " memory pressure",
            )
        elif len(active_writers) > 0:
            self._log(
                logging.DEBUG,
                f"No buffered data found to flush in {len(active_writers)} active writers",
            )
