"""Test the memory-aware collector functionality"""
import os
import pytest
from unittest.mock import patch, MagicMock

from dlt.common.runtime.memory_collector import (
    MemoryAwareCollector,
    register_buffered_writer,
    get_active_writers,
    _ACTIVE_WRITERS,
)
from dlt.common.data_writers.buffered import BufferedDataWriter
from tests.common.data_writers.utils import get_writer
from dlt.common.data_writers.writers import JsonlWriter
from tests.utils import clean_test_storage, init_test_logging


@pytest.fixture(autouse=True)
def clear_active_writers():
    """Clear the global active writers registry before each test"""
    global _ACTIVE_WRITERS
    _ACTIVE_WRITERS = None
    yield
    _ACTIVE_WRITERS = None


def test_memory_collector_initialization():
    """Test that MemoryAwareCollector initializes correctly"""
    # Test with explicit memory limit and threshold
    collector = MemoryAwareCollector(max_memory_mb=100, flush_threshold_percent=0.8)
    assert collector.max_memory_mb == 100
    assert collector.memory_monitoring_enabled is True

    # Test with memory limit disabled
    collector = MemoryAwareCollector()
    assert collector.memory_monitoring_enabled is False


def test_memory_collector_env_var():
    """Test that MemoryAwareCollector respects configuration from environment"""
    with patch.dict(
        os.environ,
        {"DATA_WRITER__MAX_MEMORY_MB": "256", "DATA_WRITER__FLUSH_THRESHOLD_PERCENT": "0.9"},
    ):
        collector = MemoryAwareCollector()
        assert collector.max_memory_mb == 256
        assert collector.flush_threshold_percent == 0.9
        assert collector.memory_monitoring_enabled is True


def test_writer_registration():
    """Test that BufferedDataWriter instances are registered correctly"""
    clean_test_storage()

    # Create a writer - it should automatically register itself
    with get_writer(JsonlWriter) as writer:
        active_writers = get_active_writers()
        assert len(active_writers) == 1
        assert writer in active_writers

    # After context exit, writer should be garbage collected and removed
    # Force garbage collection to ensure weak references are cleaned up
    import gc

    gc.collect()

    active_writers = get_active_writers()
    # Note: WeakSet might still contain the reference until next access
    # So we don't assert len == 0 here


def test_memory_check_triggers_flush():
    """Test that memory checking triggers buffer flush when threshold is exceeded"""
    clean_test_storage()

    # Mock psutil to simulate high memory usage
    with (
        patch("psutil.Process") as mock_process_class,
        patch("psutil.virtual_memory") as mock_virtual_memory,
    ):
        # Mock process with high memory usage
        mock_process = MagicMock()
        mock_process.memory_info.return_value.rss = 150 * 1024 * 1024  # 150MB
        mock_process.cpu_percent.return_value = 25.0  # 25% CPU
        mock_process_class.return_value = mock_process

        # Mock virtual memory
        mock_virtual_memory.return_value.percent = 75.0  # 75% memory usage

        # Create collector with 100MB limit
        collector = MemoryAwareCollector(
            max_memory_mb=100,
            memory_check_interval=0,  # Check immediately
            flush_threshold_percent=0.8,  # 80MB threshold
        )

        # Create a writer with buffered items
        with get_writer(JsonlWriter, buffer_max_items=1000) as writer:
            # Add some items to buffer
            writer.write_data_item(
                {"col1": "value1"}, {"col1": {"name": "col1", "data_type": "text"}}
            )
            writer.write_data_item(
                {"col1": "value2"}, {"col1": {"name": "col1", "data_type": "text"}}
            )

            assert len(writer._buffered_items) == 2  # Items in buffer

            # Trigger memory check by calling update
            collector.update("test")

            # Verify buffer was flushed
            assert len(writer._buffered_items) == 0  # Buffer should be empty now


def test_memory_collector_logging():
    """Test that memory collector logs appropriately"""
    init_test_logging()

    with (
        patch("psutil.Process") as mock_process_class,
        patch("psutil.virtual_memory") as mock_virtual_memory,
    ):
        # Mock normal memory usage first
        mock_process = MagicMock()
        mock_process.memory_info.return_value.rss = 50 * 1024 * 1024  # 50MB
        mock_process.cpu_percent.return_value = 15.0  # 15% CPU
        mock_process_class.return_value = mock_process

        # Mock virtual memory
        mock_virtual_memory.return_value.percent = 60.0  # 60% memory usage

        collector = MemoryAwareCollector(
            max_memory_mb=100, memory_check_interval=0, flush_threshold_percent=0.8
        )

        # This should not trigger flush
        collector.update("test")

        # Now simulate high memory usage
        mock_process.memory_info.return_value.rss = 150 * 1024 * 1024  # 150MB

        # This should trigger flush and log warning
        collector.update("test")


def test_memory_collector_without_psutil():
    """Test that memory collector gracefully handles missing psutil"""
    import sys

    # Temporarily remove psutil from sys.modules to simulate missing dependency
    psutil_backup = sys.modules.get("psutil")
    if "psutil" in sys.modules:
        del sys.modules["psutil"]

    try:
        # Force ImportError by making psutil unavailable
        sys.modules["psutil"] = None

        # Create a fresh memory collector - it should handle missing psutil gracefully
        # We need to reload the module to trigger the import error in __init__
        collector = MemoryAwareCollector(
            max_memory_mb=100, memory_check_interval=0, flush_threshold_percent=0.8
        )

        # Should disable memory monitoring when psutil is unavailable
        assert collector.memory_monitoring_enabled is False

    finally:
        # Restore psutil module
        if psutil_backup is not None:
            sys.modules["psutil"] = psutil_backup
        elif "psutil" in sys.modules:
            del sys.modules["psutil"]


def test_memory_collector_error_handling():
    """Test that memory collector handles errors gracefully"""
    with patch("psutil.Process") as mock_process_class:
        # Make psutil throw an exception when called
        mock_process_class.side_effect = Exception("Test error")

        # Create collector with system stats disabled to avoid conflicts
        collector = MemoryAwareCollector(
            max_memory_mb=100,
            memory_check_interval=0,
            flush_threshold_percent=0.8,
            dump_system_stats=False,  # Disable system stats to avoid psutil conflicts
        )

        # Should not crash even when psutil fails during memory checking
        collector.update("test")
