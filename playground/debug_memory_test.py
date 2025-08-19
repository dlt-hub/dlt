#!/usr/bin/env python3
"""
Debug Memory Test
================

This script tests the memory collector with realistic conditions and debugging.
"""

import os
import time
import dlt
from dlt.common.runtime.memory_collector import MemoryAwareCollector


def create_large_records(count=1000):
    """Create records that will consume significant memory"""
    # Create some memory pressure while generating data
    memory_hog = []

    for i in range(count):
        # Occasionally add large objects to memory to simulate pressure
        if i % 100 == 0:
            memory_hog.append("X" * 100000)  # Add 100KB chunks

        record = {
            "id": i,
            "large_data": "X" * 2000,  # 2KB per record
            "nested": {
                "field1": f"data_{i}" * 50,  # Another ~250 bytes
                "field2": list(range(50)),  # More list data
                "field3": {"sub": f"more_data_{i}" * 100},
            },
        }

        # Keep some data in memory to simulate pressure
        if len(memory_hog) > 20:  # Keep ~2MB in memory
            memory_hog.pop(0)

        yield record


def main():
    """Test memory limiting with debugging"""

    # Use a higher memory limit that's above current usage
    print("Setting up memory-limited pipeline...")

    # CRITICAL: Set small buffer sizes to force buffering behavior
    os.environ["DLT_DATA_WRITER__BUFFER_MAX_ITEMS"] = "10"  # Very small buffer!
    os.environ["DLT_DATA_WRITER__FILE_MAX_ITEMS"] = "100"  # Small files too

    # Get current memory usage first
    try:
        import psutil

        current_mem = psutil.Process(os.getpid()).memory_info().rss / (1024**2)
        print(f"Current memory usage: {current_mem:.1f}MB")

        # Set limit higher than current usage + some buffer
        memory_limit = int(current_mem + 50)  # Add only 50MB buffer for faster triggering
        print(f"Setting memory limit to: {memory_limit}MB")

    except ImportError:
        memory_limit = 1024  # Default to 1GB if psutil not available
        print(f"psutil not available, using default limit: {memory_limit}MB")

    # Create collector with debugging
    collector = MemoryAwareCollector(
        max_memory_mb=memory_limit,
        memory_check_interval=0.5,  # Check very frequently
        flush_threshold_percent=0.8,  # Flush at 80% of limit
        log_period=1.0,  # Log frequently
    )

    # Create pipeline with custom buffer settings
    pipeline = dlt.pipeline(
        pipeline_name="memory_debug_test",
        destination="duckdb",
        dataset_name="memory_debug",
        # progress=collector,
        progress="log",
    )

    print("Creating data source...")

    @dlt.source
    def memory_test_source():
        return dlt.resource(
            create_large_records(5000),  # 5K records = ~10MB+ of data
            name="large_table",
            write_disposition="replace",
        )

    print("Starting pipeline run...")
    print("Watch for DEBUG messages showing buffer status...")

    # Run the pipeline
    load_info = pipeline.run(memory_test_source())
    print(f"\nPipeline completed: {load_info}")


def simulate_memory_pressure():
    """Create a test that simulates memory pressure more directly"""
    print("\n=== Simulating Memory Pressure ===")

    # Force tiny buffers and low memory limit
    os.environ["DLT_DATA_WRITER__BUFFER_MAX_ITEMS"] = "5"  # Tiny buffer!

    # Create collector with very low threshold for testing
    collector = MemoryAwareCollector(
        max_memory_mb=100,  # Very low for testing
        memory_check_interval=0.1,  # Check very frequently
        flush_threshold_percent=0.1,  # Flush at 10% of limit
        log_period=0.5,
    )

    print("Creating pipeline with immediate flush threshold...")

    pipeline = dlt.pipeline(
        pipeline_name="pressure_test",
        destination="duckdb",
        dataset_name="pressure_test",
        progress=collector,
        # progress="log",
        dev_mode=True,
    )

    def create_memory_pressure_data():
        """Generate data while creating memory pressure"""
        big_memory_list = []

        for i in range(5000):
            # Add memory pressure
            if i % 10 == 0:
                big_memory_list.append("Y" * 900000)  # 900KB chunks
                if len(big_memory_list) > 10:
                    big_memory_list.pop(0)

            # Slow down generation to let buffers fill
            if i % 50 == 0:
                time.sleep(0.1)

            yield {"id": i, "data": "X" * 50000}  # 5KB per record

    @dlt.resource
    def pressure_resource():
        yield from create_memory_pressure_data()

    @dlt.source
    def pressure_source():
        return pressure_resource

    load_info = pipeline.run(pressure_source)
    print(f"Pressure test completed: {load_info}")


if __name__ == "__main__":
    print("=== Memory Debugging Test ===\n")

    # main()
    simulate_memory_pressure()

    print("\n=== Test Complete ===")
    print("Look at the DEBUG output to see:")
    print("- How many writers were found")
    print("- How much data was in buffers")
    print("- Whether flushes actually freed memory")
