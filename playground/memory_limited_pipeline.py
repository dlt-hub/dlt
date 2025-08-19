"""
Example: Memory-Limited Pipeline
===============================

This example demonstrates how to use dlt's memory-aware collector to limit RAM usage
during pipeline execution. The collector monitors memory usage and automatically flushes
buffers when memory consumption exceeds configured limits.

Features:
- Automatic memory monitoring with configurable limits
- Buffer flushing when memory thresholds are exceeded
- Environment variable and programmatic configuration
- Logging of memory usage and flush operations
"""

import os
import dlt
from dlt.sources.helpers import requests


def generate_large_dataset():
    """Generate a dataset that could consume significant memory"""
    for i in range(10000):  # Large number of records
        yield {
            "id": i,
            "data": f"This is a large text field with lots of content - record {i} " * 100,  # Large text
            "metadata": {
                "processed_at": "2024-01-01T00:00:00Z",
                "source": "memory_test",
                "tags": [f"tag_{j}" for j in range(50)]  # Large nested data
            }
        }


def main():
    """Demonstrate memory-limited pipeline execution"""
    
    # Get current memory usage and set realistic limit
    try:
        import psutil
        current_mem = psutil.Process(os.getpid()).memory_info().rss / (1024**2)
        memory_limit = int(current_mem + 300)  # Add 300MB buffer
        print(f"Current memory: {current_mem:.1f}MB, setting limit to: {memory_limit}MB")
    except ImportError:
        memory_limit = 1024  # Default 1GB
        print(f"psutil not available, using default limit: {memory_limit}MB")
    
    # Method 1: Use environment variable to set memory limit
    os.environ["DLT_MAX_MEMORY_MB"] = str(memory_limit)
    
    # Create pipeline with memory-aware progress collector
    pipeline = dlt.pipeline(
        pipeline_name="memory_limited_example",
        destination="duckdb",
        dataset_name="memory_test",
        progress="memory_aware"  # Use memory-aware collector
    )
    
    print("Running memory-limited pipeline...")
    print(f"Memory limit: {memory_limit}MB")
    print("Monitor the logs for memory usage and buffer flush messages")
    
    # Create a source that generates large amounts of data
    @dlt.source
    def large_data_source():
        return dlt.resource(
            generate_large_dataset(),
            name="large_table",
            write_disposition="replace"
        )
    
    # Run the pipeline - memory will be monitored automatically
    # When memory usage exceeds 80% of the 512MB limit (409.6MB),
    # buffered data will be automatically flushed to disk
    load_info = pipeline.run(large_data_source())
    print(f"Pipeline completed: {load_info}")


def programmatic_configuration_example():
    """Example of programmatic memory configuration"""
    from dlt.common.runtime.memory_collector import MemoryAwareCollector

    @dlt.source
    def large_data_source():
        return dlt.resource(
            generate_large_dataset(),
            name="large_table",
            write_disposition="replace"
        )
 
    
    # Create a custom memory-aware collector with specific settings
    custom_collector = MemoryAwareCollector(
        max_memory_mb=256,           # 256MB limit
        memory_check_interval=0.5,   # Check memory every 2 seconds
        flush_threshold_percent=0.7, # Flush at 70% of limit
        log_period=0.5               # Log progress every 5 seconds
    )
    
    # Use the custom collector in pipeline
    pipeline = dlt.pipeline(
        pipeline_name="custom_memory_example",
        destination="duckdb", 
        dataset_name="custom_memory_test",
        # progress=custom_collector
        progress="log"
    )
    
    print("Running pipeline with custom memory configuration...")
    print("Memory limit: 256MB, flush threshold: 70%")
    
    @dlt.source
    def medium_data_source():
        return dlt.resource(
            [{"id": i, "data": f"Record {i}"} for i in range(1000)],
            name="medium_table"
        )
    
    load_info = pipeline.run(large_data_source())
    print(f"Custom pipeline completed: {load_info}")


def configuration_via_config_file():
    """Example using configuration file"""
    
    # You can also configure via dlt configuration files
    # In config.toml or secrets.toml:
    # 
    # [data_writer]
    # max_memory_mb = 1024
    # memory_check_interval = 3.0
    # flush_threshold_percent = 0.8
    
    pipeline = dlt.pipeline(
        pipeline_name="config_file_example",
        destination="duckdb",
        dataset_name="config_memory_test", 
        progress="memory_aware"
    )
    
    print("Memory settings will be loaded from configuration files")
    
    # If no explicit config is found, environment variables are checked:
    # DLT_MAX_MEMORY_MB=1024
    
    @dlt.source
    def config_data_source():
        return dlt.resource(
            [{"message": f"Config example {i}"} for i in range(100)],
            name="config_table"
        )
    
    load_info = pipeline.run(config_data_source())
    print(f"Config-based pipeline completed: {load_info}")


if __name__ == "__main__":
    print("=== Memory Management Examples ===\n")
    
    print("1. Environment Variable Configuration")
    # main()
    print()
    
    print("2. Programmatic Configuration") 
    programmatic_configuration_example()
    print()
    
    print("3. Configuration File Example")
    # configuration_via_config_file()
    
    print("\n=== Examples completed ===")
    print("\nKey benefits of memory-aware collection:")
    print("- Prevents out-of-memory errors during large data processing")
    print("- Automatically flushes buffers when memory thresholds are exceeded")
    print("- Configurable memory limits and flush thresholds")
    print("- Detailed logging of memory usage and flush operations")
    print("- Works with all dlt destinations and sources")
