#!/usr/bin/env python3
"""
dlt Memory Management Demo

This script demonstrates the memory-aware collector feature by loading data from
the Jaffle Shop API, which requires significant memory during processing.

Real-world memory usage:
  - Extract phase: ~281MB RAM
  - Load phase: >1GB RAM
  - Container limit: 256MB

Usage:
  - Without memory limiting: Container will crash with OOM
  - With memory limiting: Pipeline completes successfully with automatic buffer flushes
"""

import os
import sys
import time
import logging

import dlt
from dlt.common import logger

# Import the Jaffle Shop source
from jaffle_source import source as jaffle_source

LARGE_TABLES = ["items", "customers", "orders"]


# Configure logging to see memory management messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def jaffle_api_to_duckdb():
    """
    Load Jaffle Shop API data with memory management demo.
    
    This real-world pipeline demonstrates memory pressure scenarios:
    - Extract phase: ~281MB RAM usage
    - Load phase: >1GB RAM usage 
    - Container limit: 256MB (will crash without memory management)
    """
    print("=" * 80)
    print("🧠 DLT Memory Management Demo - Jaffle Shop API")
    print("=" * 80)
    
    # Check memory configuration
    max_memory_mb = os.environ.get("DATA_WRITER__MAX_MEMORY_MB")
    memory_check_interval = os.environ.get("DATA_WRITER__MEMORY_CHECK_INTERVAL", "2.0")
    flush_threshold = os.environ.get("DATA_WRITER__FLUSH_THRESHOLD_PERCENT", "0.8")
    extract_workers = os.environ.get("EXTRACT__WORKERS", "4")
    
    print(f"📋 Configuration:")
    print(f"  • Data Source: Jaffle Shop REST API")
    print(f"  • Extract Workers: {extract_workers}")
    print(f"  • Max Memory Limit: {max_memory_mb or 'NOT SET (will likely crash)'} MB")
    print(f"  • Memory Check Interval: {memory_check_interval} seconds")
    print(f"  • Flush Threshold: {float(flush_threshold) * 100:.0f}%")
    print()
    
    if max_memory_mb:
        print("✅ Memory limiting ENABLED - Pipeline should complete successfully")
        progress_mode = "memory_aware"
    else:
        print("⚠️  Memory limiting DISABLED - Container will likely crash with OOM")
        progress_mode = "log"  # Use log progress to see what happens before crash
    
    print(f"📊 Progress mode: {progress_mode}")
    print()
    print("📈 Expected memory usage:")
    print("  • Extract phase: ~281MB (will exceed 256MB limit)")
    print("  • Load phase: >1GB (definitely exceeds limit)")
    print("=" * 80)
    print()
    
    # Set extract workers for more memory pressure
    os.environ["EXTRACT__WORKERS"] = extract_workers

    # set buffer items for normalize stage to make the buffers flush more often
    os.environ["NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS"] = "100"
    
    try:
        # Create pipeline with memory management
        pipeline = dlt.pipeline(
            pipeline_name="memory_pressure_demo_pipe",
            destination="postgres",
            dataset_name="jaffle_api_to_duckdb",
            progress=progress_mode,
            dev_mode=True  # Clean slate each run
        )
        
        print("🚀 Starting Jaffle Shop API data extraction...")
        print("🌐 Fetching data from: https://jaffle-shop.scalevector.ai/api/v1/")
        print("📦 Resources: customers, orders, items, products, supplies, stores, row_counts")
        print()
        
        start_time = time.time()
        
        # Run the real-world memory-intensive pipeline
        load_info = pipeline.run(jaffle_source.with_resources("items"))
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print()
        print("=" * 80)
        print("🎉 SUCCESS! Jaffle Shop pipeline completed without OOM crash")
        print("=" * 80)
        print(f"⏱️  Total execution time: {execution_time:.2f} seconds")
        print(f"📊 Load info: {load_info}")
        
        # Show detailed statistics
        # if hasattr(pipeline.last_trace, 'last_extract_info') and pipeline.last_trace.last_extract_info:
        #     extract_info = pipeline.last_trace.last_extract_info
        #     if extract_info.row_counts:
        #         total_rows = sum(extract_info.row_counts.values())
        #         print(f"📈 Total rows extracted: {total_rows:,}")
        #         print("📋 Rows by table:")
        #         for table, count in extract_info.row_counts.items():
        #             print(f"    • {table}: {count:,} rows")
        
        print()
        print("🧠 Memory management successfully handled real API data processing!")
        
        return pipeline
        
    except Exception as e:
        print()
        print("=" * 80)
        print("💥 ERROR occurred during Jaffle Shop pipeline execution")
        print("=" * 80)
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
        
        if "killed" in str(e).lower() or "memory" in str(e).lower():
            print()
            print("🚨 This looks like an out-of-memory error!")
            print("💡 The Jaffle Shop API data exceeded the container's memory limit.")
            print("💡 Try running with memory limiting enabled:")
            print("   docker run --memory=256m -e DATA_WRITER__MAX_MEMORY_MB=200 dlt-ram-demo")
        elif "connection" in str(e).lower() or "network" in str(e).lower():
            print()
            print("🌐 Network error occurred!")
            print("💡 Make sure you have internet connectivity to access the Jaffle Shop API.")
        
        sys.exit(1)

def main():
    """Main demo function - calls the Jaffle Shop API pipeline"""
    return jaffle_api_to_duckdb()

if __name__ == "__main__":
    main()
