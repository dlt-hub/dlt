# 🧠 DLT Memory Management Demo

This demo showcases the **memory-aware collector** feature in dlt, which automatically monitors RAM usage and flushes buffers to prevent out-of-memory (OOM) crashes during large data processing tasks.

## 🎯 Demo Overview

The demo uses a real-world data pipeline that loads data from the **Jaffle Shop REST API** in a container with only 128MB of RAM:

- **Real memory usage**: Extract phase (~281MB), Load phase (>1GB)  
- **Container baseline**: ~52MB (Python + dlt + dependencies)
- **Without memory limiting**: Container crashes with OOM error during extract/load
- **With memory limiting**: Pipeline completes successfully with automatic buffer flushes

## 🐳 Quick Start

### 1. Build the Demo Container

```bash
# From the dlt repository root
docker build -f Dockerfile.ram-demo -t dlt-ram-demo .
```

### 2. Run Demo Scenarios

#### Scenario A: Crash (No Memory Limiting)
```bash
docker run --memory=128m --rm dlt-ram-demo
```
**Expected Result**: Container gets killed with OOM error

#### Scenario B: Success (With Memory Limiting)
```bash
docker run --memory=128m --rm \
  -e DATA_WRITER__MAX_MEMORY_MB=80 \
  dlt-ram-demo
```
**Expected Result**: Pipeline completes successfully with memory management logs

#### Scenario C: Aggressive (Ultra-Low Memory Limiting)
```bash
docker run --memory=128m --rm \
  -e DATA_WRITER__MAX_MEMORY_MB=60 \
  -e DATA_WRITER__FLUSH_THRESHOLD_PERCENT=0.5 \
  dlt-ram-demo
```
**Expected Result**: Frequent buffer flushes, pipeline still completes successfully

### 3. Advanced Monitoring

Run with real-time memory monitoring:
```bash
# Terminal 1: Run demo with memory limiting
docker run --memory=128m --name ram-demo \
  -e DATA_WRITER__MAX_MEMORY_MB=80 \
  -e DATA_WRITER__MEMORY_CHECK_INTERVAL=1.0 \
  dlt-ram-demo

# Terminal 2: Watch memory usage in real-time
docker stats ram-demo

# Cleanup when done
docker rm ram-demo
```

## ⚙️ Configuration Options

Control the demo behavior with environment variables:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `DATA_WRITER__MAX_MEMORY_MB` | Memory limit in MB (enables memory management) | None (disabled) | `80` |
| `DATA_WRITER__MEMORY_CHECK_INTERVAL` | Check frequency in seconds | `2.0` | `1.0` |
| `DATA_WRITER__FLUSH_THRESHOLD_PERCENT` | Flush trigger percentage | `0.8` (80%) | `0.7` |

### Example: Ultra-Aggressive Memory Management
```bash
docker run --memory=128m --rm \
  -e DATA_WRITER__MAX_MEMORY_MB=60 \
  -e DATA_WRITER__MEMORY_CHECK_INTERVAL=0.5 \
  -e DATA_WRITER__FLUSH_THRESHOLD_PERCENT=0.5 \
  dlt-ram-demo
```

## 📊 What to Observe

### Without Memory Limiting
```
🧠 DLT Memory Management Demo - Jaffle Shop API
📋 Configuration:
  • Data Source: Jaffle Shop REST API
  • Extract Workers: 4
  • Max Memory Limit: NOT SET (will likely crash) MB
⚠️  Memory limiting DISABLED - Container will likely crash with OOM
📈 Expected memory usage:
  • Extract phase: ~281MB (will exceed 256MB limit)
  • Load phase: >1GB (definitely exceeds limit)
🚀 Starting Jaffle Shop API data extraction...
🌐 Fetching data from: https://jaffle-shop.scalevector.ai/api/v1/
📦 Resources: customers, orders, items, products, supplies, stores, row_counts
...
Killed  # OOM by Docker
```

### With Memory Limiting
```
🧠 DLT Memory Management Demo - Jaffle Shop API
📋 Configuration:
  • Data Source: Jaffle Shop REST API
  • Extract Workers: 4
  • Max Memory Limit: 200 MB
✅ Memory limiting ENABLED - Pipeline should complete successfully
🚀 Starting Jaffle Shop API data extraction...
🌐 Fetching data from: https://jaffle-shop.scalevector.ai/api/v1/
📦 Resources: customers, orders, items, products, supplies, stores, row_counts
WARNING - Memory usage (162.3MB) exceeds threshold (160.0MB). Flushing buffers...
INFO - Flushed 3 buffered writers (15000 items) due to memory pressure
INFO - Buffer flush completed. Memory freed: 45.2MB. Current usage: 117.1MB
...
🎉 SUCCESS! Jaffle Shop pipeline completed without OOM crash
📈 Total rows extracted: 50,123
📋 Rows by table:
    • customers: 100 rows
    • orders: 12,345 rows
    • items: 25,678 rows
    • products: 10 rows
    • supplies: 11,980 rows
    • stores: 10 rows
🧠 Memory management successfully handled real API data processing!
```

## 🔧 Demo Architecture

```
┌─────────────────────────────────────┐
│ Docker Container (128MB RAM limit)  │
│ ┌─────────────────────────────────┐ │
│ │ Python Process (~52MB baseline)│ │
│ │ ├── dlt pipeline               │ │
│ │ ├── MemoryAwareCollector       │ │
│ │ ├── REST API Source (4 workers)│ │
│ │ ├── BufferedDataWriter(s)      │ │
│ │ └── Jaffle Shop API Data       │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
         ↕ HTTPS
┌─────────────────────────────────────┐
│ Jaffle Shop REST API                │
│ https://jaffle-shop.scalevector.ai  │
│ ├── /customers  (paginated)        │
│ ├── /orders     (paginated)        │ 
│ ├── /items      (paginated)        │
│ ├── /products   (paginated)        │
│ ├── /supplies   (paginated)        │
│ ├── /stores     (paginated)        │
│ └── /row-counts (single page)      │
└─────────────────────────────────────┘
```

## 🛠️ Customizing the Demo

### Modify API Configuration

Edit `jaffle_source.py` to change the REST API behavior:

```python
# Increase page size for more memory pressure
"resource_defaults": {
    "endpoint": {
        "params": {
            "page_size": 10000  # Larger pages = more memory usage
        }
    }
}

# Add more concurrent workers
os.environ["EXTRACT__WORKERS"] = "8"  # More workers = more memory pressure
```

### Different Destinations

Change the destination in `demo_memory_pressure.py`:

```python
pipeline = dlt.pipeline(
    pipeline_name="jaffle_api_to_duckdb",
    destination="postgres",  # or "bigquery", "snowflake", etc.
    progress="memory_aware"
)
```

### Use Different API Sources

Replace `jaffle_source.py` with any other dlt REST API source:

```python
# Example: Use a different REST API source
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {"base_url": "https://your-api.com/"},
    "resources": [{"name": "data", "endpoint": {"path": "data"}}]
})
```

## 🔍 Troubleshooting

### Container Exits Immediately
```bash
# Check logs
docker logs <container_id>

# Run with interactive shell to debug
docker run --memory=256m -it --entrypoint bash dlt-ram-demo
```

### Memory Limiting Not Working
```bash
# Verify environment variables are set
docker run --memory=256m --rm \
  -e DATA_WRITER__MAX_MEMORY_MB=200 \
  dlt-ram-demo env | grep DATA_WRITER
```

### Network Connectivity Issues
```bash
# Test API connectivity
docker run --rm dlt-ram-demo python -c "
import requests
try:
    response = requests.get('https://jaffle-shop.scalevector.ai/api/v1/row-counts')
    print(f'API Status: {response.status_code}')
    print(f'Response: {response.json()}')
except Exception as e:
    print(f'API Error: {e}')
"

# Test from inside container
docker run --rm -it dlt-ram-demo bash
curl https://jaffle-shop.scalevector.ai/api/v1/row-counts
```

### Monitor Resource Usage
```bash
# Real-time container stats
docker stats --no-stream

# System memory info
docker run --rm dlt-ram-demo python -c "
import psutil
print(f'Available memory: {psutil.virtual_memory().available / 1024**2:.1f} MB')
print(f'Process memory: {psutil.Process().memory_info().rss / 1024**2:.1f} MB')
"
```

## 📈 Performance Notes

- **Real API Data**: Jaffle Shop REST API with 7 paginated resources
- **Memory Usage**: Extract phase (~281MB), Load phase (>1GB) 
- **Container Limit**: 128MB RAM limit simulates resource-constrained environment
- **Python Baseline**: ~52MB (Python + dlt + dependencies)
- **Available for Processing**: ~76MB (128MB - 52MB baseline)
- **Memory Threshold**: Default 70% of limit (80MB × 0.7 = 56MB trigger point)
- **Extract Workers**: 4 concurrent workers increase memory pressure
- **Expected Flushes**: 20-50 automatic buffer flushes during execution
- **Network Dependency**: Requires internet connectivity to access API

## 🎓 Learning Outcomes

After running this demo, you'll understand:

1. **How memory pressure affects dlt pipelines** (OOM crashes without management)
2. **Automatic buffer flushing in action** (memory-aware collector prevents crashes)
3. **Configuration options** (memory limits, thresholds, check intervals)
4. **Real-world application** (containerized environments, cloud functions)

## 🚀 Next Steps

- Try different memory limits and thresholds
- Experiment with various data sizes and patterns
- Test with different dlt destinations
- Integrate into your own memory-constrained environments

---

*This demo showcases dlt's intelligent memory management capabilities, enabling reliable processing of large datasets in resource-constrained environments.* 🧠✨
