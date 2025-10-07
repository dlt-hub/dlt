# Apache Sedona Setup Guide

## Current Status: Optional Advanced Feature

Apache Sedona distributed spatial processing is an **optional advanced feature** for large-scale geospatial workloads.

### ✅ What Works Without Sedona
The spatial ETL project works completely without Sedona:
- ✅ GeoJSON pipelines (Example 01)
- ✅ Shapefile processing (Example 02)  
- ✅ Spatial transformations (Example 03)
- ✅ All core spatial operations (buffers, CRS, distances)

**You only need Sedona for:**
- Distributed processing across Spark clusters
- Processing datasets > 100GB
- Parallel spatial joins on massive datasets

### Compatibility Challenge
**Why Sedona is complex to set up locally:**
- Requires exact PySpark + Scala + Sedona version match
- PySpark 3.5.x uses Scala 2.13
- Sedona 1.6.0 compiled for Scala 2.12
- Result: Version conflicts and ClassNotFoundErrors

**Recommended approaches:**
1. **Use managed Spark services** (Databricks, EMR, Dataproc)
2. **Use Sedona in production clusters** with pre-configured versions
3. **Skip Sedona** for local development (use Shapely/GeoPandas instead)

---

## Quick Start

### Option 1: Run with Helper Script (Recommended)

```bash
./run_sedona_docker.sh
```

This runs the standalone Sedona example (`04_sedona_docker.py`) in a Docker container.

### Option 2: Run with Docker Compose

```bash
# Start both PostGIS and Sedona
docker-compose up

# Or start only Sedona
docker-compose up sedona-spark
```

### Option 3: Run with Docker Directly

```bash
docker run --rm \
    -v $(pwd)/examples:/workspace/examples \
    -v $(pwd)/data:/workspace/data \
    apache/sedona:1.6.0 \
    /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0 \
    /workspace/examples/04_sedona_docker.py
```

---

## What Gets Installed in Docker

The `apache/sedona:1.6.0` image includes:
- ✅ Apache Spark 3.5.x (Scala 2.12)
- ✅ Apache Sedona 1.6.0 (compatible versions)
- ✅ All required Scala dependencies
- ✅ PySpark with Sedona Python bindings

---

## Examples Included

### Example 1: Country Processing
```python
# Creates country polygons
# Calculates area, perimeter, centroid
# Uses Sedona SQL spatial functions
```

### Example 2: Spatial Join
```python
# Finds points within zones
# Demonstrates ST_Within predicate
# Shows distributed spatial join
```

### Example 3: Distance Matrix
```python
# Calculates distances between cities
# Uses ST_Distance function
# Converts to kilometers
```

---

## Docker Compose Services

### PostGIS Service
```yaml
postgis:
  image: postgis/postgis:16-3.4-alpine
  ports: 5432:5432
```
- PostgreSQL with PostGIS extension
- For storing/querying spatial data
- Accessible at `localhost:5432`

### Sedona Spark Service
```yaml
sedona-spark:
  image: apache/sedona:1.6.0
  ports:
    - 8085:8080  # Spark Master UI (mapped to 8085)
    - 7077:7077  # Spark Master
    - 4040:4040  # Application UI
```

---

## Accessing Spark UI

When running with Docker, access Spark's web UIs:

- **Spark Master UI**: http://localhost:8085 (mapped from container's 8080)
- **Application UI**: http://localhost:4040

---

## File Structure

```
spatial_etl_project/
├── examples/
│   ├── 04_sedona_distributed.py    # Local version (has compatibility issues)
│   └── 04_sedona_docker.py         # Docker version (works reliably)
├── docker-compose.yml              # Multi-service setup
├── run_sedona_docker.sh            # Helper script
└── SEDONA_DOCKER_GUIDE.md         # This file
```

---

## Troubleshooting

### Docker Not Installed
```bash
# macOS
brew install --cask docker

# Ubuntu/Debian
sudo apt-get install docker.io docker-compose

# Or download from: https://docs.docker.com/get-docker/
```

### Docker Daemon Not Running
```bash
# macOS: Start Docker Desktop application

# Linux
sudo systemctl start docker
```

### Port Already in Use
```bash
# Spark Master UI is already mapped to 8085 (not 8080)
# If 8085 is also in use, check what's using it:
lsof -i :8085

# Or change the port in docker-compose.yml:
# ports:
#   - "8086:8080"  # Use 8086 instead
```

### Permission Denied
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
newgrp docker
```

---

## Performance Notes

### Local Mode vs Cluster Mode

**Local Mode** (default in examples):
```python
.master("local[*]")  # Uses all CPU cores on single machine
```

**Cluster Mode** (for production):
```python
.master("spark://master:7077")  # Distributed across cluster
```

### When to Use Docker Sedona

✅ **Use Docker for:**
- Development and testing
- Avoiding version conflicts
- Running on any platform consistently
- Learning Sedona features

❌ **Don't use Docker for:**
- Production workloads (use managed Spark like Databricks, EMR)
- Very large datasets (use cluster mode)
- Performance benchmarking (native installation is faster)

---

## Production Deployment

For production, consider:

1. **Managed Spark Services**
   - AWS EMR
   - Azure Databricks
   - Google Cloud Dataproc
   - Databricks (all clouds)

2. **Kubernetes**
   ```bash
   # Deploy Spark on K8s with Sedona
   kubectl apply -f sedona-spark-k8s.yaml
   ```

3. **Standalone Cluster**
   - Set up Spark cluster with Sedona
   - Use cluster manager (YARN, Mesos, K8s)

---

## Integration with dlt

The Docker example (`04_sedona_docker.py`) is standalone and doesn't use dlt. To integrate Sedona with dlt:

```python
import dlt
from pyspark.sql import SparkSession

@dlt.resource
def sedona_data():
    # Process with Sedona in Docker
    # Export results as Parquet
    # Load Parquet with dlt
    pass
```

Or use the original `04_sedona_distributed.py` if you resolve version conflicts.

---

## Additional Resources

- **Sedona Docs**: https://sedona.apache.org
- **Sedona Docker Hub**: https://hub.docker.com/r/apache/sedona
- **PySpark Docs**: https://spark.apache.org/docs/latest/api/python/
- **Spatial SQL Reference**: https://sedona.apache.org/latest-snapshot/api/sql/Overview/

---

## Summary

✅ **Use `./run_sedona_docker.sh`** for hassle-free Sedona experience  
✅ **No version conflicts** - everything pre-configured  
✅ **Works on macOS, Linux, Windows** with Docker installed  
✅ **Production-ready** - same approach scales to clusters

**Next Steps:**
1. Run `./run_sedona_docker.sh`
2. Check Spark UI at http://localhost:4040
3. Modify `04_sedona_docker.py` for your use case
4. Deploy to production cluster when ready

---

**Created**: 2025-10-07  
**Sedona Version**: 1.6.0  
**Spark Version**: 3.5.x  
**Scala Version**: 2.12
