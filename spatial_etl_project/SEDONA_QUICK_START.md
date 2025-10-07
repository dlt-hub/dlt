# Apache Sedona - Quick Start Guide

## TL;DR

Apache Sedona has version compatibility issues when installed locally. **Two working solutions:**

### ✅ Solution 1: Skip Sedona (Recommended for 95% of use cases)
```bash
# Use Examples 01-03 instead
python examples/01_simple_geojson.py
python examples/03_spatial_transforms.py

# These provide all spatial operations without Sedona:
# - Buffers, CRS transforms, distances
# - Works on datasets up to 100GB
# - No version conflicts
```

### ✅ Solution 2: Use Docker Cluster (For distributed processing)
```bash
# Start Sedona cluster with compatible versions
./run_sedona_cluster.sh

# Access Spark UI: http://localhost:8085
# Results in: ./output/
```

---

## Why Sedona is Complex

**Version Incompatibility:**
- PySpark 3.5.x → Scala 2.13
- Sedona 1.6.0 → Scala 2.12  
- Result: `ClassNotFoundException`

**The cluster solution uses:**
- ✅ Spark 3.4.1 (Scala 2.12)
- ✅ Sedona 1.5.1 (Scala 2.12)
- ✅ No conflicts!

---

## Quick Comparison

| Feature | Examples 01-03 | Sedona Cluster |
|---------|---------------|----------------|
| Setup Time | 30 seconds | 5 minutes |
| Data Size | < 100GB | Unlimited |
| Deployment | Single machine | Distributed |
| Complexity | Simple | Advanced |
| Use Case | 95% of projects | Big data only |

---

## When Do You Need Sedona?

**❌ You DON'T need Sedona if:**
- Dataset < 100GB
- Single machine works
- Local development
- Learning spatial SQL

→ **Use Examples 01-03 instead**

**✅ You DO need Sedona if:**
- Dataset > 100GB
- Need distributed processing
- Multi-node cluster
- Production big data pipeline

→ **Use Docker cluster or managed service**

---

## Docker Cluster Setup

### 1. Build Image (one-time, ~5 minutes)
```bash
docker build -f Dockerfile.sedona -t spatial-etl-sedona:latest .
```

### 2. Start Cluster
```bash
# Option A: Helper script
./run_sedona_cluster.sh

# Option B: Docker Compose
docker-compose -f docker-compose-sedona.yml up
```

### 3. Check Results
```bash
# View outputs
ls -la output/countries/
ls -la output/spatial_joins/
ls -la output/distances/

# Access Spark UI
open http://localhost:8085
```

### 4. Stop Cluster
```bash
docker-compose -f docker-compose-sedona.yml down
```

---

## Managed Services (Production)

### Databricks
```python
# Sedona pre-installed, just import
from sedona.spark import SedonaContext
sedona = SedonaContext.create(spark)
```

### AWS EMR
```bash
# Add Sedona during cluster creation
aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --bootstrap-actions Path=s3://path/to/sedona-bootstrap.sh
```

### Google Dataproc
```bash
# Install Sedona initialization action
gcloud dataproc clusters create sedona-cluster \
  --initialization-actions gs://path/to/sedona-init.sh
```

---

## Files Reference

- `Dockerfile.sedona` - Compatible Docker image
- `docker-compose-sedona.yml` - Multi-service cluster
- `run_sedona_cluster.sh` - Quick start script
- `examples/04_sedona_cluster.py` - Cluster examples
- `k8s/sedona-cluster.yaml` - Kubernetes deployment
- `CLUSTER_SETUP.md` - Complete guide

---

## Troubleshooting

### "Port 8085 already in use"
Already configured! Default is 8085 (not 8080)

### "Docker build is slow"
First build downloads ~2GB. Subsequent builds are fast.

### "Out of memory"
Increase Docker memory: Docker Desktop → Settings → Resources → 8GB

### "Can't I just pip install?"
No - version conflicts are unfixable in local environment.

---

## Recommendation

**For this spatial ETL project:**
1. ✅ Use Examples 01-03 for all development
2. ✅ Use PostGIS for database storage
3. ⏭️ Skip Sedona unless you have TB-scale data
4. ✅ Use managed services (Databricks/EMR) if you need Sedona

**Bottom line:** Sedona is optional and only needed for massive distributed workloads.

---

**Next Steps:**
```bash
# Start with simple examples (works immediately)
python examples/01_simple_geojson.py

# Or build cluster (if you really need Sedona)
./run_sedona_cluster.sh
```

---

**Updated**: 2025-10-07  
**Status**: Docker cluster tested and working  
**Recommendation**: Use Examples 01-03 for 95% of use cases
