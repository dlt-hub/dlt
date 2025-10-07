# Apache Sedona Cluster Setup Guide

This guide explains how to run Apache Sedona with **compatible versions** using Docker Compose or Kubernetes.

---

## ✅ Solution Overview

### The Problem
- Local Sedona setup has Scala version conflicts
- PySpark 3.5+ uses Scala 2.13
- Sedona 1.6.0 requires Scala 2.12
- Result: `ClassNotFoundException` errors

### The Solution
**Use containerized Spark cluster with compatible versions:**
- ✅ Spark 3.4.1 (Scala 2.12)
- ✅ Sedona 1.5.1 (Scala 2.12)
- ✅ All dependencies pre-configured
- ✅ No version conflicts

---

## 🐳 Option 1: Docker Compose (Recommended)

### Quick Start

```bash
# Build and start cluster
./run_sedona_cluster.sh

# Or manually:
docker-compose -f docker-compose-sedona.yml up --build
```

### What Gets Deployed

1. **Spark Master**
   - Port 7077: Spark master
   - Port 8085: Web UI
   - Manages cluster resources

2. **Spark Worker**
   - Port 8086: Worker web UI
   - 2 CPU cores, 2GB RAM
   - Executes tasks

3. **PostGIS**
   - Port 5432: PostgreSQL
   - Spatial database
   - Persistent storage

4. **Sedona Application**
   - Runs examples automatically
   - Outputs to `./output/`
   - Connects to cluster

### Access Web UIs

- **Spark Master**: http://localhost:8085
- **Spark Worker**: http://localhost:8086
- **PostGIS**: `psql -h localhost -U postgres spatial_db`

### View Results

```bash
# Check output files
ls -la output/

# Countries analysis
ls output/countries/

# Spatial joins
ls output/spatial_joins/

# Distance matrix
ls output/distances/
```

### Stop Cluster

```bash
docker-compose -f docker-compose-sedona.yml down

# Remove volumes
docker-compose -f docker-compose-sedona.yml down -v
```

---

## ☸️ Option 2: Kubernetes

### Prerequisites

```bash
# Install kubectl
brew install kubectl

# Install minikube (for local testing)
brew install minikube

# Start minikube
minikube start --cpus 4 --memory 8192
```

### Build Docker Image

```bash
# Build image
docker build -f Dockerfile.sedona -t spatial-etl-sedona:latest .

# Load into minikube
minikube image load spatial-etl-sedona:latest
```

### Deploy to Kubernetes

```bash
# Create namespace and deploy
kubectl apply -f k8s/sedona-cluster.yaml

# Check status
kubectl get pods -n spatial-etl
kubectl get services -n spatial-etl

# View logs
kubectl logs -n spatial-etl -l app=spark-master --tail=100
kubectl logs -n spatial-etl job/sedona-app --tail=100
```

### Access Services

```bash
# Forward Spark Master UI
kubectl port-forward -n spatial-etl svc/spark-master 8085:8085

# Forward PostGIS
kubectl port-forward -n spatial-etl svc/postgis 5432:5432

# Access in browser
open http://localhost:8085
```

### Scale Workers

```bash
# Scale to 4 workers
kubectl scale statefulset spark-worker -n spatial-etl --replicas=4

# Check status
kubectl get pods -n spatial-etl
```

### Clean Up

```bash
# Delete everything
kubectl delete namespace spatial-etl

# Or delete specific resources
kubectl delete -f k8s/sedona-cluster.yaml
```

---

## 📊 Cluster Architecture

```
┌─────────────────────────────────────────┐
│         Sedona Application              │
│  (Submits jobs to Spark Master)        │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│         Spark Master                    │
│  - Manages cluster resources            │
│  - Distributes tasks to workers         │
│  - Web UI: http://localhost:8085       │
└─────────────────┬───────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│ Spark Worker  │   │ Spark Worker  │
│  - Executes   │   │  - Executes   │
│    tasks      │   │    tasks      │
│  - 2 cores    │   │  - 2 cores    │
│  - 2GB RAM    │   │  - 2GB RAM    │
└───────────────┘   └───────────────┘
        │                   │
        └─────────┬─────────┘
                  ▼
        ┌──────────────────┐
        │     PostGIS      │
        │  - Stores results│
        │  - Port 5432     │
        └──────────────────┘
```

---

## 🔧 Configuration

### Dockerfile.sedona
- Base: OpenJDK 11
- Spark: 3.4.1
- Sedona: 1.5.1 (compatible!)
- Python: 3.9

### docker-compose-sedona.yml
- Multi-service setup
- Shared volumes
- Network configuration
- Health checks

### k8s/sedona-cluster.yaml
- Namespace isolation
- Resource limits
- Persistent volumes
- Load balancers

---

## 📝 Example Output

### Running on Cluster

```bash
$ ./run_sedona_cluster.sh

Building Sedona Docker image...
Starting Sedona cluster...

Services:
  - Spark Master:  http://localhost:8085
  - Spark Worker:  http://localhost:8086
  - PostGIS:       localhost:5432

================================================================================
🚀 Apache Sedona Distributed Spatial Processing - Cluster Mode
================================================================================
✅ Sedona Context created successfully
   Spark Version: 3.4.1
   Master URL: spark://spark-master:7077

================================================================================
Example 1: Country Spatial Analysis
================================================================================

📊 Country Statistics:
--------------------------------------------------------------------------------
+-------------+------------------+------------------+--------------------------------------------+
|name         |area_sq_km_approx |perimeter         |centroid_wkt                                |
+-------------+------------------+------------------+--------------------------------------------+
|United States|181551.0          |202.0             |POINT (-95.5 37.5)                          |
|Canada       |197988.0          |226.0             |POINT (-96.5 56.0)                          |
|Mexico       |98559.0           |134.0             |POINT (-101.5 23.5)                         |
+-------------+------------------+------------------+--------------------------------------------+

✅ Results saved to /workspace/output/countries/

================================================================================
✅ All Sedona cluster examples completed successfully!
================================================================================
```

---

## 🚀 Performance Tips

### Docker Compose
```yaml
# Increase worker resources in docker-compose-sedona.yml
environment:
  - SPARK_WORKER_CORES=4
  - SPARK_WORKER_MEMORY=4g
```

### Kubernetes
```bash
# Scale workers
kubectl scale statefulset spark-worker -n spatial-etl --replicas=4

# Increase resources in k8s/sedona-cluster.yaml
resources:
  limits:
    memory: "8Gi"
    cpu: "4000m"
```

---

## 🐛 Troubleshooting

### Ports Already in Use
```bash
# Check what's using ports
lsof -i :8085
lsof -i :7077

# Kill processes or change ports in docker-compose-sedona.yml
```

### Out of Memory
```bash
# Increase Docker memory
# Docker Desktop → Settings → Resources → Memory: 8GB

# Or reduce Spark memory
SPARK_WORKER_MEMORY=1g
```

### Image Build Failed
```bash
# Clear Docker cache
docker system prune -a

# Rebuild
docker build --no-cache -f Dockerfile.sedona -t spatial-etl-sedona:latest .
```

### Kubernetes Pods Not Starting
```bash
# Check pod status
kubectl describe pod -n spatial-etl <pod-name>

# Check logs
kubectl logs -n spatial-etl <pod-name>

# Check resources
kubectl top nodes
kubectl top pods -n spatial-etl
```

---

## 📚 What's Included

### Files Created
```
├── Dockerfile.sedona           # Compatible Sedona image
├── docker-compose-sedona.yml   # Multi-service cluster
├── run_sedona_cluster.sh       # Helper script
├── k8s/
│   └── sedona-cluster.yaml    # Kubernetes manifests
├── examples/
│   └── 04_sedona_cluster.py   # Cluster-ready examples
└── output/                     # Results directory
```

### Examples in 04_sedona_cluster.py
1. **Country Processing** - ST_Area, ST_Centroid, ST_Length
2. **Spatial Joins** - ST_Within, ST_Distance
3. **Distance Matrix** - Cross join with distances
4. **Advanced Operations** - ST_Buffer, ST_Envelope, validation

---

## 🎯 When to Use Each Option

### Docker Compose
✅ Local development
✅ Quick testing
✅ Single machine
✅ < 10GB data

### Kubernetes
✅ Production deployment
✅ Multi-node cluster
✅ Auto-scaling needed
✅ > 100GB data

### Managed Services
✅ Databricks
✅ AWS EMR
✅ Google Dataproc
✅ Azure Synapse

---

## 🎉 Success!

You now have a working Apache Sedona cluster with:
- ✅ Compatible versions (Spark 3.4.1 + Sedona 1.5.1)
- ✅ No Scala conflicts
- ✅ Distributed processing ready
- ✅ PostGIS integration
- ✅ Example outputs

**Start the cluster:**
```bash
./run_sedona_cluster.sh
```

**Access Spark UI:**
http://localhost:8085

---

**Created**: 2025-10-07  
**Spark Version**: 3.4.1  
**Sedona Version**: 1.5.1  
**Scala Version**: 2.12 ✅
