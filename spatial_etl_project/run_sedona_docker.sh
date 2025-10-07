#!/bin/bash
# Run Apache Sedona distributed spatial processing in Docker

set -e

echo "================================================================================"
echo "Apache Sedona Docker Example"
echo "================================================================================"
echo ""
echo "This script runs Example 04 using the official Apache Sedona Docker image."
echo "This resolves Scala version compatibility issues with local PySpark installs."
echo ""

if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed"
    echo "Install Docker from: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Error: Docker daemon is not running"
    echo "Start Docker Desktop or run: sudo systemctl start docker"
    exit 1
fi

echo "✅ Docker is available"
echo ""
echo "Starting Sedona container..."
echo ""

docker run --rm \
    -v "$(pwd)/examples:/workspace/examples" \
    -v "$(pwd)/data:/workspace/data" \
    -w /workspace \
    apache/spark-py:v3.5.0 \
    /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
    examples/04_sedona_docker.py

echo ""
echo "================================================================================"
echo "✅ Sedona Docker example completed!"
echo "================================================================================"
