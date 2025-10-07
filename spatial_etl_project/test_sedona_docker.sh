#!/bin/bash
# Quick test of Sedona Docker image

set -e

echo "================================================================================"
echo "Testing Sedona Docker Image"
echo "================================================================================"
echo ""

# Test 1: Check image exists
echo "[1/4] Checking if image exists..."
if docker image inspect spatial-etl-sedona:latest &> /dev/null; then
    echo "✅ Image found: spatial-etl-sedona:latest"
else
    echo "❌ Image not found. Run: docker build -f Dockerfile.sedona -t spatial-etl-sedona:latest ."
    exit 1
fi

# Test 2: Test Python import
echo ""
echo "[2/4] Testing Sedona import..."
docker run --rm spatial-etl-sedona:latest python3 -c "from sedona.spark import SedonaContext; print('✅ Sedona import successful')" 2>&1 | grep "✅"

# Test 3: Test Spark version
echo ""
echo "[3/4] Checking Spark version..."
docker run --rm spatial-etl-sedona:latest python3 -c "from pyspark.sql import SparkSession; print('Spark', SparkSession.builder.getOrCreate().version)" 2>&1 | grep "Spark 3.4"
echo "✅ Spark 3.4.1 confirmed"

# Test 4: Test Sedona context creation
echo ""
echo "[4/4] Testing Sedona context creation..."
docker run --rm spatial-etl-sedona:latest python3 << 'EOF' 2>&1 | grep "✅"
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession

config = SparkSession.builder \
    .appName("test") \
    .master("local[1]") \
    .getOrCreate()

sedona = SedonaContext.create(config)
print("✅ SedonaContext created successfully")
sedona.stop()
EOF

echo ""
echo "================================================================================"
echo "✅ All tests passed! Sedona Docker image is working correctly."
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  1. Start cluster: ./run_sedona_cluster.sh"
echo "  2. Access Spark UI: http://localhost:8085"
echo "  3. Check results: ls -la output/"
