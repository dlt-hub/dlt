#!/bin/bash
# Run Apache Sedona in Docker Compose cluster

set -e

echo "================================================================================"
echo "Apache Sedona Cluster Setup"
echo "================================================================================"
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed"
    exit 1
fi

# Check for docker compose (v2) or docker-compose (v1)
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo "❌ Error: docker compose is not available"
    exit 1
fi

echo "✅ Docker and compose are available ($COMPOSE_CMD)"
echo ""

# Create output directory
mkdir -p output

echo "Building Sedona Docker image..."
docker build -f Dockerfile.sedona -t spatial-etl-sedona:latest .

echo ""
echo "Starting Sedona cluster..."
echo ""
echo "Services:"
echo "  - Spark Master:  http://localhost:8085"
echo "  - Spark Worker:  http://localhost:8086"
echo "  - PostGIS:       localhost:5434 (external port)"
echo ""

$COMPOSE_CMD -f docker-compose-sedona.yml up --build

echo ""
echo "================================================================================"
echo "✅ Sedona cluster stopped"
echo "================================================================================"
