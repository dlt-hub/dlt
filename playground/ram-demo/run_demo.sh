#!/bin/bash

# DLT Memory Management Demo Runner
# This script provides convenient commands to run different demo scenarios

set -e

DOCKER_IMAGE="dlt-ram-demo"
MEMORY_LIMIT="256m"
DLT_MEMORY_LIMIT="180"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}🧠 DLT Memory Management Demo${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  build       Build the demo Docker image"
    echo "  crash       Run demo without memory limiting (will crash)"
    echo "  success     Run demo with memory limiting (will succeed)"
    echo "  monitor     Run demo with real-time memory monitoring"
    echo "  clean       Clean up Docker containers and images"
    echo "  help        Show this help message"
    echo ""
    echo "Configuration:"
    echo "  MEMORY_LIMIT=\"${MEMORY_LIMIT}\"        # Container memory limit"
    echo "  DLT_MEMORY_LIMIT=\"${DLT_MEMORY_LIMIT}\"       # DLT memory limit in MB"
    echo ""
    echo "Examples:"
    echo "  $0 build && $0 crash       # Build then show crash scenario"
    echo "  $0 success                 # Show successful memory management (${DLT_MEMORY_LIMIT}MB limit)"
    echo "  $0 monitor                 # Watch memory usage in real-time"
    echo ""
    echo "Tuning:"
    echo "  MEMORY_LIMIT=128m DLT_MEMORY_LIMIT=80 $0 success    # Lower limits"
    echo "  MEMORY_LIMIT=512m DLT_MEMORY_LIMIT=400 $0 success   # Higher limits"
}

build_image() {
    print_header
    echo -e "${YELLOW}📦 Building Docker image...${NC}"
    echo ""
    
    if ! docker build -f ../../Dockerfile.ram-demo -t $DOCKER_IMAGE ../../; then
        echo -e "${RED}❌ Failed to build Docker image${NC}"
        exit 1
    fi
    
    echo ""
    echo -e "${GREEN}✅ Docker image built successfully${NC}"
    echo -e "Image: ${DOCKER_IMAGE}"
}

run_crash_scenario() {
    print_header
    echo -e "${RED}💥 CRASH SCENARIO: No Memory Limiting${NC}"
    echo ""
    echo -e "${YELLOW}This will demonstrate an OOM crash...${NC}"
    echo -e "Container memory limit: ${MEMORY_LIMIT}"
    echo -e "DLT memory limiting: ${RED}DISABLED${NC}"
    echo ""
    echo "Press Enter to continue or Ctrl+C to cancel..."
    read
    
    echo -e "${BLUE}🚀 Starting container (expect crash)...${NC}"
    echo ""
    
    set +e  # Don't exit on failure - we expect this to crash
    docker run --memory=$MEMORY_LIMIT --rm $DOCKER_IMAGE
    exit_code=$?
    set -e
    
    echo ""
    if [ $exit_code -ne 0 ]; then
        echo -e "${RED}💀 Container crashed as expected (exit code: $exit_code)${NC}"
        echo -e "${YELLOW}This demonstrates what happens without memory management${NC}"
    else
        echo -e "${YELLOW}⚠️  Container didn't crash - you might need more aggressive data generation${NC}"
    fi
}

run_success_scenario() {
    print_header
    echo -e "${GREEN}✅ SUCCESS SCENARIO: With Memory Limiting${NC}"
    echo ""
    echo -e "${YELLOW}This will demonstrate successful memory management...${NC}"
    echo -e "Container memory limit: ${MEMORY_LIMIT}"
    echo -e "DLT memory limiting: ${GREEN}${DLT_MEMORY_LIMIT}MB (enabled)${NC}"
    echo ""
    echo "Press Enter to continue or Ctrl+C to cancel..."
    read
    
    echo -e "${BLUE}🚀 Starting container with memory management...${NC}"
    echo ""
    
    docker run --memory=$MEMORY_LIMIT --rm \
        -e DATA_WRITER__MAX_MEMORY_MB=$DLT_MEMORY_LIMIT \
        -e DATA_WRITER__MEMORY_CHECK_INTERVAL=1.0 \
        -e DATA_WRITER__FLUSH_THRESHOLD_PERCENT=0.7 \
        $DOCKER_IMAGE
    
    echo ""
    echo -e "${GREEN}🎉 Success! Memory management prevented OOM crash${NC}"
}

run_monitor_scenario() {
    print_header
    echo -e "${BLUE}📊 MONITORING SCENARIO: Real-time Memory Tracking${NC}"
    echo ""
    echo -e "${YELLOW}This will run the demo with real-time memory monitoring...${NC}"
    echo -e "Container memory limit: ${MEMORY_LIMIT}"
    echo -e "DLT memory limiting: ${GREEN}${DLT_MEMORY_LIMIT}MB (enabled)${NC}"
    echo -e "Monitoring: ${BLUE}Real-time docker stats${NC}"
    echo ""
    echo "Two terminals will be used:"
    echo "1. This terminal: Docker stats monitoring"
    echo "2. Background: DLT pipeline execution"
    echo ""
    echo "Press Enter to continue or Ctrl+C to cancel..."
    read
    
    # Start container in background
    container_name="ram-demo-monitor"
    
    echo -e "${BLUE}🚀 Starting container in background...${NC}"
    docker run --memory=$MEMORY_LIMIT --name $container_name \
        -e DATA_WRITER__MAX_MEMORY_MB=$DLT_MEMORY_LIMIT \
        -e DATA_WRITER__MEMORY_CHECK_INTERVAL=1.0 \
        -e DATA_WRITER__FLUSH_THRESHOLD_PERCENT=0.7 \
        $DOCKER_IMAGE &
    
    container_pid=$!
    
    # Wait a moment for container to start
    sleep 2
    
    echo -e "${BLUE}📊 Starting real-time monitoring (Ctrl+C to stop)...${NC}"
    echo ""
    
    # Monitor until container stops
    docker stats $container_name || true
    
    # Wait for container to finish
    wait $container_pid 2>/dev/null || true
    
    # Cleanup
    docker rm $container_name 2>/dev/null || true
    
    echo ""
    echo -e "${GREEN}📈 Monitoring complete${NC}"
}

clean_up() {
    print_header
    echo -e "${YELLOW}🧹 Cleaning up Docker resources...${NC}"
    echo ""
    
    # Remove containers
    echo "Removing containers..."
    docker ps -a --filter="ancestor=$DOCKER_IMAGE" --format="table {{.ID}}\t{{.Status}}" | tail -n +2 | awk '{print $1}' | xargs -r docker rm -f || true
    docker ps -a --filter="name=ram-demo" --format="table {{.ID}}\t{{.Status}}" | tail -n +2 | awk '{print $1}' | xargs -r docker rm -f || true
    
    # Remove image
    echo "Removing image..."
    docker rmi $DOCKER_IMAGE 2>/dev/null || true
    
    # Clean up dangling images
    echo "Cleaning up dangling images..."
    docker image prune -f || true
    
    echo ""
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# Main script logic
case "${1:-help}" in
    build)
        build_image
        ;;
    crash)
        if ! docker image inspect $DOCKER_IMAGE >/dev/null 2>&1; then
            echo -e "${YELLOW}⚠️  Docker image not found. Building first...${NC}"
            build_image
            echo ""
        fi
        run_crash_scenario
        ;;
    success)
        if ! docker image inspect $DOCKER_IMAGE >/dev/null 2>&1; then
            echo -e "${YELLOW}⚠️  Docker image not found. Building first...${NC}"
            build_image
            echo ""
        fi
        run_success_scenario
        ;;
    monitor)
        if ! docker image inspect $DOCKER_IMAGE >/dev/null 2>&1; then
            echo -e "${YELLOW}⚠️  Docker image not found. Building first...${NC}"
            build_image
            echo ""
        fi
        run_monitor_scenario
        ;;
    clean)
        clean_up
        ;;
    help|--help|-h)
        print_usage
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac
