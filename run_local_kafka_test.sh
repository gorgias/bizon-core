#!/bin/bash

# Local Kafka E2E Test Runner
# Uses the existing kafka_async_test_setup configuration

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✅${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ❌${NC} $1"
}

warning() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠️${NC} $1"
}

# Check if we're in the right directory
if [ ! -d "tests/e2e/kafka_async_test_setup" ]; then
    error "Please run this script from the bizon-core root directory"
    exit 1
fi

# Function to cleanup
cleanup() {
    log "Cleaning up..."
    cd tests/e2e/kafka_async_test_setup
    docker-compose down -v 2>/dev/null || true
    cd - >/dev/null
    
    # Clean up output files
    rm -f high_volume.json low_volume.json kafka_test.json
    
    success "Cleanup completed"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

echo "🚀 Starting Local Kafka E2E Test"
echo "================================="

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    error "Docker is not running. Please start Docker first."
    exit 1
fi

# Navigate to test setup directory
cd tests/e2e/kafka_async_test_setup

log "Starting Kafka cluster..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d

log "Waiting for Kafka to be ready..."
# Wait for Kafka to be ready (check every 5 seconds for up to 2 minutes)
for i in {1..24}; do
    if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        success "Kafka is ready!"
        break
    fi
    
    if [ $i -eq 24 ]; then
        error "Kafka failed to start within 2 minutes"
        docker-compose logs kafka
        exit 1
    fi
    
    log "Waiting for Kafka... (attempt $i/24)"
    sleep 5
done

# Optional: Start the test producer to generate data
log "Starting test data producer..."
docker-compose up test-producer &
PRODUCER_PID=$!

# Wait a bit for some data to be produced
sleep 10

log "Stopping producer and running bizon test..."
kill $PRODUCER_PID 2>/dev/null || true

# Navigate back to root directory for running the test
cd - >/dev/null

# Optional: View Kafka UI
log "Kafka UI is available at: http://localhost:8080"

# Run the actual test
log "Running Kafka async polling E2E test..."

if python -m pytest tests/e2e/test_e2e_kafka_async_polling.py::TestKafkaAsyncPollingE2E::test_async_polling_prevents_starvation -v -s; then
    success "Test passed! Async polling prevented topic starvation."
    echo ""
    echo "📊 Test Results Summary:"
    echo "========================"
    echo "✅ Kafka cluster started successfully"
    echo "✅ Test data produced (high and low volume topics)"  
    echo "✅ Async polling prevented low-volume topic starvation"
    echo "✅ File outputs generated and validated"
    echo ""
    echo "🎯 The async polling implementation is working correctly!"
else
    error "Test failed!"
    echo ""
    echo "❌ Test Results Summary:"
    echo "========================"
    echo "❌ Async polling test failed"
    echo "💡 Check the test output above for details"
    echo "🔍 You can inspect Kafka topics at: http://localhost:8080"
    exit 1
fi
