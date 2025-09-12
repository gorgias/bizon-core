# Local Kafka E2E Testing

The CI `kafka-e2e` test is failing, so this guide shows you how to run the same test locally using the existing test setup.

## Quick Start

### Option 1: Using the Shell Script (Recommended)
```bash
# From bizon-core root directory
./run_local_kafka_test.sh
```

### Option 2: Using the Python Script
```bash
# From bizon-core root directory  
python run_kafka_test_simple.py
```

### Option 3: Manual Steps (for debugging)
```bash
# 1. Start Kafka cluster
cd tests/e2e/kafka_async_test_setup/
docker-compose up -d

# 2. Wait for Kafka to be ready (~30 seconds)
# Check: docker-compose logs kafka

# 3. Optional: View Kafka UI at http://localhost:8080

# 4. Run the test from bizon-core root
cd ../../..
python -m pytest tests/e2e/test_e2e_kafka_async_polling.py::TestKafkaAsyncPollingE2E::test_async_polling_prevents_starvation -v -s

# 5. Cleanup
cd tests/e2e/kafka_async_test_setup/
docker-compose down -v
```

## What the Test Does

This replicates the exact same test that's failing in CI:

1. **Starts Kafka cluster** using the existing `kafka_async_test_setup/docker-compose.yml`
2. **Produces test data**:
   - High-volume topic: Bursts of messages (simulates heavy load)
   - Low-volume topic: Steady critical messages (must not be starved)
3. **Runs bizon with async polling** to consume from both topics
4. **Validates results**: Ensures low-volume topic gets fair representation

## Expected Results

### ✅ Success (Async Polling Working)
- High-volume topic: ~1000+ messages consumed
- Low-volume topic: ~20+ messages consumed (NO starvation!)
- Low-volume gets ≥1% representation despite being outnumbered

### ❌ Failure (Starvation Detected)
- High-volume topic: ~1000+ messages consumed  
- Low-volume topic: 0-5 messages consumed (STARVED!)
- Async polling is not working correctly

## Debugging

### If Kafka won't start:
```bash
# Check Docker is running
docker info

# View Kafka logs
cd tests/e2e/kafka_async_test_setup/
docker-compose logs kafka

# Try the simple compose file
docker-compose -f simple-docker-compose.yml up -d
```

### If test fails:
```bash
# Run with verbose output
python run_kafka_test_simple.py --verbose

# Keep containers running for inspection
python run_kafka_test_simple.py --no-cleanup

# Check Kafka UI: http://localhost:8080
# Look for topics: async-test-high-volume, async-test-low-volume
```

### If test passes locally but fails in CI:
- Environment differences (timing, resources)
- Docker version differences
- Network configuration issues
- Consider increasing timeouts in the test

## Files Overview

- `tests/e2e/kafka_async_test_setup/`: Complete test environment
  - `docker-compose.yml`: Full Kafka cluster with UI and producer
  - `simple-docker-compose.yml`: Minimal Kafka-only setup
  - `producer_script.py`: Test data generator
  - `README.md`: Detailed documentation
- `tests/e2e/test_e2e_kafka_async_polling.py`: The actual test
- `run_local_kafka_test.sh`: Automated test runner (shell)
- `run_kafka_test_simple.py`: Automated test runner (Python)

This setup gives you the exact same test environment as CI, allowing you to debug the async polling implementation locally.
