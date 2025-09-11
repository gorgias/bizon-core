# Kafka Async Polling E2E Test Setup

This directory contains the setup for running end-to-end tests of Kafka async polling with topic starvation prevention.

## Quick Start

1. **Start Kafka cluster:**
   ```bash
   cd tests/e2e/kafka_async_test_setup/
   docker-compose up -d
   ```

2. **Wait for services to be ready** (about 30 seconds)

3. **Run the test:**
   ```bash
   # From bizon-core root directory
   pytest tests/e2e/test_e2e_kafka_async_polling.py::TestKafkaAsyncPollingE2E::test_async_polling_prevents_starvation -v -s
   ```

4. **View Kafka UI** (optional):
   - Open http://localhost:8080
   - Monitor topics and consumer groups

5. **Cleanup:**
   ```bash
   docker-compose down -v
   ```

## Test Scenario

The test simulates a realistic scenario where async polling prevents topic starvation:

### 🔥 **High-Volume Topic**: `async-test-high-volume`
- **Pattern**: Bursts of 20 messages every 500ms
- **Total**: ~2400 messages over 2 minutes  
- **Partitions**: 3 (load balanced)
- **Simulation**: Heavy analytics/logging data

### 🐌 **Low-Volume Topic**: `async-test-low-volume`
- **Pattern**: 1 message every 3 seconds
- **Total**: ~40 messages over 2 minutes
- **Partitions**: 1 
- **Simulation**: Critical alerts that must not be starved

## Expected Results

### ✅ **With Async Polling (New)**
- **High-volume**: ~1000+ messages consumed
- **Low-volume**: ~20+ messages consumed (NO starvation!)
- **Fairness**: Low-volume gets ≥1% representation

### ❌ **With Sync Polling (Traditional)**
- **High-volume**: ~1000+ messages consumed  
- **Low-volume**: 0-5 messages consumed (STARVED!)
- **Problem**: Low-volume completely dominated

## Test Architecture

```
┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│  Docker Kafka    │    │   Bizon Async   │    │   File Output    │
│                  │    │     Poller      │    │                  │
│ • High-vol topic │───▶│                 │───▶│ • high_vol.json  │
│ • Low-vol topic  │    │ Fair round-robin│    │ • low_vol.json   │
│                  │    │ 50ms polling    │    │                  │
└──────────────────┘    └─────────────────┘    └──────────────────┘
```

## Configuration Details

The test uses these Kafka async polling settings:
- `enable_async_polling: true`
- `poll_interval_ms: 50` (fast polling)
- `max_poll_records: 100` (reasonable batches)
- `partition_pause_threshold: 500` (backpressure control)

## Manual Testing

You can also run components individually:

1. **Start only Kafka:**
   ```bash
   docker-compose up kafka zookeeper -d
   ```

2. **Run test producer manually:**
   ```bash
   docker-compose run test-producer
   ```

3. **Create custom bizon config** and test different scenarios

## Troubleshooting

- **"Kafka not ready"**: Wait 30-60 seconds for Kafka startup
- **"Topic not found"**: Check if test-producer created topics
- **"Connection refused"**: Ensure Docker containers are running
- **Permission errors**: Check Docker daemon is running

## Files

- `docker-compose.yml`: Kafka cluster setup
- `producer_script.py`: Test data generator  
- `producer.Dockerfile`: Producer container
- `../test_e2e_kafka_async_polling.py`: Main test file

This setup provides a realistic test environment to validate that the async polling implementation successfully prevents topic starvation in production-like scenarios.