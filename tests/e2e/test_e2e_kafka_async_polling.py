import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List

import pytest
import yaml
from confluent_kafka import Consumer, Producer

from bizon.engine.engine import RunnerFactory


class MockKafkaProducer:
    """Mock Kafka producer to simulate high-volume and low-volume topics"""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": "bizon-test-producer"})

    def produce_high_volume_topic(self, topic: str, num_messages: int = 100):
        """Produce many messages to simulate a high-volume topic"""
        for i in range(num_messages):
            message = {
                "id": f"high-vol-{i}",
                "type": "high_volume",
                "timestamp": int(time.time() * 1000),
                "data": f"High volume message {i}",
                "batch_id": i // 10,  # Group messages in batches
            }

            self.producer.produce(
                topic, key=f"high-vol-{i}", value=json.dumps(message), partition=i % 3  # Spread across 3 partitions
            )

        # Flush every 10 messages to create bursts
        if num_messages > 0:
            self.producer.flush()

    def produce_low_volume_topic(self, topic: str, num_messages: int = 5):
        """Produce few messages to simulate a low-volume topic"""
        for i in range(num_messages):
            message = {
                "id": f"low-vol-{i}",
                "type": "low_volume",
                "timestamp": int(time.time() * 1000),
                "data": f"Low volume message {i}",
                "priority": "high",  # These should not be starved
            }

            self.producer.produce(
                topic, key=f"low-vol-{i}", value=json.dumps(message), partition=0  # Single partition for simplicity
            )

            # Add delay between messages to simulate low volume
            time.sleep(0.1)

        if num_messages > 0:
            self.producer.flush()

    def close(self):
        self.producer.flush()


@pytest.fixture(scope="module")
def temp_output_dir():
    """Create temporary directory for test output files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture(scope="module")
def kafka_producer():
    """Create and cleanup Kafka producer"""
    producer = MockKafkaProducer()
    yield producer
    producer.close()


def create_bizon_config(topics: List[str], output_dir: str, enable_async_polling: bool = True) -> str:
    """Create bizon configuration for Kafka async polling test"""

    # Create topic configs
    topic_configs = []
    for i, topic in enumerate(topics):
        topic_configs.append({"name": topic, "destination_id": topic.replace("-", "_")})  # file-safe names

    config = {
        "name": "test_kafka_async_polling",
        "source": {
            "name": "kafka",
            "stream": "topic",
            "sync_mode": "stream",
            "topics": topic_configs,
            "bootstrap_servers": "localhost:9092",
            "group_id": "bizon-async-test",
            "batch_size": 50,
            "consumer_timeout": 5,
            "enable_async_polling": enable_async_polling,
            "poll_interval_ms": 50,
            "max_poll_records": 100,
            "partition_pause_threshold": 500,
            "skip_message_empty_value": True,
            "message_encoding": "utf-8",
            "max_iterations": 10,  # Limit test duration
            "authentication": {
                "type": "basic",
                "params": {"username": "test", "password": "test"},
                "schema_registry_type": "apicurio",
                "schema_registry_url": "",
                "schema_registry_username": "",
                "schema_registry_password": "",
            },
        },
        "destination": {"name": "file", "config": {"output_dir": output_dir, "record_schemas": []}},
        "engine": {
            "runner": {"type": "stream"},
            "backend": {"type": "sqlite", "config": {"database_path": os.path.join(output_dir, "test.db")}},
        },
    }

    # Add record schemas for each topic
    for topic in topics:
        destination_id = topic.replace("-", "_")
        config["destination"]["config"]["record_schemas"].append(
            {
                "destination_id": destination_id,
                "record_schema": [
                    {"name": "id", "type": "string", "nullable": False},
                    {"name": "type", "type": "string", "nullable": False},
                    {"name": "timestamp", "type": "integer", "nullable": False},
                    {"name": "data", "type": "string", "nullable": False},
                ],
            }
        )

    return yaml.dump(config)


def analyze_output_files(output_dir: str, topics: List[str]) -> Dict[str, List[Dict]]:
    """Analyze output files to verify fair consumption"""
    results = {}

    for topic in topics:
        destination_id = topic.replace("-", "_")
        file_path = os.path.join(output_dir, f"{destination_id}.json")
        records = []

        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        records.append(record)
                    except json.JSONDecodeError:
                        continue

        results[topic] = records

    return results


class TestKafkaAsyncPollingE2E:
    """End-to-end tests for Kafka async polling with file destination"""

    @pytest.mark.skip(reason="Requires running Kafka instance")
    def test_async_polling_prevents_starvation(self, temp_output_dir, kafka_producer):
        """Test that async polling prevents low-volume topic starvation"""

        high_volume_topic = "high-volume-test-topic"
        low_volume_topic = "low-volume-test-topic"
        topics = [high_volume_topic, low_volume_topic]

        # Step 1: Produce test data
        print("Producing test data...")

        # Produce high volume data in background
        def produce_high_volume():
            for batch in range(3):  # 3 batches of 50 messages = 150 total
                kafka_producer.produce_high_volume_topic(high_volume_topic, 50)
                time.sleep(1)  # Small delay between batches

        # Produce low volume data
        def produce_low_volume():
            for batch in range(5):  # 5 messages over 5 seconds
                kafka_producer.produce_low_volume_topic(low_volume_topic, 1)
                time.sleep(2)  # 2 second delay between messages

        # Start producers
        high_vol_thread = threading.Thread(target=produce_high_volume)
        low_vol_thread = threading.Thread(target=produce_low_volume)

        high_vol_thread.start()
        low_vol_thread.start()

        # Wait for producers to finish
        high_vol_thread.join()
        low_vol_thread.join()

        print(f"Data produced to topics: {topics}")

        # Step 2: Run bizon with async polling enabled
        print("Running bizon with async polling enabled...")

        config_yaml = create_bizon_config(topics, temp_output_dir, enable_async_polling=True)
        config_dict = yaml.safe_load(config_yaml)

        runner = RunnerFactory.create_from_config_dict(config_dict)

        # Run the pipeline
        result = runner.run()
        print(f"Pipeline completed with status: {result}")

        # Step 3: Analyze results
        print("Analyzing output files...")
        results = analyze_output_files(temp_output_dir, topics)

        # Verify both topics were consumed
        high_vol_records = results[high_volume_topic]
        low_vol_records = results[low_volume_topic]

        print(f"High volume topic records: {len(high_vol_records)}")
        print(f"Low volume topic records: {len(low_vol_records)}")

        # Assertions
        assert len(high_vol_records) > 0, "High volume topic should have records"
        assert len(low_vol_records) > 0, "Low volume topic should NOT be starved - this is the key test!"

        # Verify record content
        if high_vol_records:
            assert high_vol_records[0]["type"] == "high_volume"
            assert "high-vol-" in high_vol_records[0]["id"]

        if low_vol_records:
            assert low_vol_records[0]["type"] == "low_volume"
            assert "low-vol-" in low_vol_records[0]["id"]

        # Key metric: Low volume topic should get fair representation
        # Even though high-volume has 30x more messages, low-volume should not be starved
        total_records = len(high_vol_records) + len(low_vol_records)
        low_vol_percentage = len(low_vol_records) / total_records if total_records > 0 else 0

        print(f"Low volume topic percentage: {low_vol_percentage:.2%}")

        # Low volume should get at least some representation (not zero)
        assert low_vol_percentage > 0.01, f"Low volume topic got {low_vol_percentage:.2%} - this indicates starvation!"

        print("✅ Async polling successfully prevented topic starvation!")

    @pytest.mark.skip(reason="Requires running Kafka instance")
    def test_sync_vs_async_polling_comparison(self, temp_output_dir, kafka_producer):
        """Compare sync vs async polling to demonstrate starvation prevention"""

        high_volume_topic = "comparison-high-vol"
        low_volume_topic = "comparison-low-vol"
        topics = [high_volume_topic, low_volume_topic]

        # Produce test data
        print("Producing comparison test data...")
        kafka_producer.produce_high_volume_topic(high_volume_topic, 200)  # Lots of messages
        kafka_producer.produce_low_volume_topic(low_volume_topic, 3)  # Few messages

        results_comparison = {}

        for polling_mode in ["sync", "async"]:
            print(f"\nTesting {polling_mode} polling...")

            mode_output_dir = os.path.join(temp_output_dir, polling_mode)
            os.makedirs(mode_output_dir, exist_ok=True)

            # Configure bizon for this polling mode
            enable_async = polling_mode == "async"
            config_yaml = create_bizon_config(topics, mode_output_dir, enable_async_polling=enable_async)
            config_dict = yaml.safe_load(config_yaml)

            # Run pipeline
            runner = RunnerFactory.create_from_config_dict(config_dict)
            runner.run()

            # Analyze results
            results = analyze_output_files(mode_output_dir, topics)
            results_comparison[polling_mode] = results

            high_vol_count = len(results[high_volume_topic])
            low_vol_count = len(results[low_volume_topic])
            total = high_vol_count + low_vol_count
            low_vol_pct = (low_vol_count / total * 100) if total > 0 else 0

            print(f"{polling_mode.upper()} Results:")
            print(f"  High volume: {high_vol_count} records")
            print(f"  Low volume: {low_vol_count} records ({low_vol_pct:.1f}%)")

        # Compare results
        sync_low_vol = len(results_comparison["sync"][low_volume_topic])
        async_low_vol = len(results_comparison["async"][low_volume_topic])

        print(f"\nComparison:")
        print(f"  Sync polling - Low volume records: {sync_low_vol}")
        print(f"  Async polling - Low volume records: {async_low_vol}")

        # Async should consume more low-volume messages (less starvation)
        assert async_low_vol >= sync_low_vol, "Async polling should prevent starvation better than sync"

        if sync_low_vol == 0 and async_low_vol > 0:
            print("✅ Async polling prevented complete starvation that occurred with sync polling!")
        elif async_low_vol > sync_low_vol:
            improvement = ((async_low_vol - sync_low_vol) / max(sync_low_vol, 1)) * 100
            print(f"✅ Async polling improved low-volume consumption by {improvement:.0f}%!")


def test_kafka_config_generation():
    """Unit test for configuration generation"""
    topics = ["test-topic-1", "test-topic-2"]
    output_dir = "/tmp/test"

    config_yaml = create_bizon_config(topics, output_dir)
    config_dict = yaml.safe_load(config_yaml)

    # Verify config structure
    assert config_dict["source"]["name"] == "kafka"
    assert config_dict["source"]["enable_async_polling"] == True
    assert len(config_dict["source"]["topics"]) == 2
    assert config_dict["source"]["topics"][0]["name"] == "test-topic-1"
    assert config_dict["source"]["topics"][0]["destination_id"] == "test_topic_1"

    assert config_dict["destination"]["name"] == "file"
    assert len(config_dict["destination"]["config"]["record_schemas"]) == 2


if __name__ == "__main__":
    """Run manual test for development"""

    # For manual testing without pytest
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        producer = MockKafkaProducer()

        # Quick test of config generation
        test_kafka_config_generation()
        print("✅ Config generation test passed")

        print("Manual end-to-end test would require running Kafka...")
        print("Use pytest to run the full e2e tests with a Kafka instance")

        producer.close()
