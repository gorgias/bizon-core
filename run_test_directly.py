#!/usr/bin/env python3
"""
Direct test runner - bypasses pytest skip markers
"""

import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import yaml
from confluent_kafka import Producer

from bizon.engine.engine import RunnerFactory


def log(message: str, level: str = "INFO"):
    """Simple logging"""
    icons = {"INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌", "WARNING": "⚠️"}
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {icons.get(level, '📝')} {message}")


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


def create_bizon_config(topics: list, output_dir: str, enable_async_polling: bool = True) -> str:
    """Create bizon configuration for Kafka async polling test"""

    # Create topic configs (matching CI format exactly)
    topic_configs = [
        {"name": "high-volume-test-topic", "destination_id": "high_volume_test_topic"},
        {"name": "low-volume-test-topic", "destination_id": "low_volume_test_topic"},
    ]

    config = {
        "name": "test_kafka_async_polling",
        "source": {
            "name": "kafka",
            "stream": "topic",
            "sync_mode": "stream",
            "topics": topic_configs,
            "bootstrap_servers": "localhost:9092",
            "group_id": f"bizon-async-test-{int(time.time())}",
            "batch_size": 50,
            "consumer_timeout": 5,
            "enable_async_polling": enable_async_polling,
            "poll_interval_ms": 50,
            "max_poll_records": 100,
            "partition_pause_threshold": 500,
            "skip_message_empty_value": True,
            "message_encoding": "utf-8",
            "max_iterations": 8,
            "consumer_config": {
                "security.protocol": "PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "auto.offset.reset": "earliest",
            },
            "authentication": {
                "type": "basic",
                "params": {"username": "test", "password": "test"},
                "schema_registry_type": "apicurio",
                "schema_registry_url": "http://localhost:8081",
                "schema_registry_username": "test",
                "schema_registry_password": "test",
            },
        },
        "destination": {
            "name": "file",
            "config": {"format": "json", "destination_id": "kafka_test", "record_schemas": []},
        },
        "engine": {
            "runner": {"type": "stream"},
            "backend": {"type": "sqlite", "config": {"database": "bizon_test", "schema": "public"}},
        },
    }

    # Add record schemas for each topic (matching CI format exactly)
    config["destination"]["config"]["record_schemas"] = [
        {
            "destination_id": "high_volume_test_topic",
            "record_schema": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "type", "type": "string", "nullable": False},
                {"name": "data", "type": "string", "nullable": False},
                {"name": "timestamp", "type": "integer", "nullable": False},
            ],
        },
        {
            "destination_id": "low_volume_test_topic",
            "record_schema": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "type", "type": "string", "nullable": False},
                {"name": "data", "type": "string", "nullable": False},
                {"name": "priority", "type": "string", "nullable": False},
                {"name": "timestamp", "type": "integer", "nullable": False},
            ],
        },
    ]

    return yaml.dump(config)


def analyze_output_files(topics: list):
    """Analyze output files to verify fair consumption"""
    results = {}

    # Files are created based on destination_id from record schemas
    file_mappings = {
        "high-volume-test-topic": "high_volume_test_topic.json",
        "low-volume-test-topic": "low_volume_test_topic.json",
    }

    for topic in topics:
        file_path = file_mappings.get(topic, f"{topic.replace('-', '_')}.json")
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


def test_async_polling_prevents_starvation():
    """Test that async polling prevents low-volume topic starvation"""

    log("🚀 Starting Kafka Async Polling Starvation Test")

    high_volume_topic = "high-volume-test-topic"
    low_volume_topic = "low-volume-test-topic"
    topics = [high_volume_topic, low_volume_topic]

    with tempfile.TemporaryDirectory() as temp_output_dir:
        kafka_producer = MockKafkaProducer()

        try:
            # Step 1: Produce test data
            log("Producing test data...")

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

            log(f"Data produced to topics: {topics}")

            # Step 2: Run bizon with async polling enabled
            log("Running bizon with async polling enabled...")

            config_yaml = create_bizon_config(topics, temp_output_dir, enable_async_polling=True)
            config_dict = yaml.safe_load(config_yaml)

            # Set environment for production mode
            os.environ["ENVIRONMENT"] = "production"

            runner = RunnerFactory.create_from_config_dict(config_dict)

            # Run the pipeline
            result = runner.run()
            log(f"Pipeline completed with status: {result}")

            # Step 3: Analyze results
            log("Analyzing output files...")
            results = analyze_output_files(temp_output_dir, topics)

            # Verify both topics were consumed
            high_vol_records = results[high_volume_topic]
            low_vol_records = results[low_volume_topic]

            log(f"High volume topic records: {len(high_vol_records)}")
            log(f"Low volume topic records: {len(low_vol_records)}")

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

            log(f"Low volume topic percentage: {low_vol_percentage:.2%}")

            # Low volume should get at least some representation (not zero)
            assert (
                low_vol_percentage > 0.01
            ), f"Low volume topic got {low_vol_percentage:.2%} - this indicates starvation!"

            log("✅ Async polling successfully prevented topic starvation!", "SUCCESS")

            # Display results
            print("\n" + "=" * 60)
            print("📊 KAFKA ASYNC POLLING TEST RESULTS")
            print("=" * 60)
            print(f"High-volume messages consumed: {len(high_vol_records)}")
            print(f"Low-volume messages consumed:  {len(low_vol_records)}")
            print(f"Total messages consumed:       {total_records}")
            print(f"Low-volume representation:     {low_vol_percentage:.2%}")
            print("\n🎉 SUCCESS: Async polling prevented topic starvation!")

            return True

        except Exception as e:
            log(f"Test failed with error: {e}", "ERROR")
            return False
        finally:
            kafka_producer.close()


if __name__ == "__main__":
    success = test_async_polling_prevents_starvation()
    sys.exit(0 if success else 1)
