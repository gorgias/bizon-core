#!/usr/bin/env python3
"""
Test data producer for Kafka async polling e2e tests
Produces high-volume and low-volume data to test starvation prevention
"""

import json
import os
import threading
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def wait_for_kafka(bootstrap_servers, max_retries=30):
    """Wait for Kafka to be ready"""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    for attempt in range(max_retries):
        try:
            metadata = admin_client.list_topics(timeout=5)
            print(f"✅ Kafka is ready! Found {len(metadata.topics)} topics")
            return True
        except Exception as e:
            print(f"⏳ Waiting for Kafka... (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2)

    raise Exception("Kafka not ready after maximum retries")


def create_test_topics(bootstrap_servers, topics):
    """Create test topics if they don't exist"""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Check existing topics
    existing_topics = admin_client.list_topics().topics.keys()
    topics_to_create = [topic for topic in topics if topic not in existing_topics]

    if not topics_to_create:
        print(f"✅ All topics already exist: {topics}")
        return

    # Create missing topics
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics_to_create]

    futures = admin_client.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"✅ Created topic: {topic}")
        except Exception as e:
            print(f"❌ Failed to create topic {topic}: {e}")


def produce_high_volume_bursts(producer, topic, duration_seconds=60):
    """Produce bursts of high-volume messages"""
    print(f"🚀 Starting high-volume producer for topic: {topic}")

    message_count = 0
    start_time = time.time()

    while time.time() - start_time < duration_seconds:
        # Produce burst of 20 messages
        for i in range(20):
            message = {
                "id": f"high-vol-{message_count}",
                "type": "high_volume",
                "timestamp": int(time.time() * 1000),
                "data": f"High volume burst message {message_count}",
                "batch_id": message_count // 20,
                "partition_key": message_count % 3,
            }

            producer.produce(
                topic, key=f"high-vol-{message_count}", value=json.dumps(message), partition=message_count % 3
            )

            message_count += 1

        # Flush burst and wait
        producer.flush()
        print(f"📦 High-volume: Sent burst of 20 messages (total: {message_count})")

        # Wait between bursts to simulate bursty traffic
        time.sleep(0.5)

    producer.flush()
    print(f"✅ High-volume producer finished. Total messages: {message_count}")


def produce_low_volume_steady(producer, topic, duration_seconds=60):
    """Produce steady low-volume messages"""
    print(f"🐌 Starting low-volume producer for topic: {topic}")

    message_count = 0
    start_time = time.time()

    while time.time() - start_time < duration_seconds:
        message = {
            "id": f"low-vol-{message_count}",
            "type": "low_volume",
            "timestamp": int(time.time() * 1000),
            "data": f"Low volume steady message {message_count}",
            "priority": "critical",  # These should never be starved!
            "sequence": message_count,
        }

        producer.produce(
            topic,
            key=f"low-vol-{message_count}",
            value=json.dumps(message),
            partition=0,  # Single partition for simplicity
        )

        message_count += 1

        if message_count % 5 == 0:
            producer.flush()
            print(f"📨 Low-volume: Sent message {message_count}")

        # Steady rate: 1 message every 3 seconds
        time.sleep(3)

    producer.flush()
    print(f"✅ Low-volume producer finished. Total messages: {message_count}")


def main():
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    duration = int(os.environ.get("PRODUCER_DURATION_SECONDS", "120"))  # 2 minutes

    high_volume_topic = "async-test-high-volume"
    low_volume_topic = "async-test-low-volume"
    topics = [high_volume_topic, low_volume_topic]

    print(f"🎯 Kafka Async Polling Test Data Producer")
    print(f"📡 Bootstrap servers: {bootstrap_servers}")
    print(f"⏱️  Duration: {duration} seconds")
    print(f"📋 Topics: {topics}")

    # Wait for Kafka
    wait_for_kafka(bootstrap_servers)

    # Create topics
    create_test_topics(bootstrap_servers, topics)

    # Setup producer
    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "async-test-producer",
        "acks": "all",  # Wait for all replicas
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 10,
        "buffer.memory": 33554432,
    }

    producer = Producer(producer_config)

    # Start both producers in parallel
    high_volume_thread = threading.Thread(
        target=produce_high_volume_bursts, args=(producer, high_volume_topic, duration)
    )

    low_volume_thread = threading.Thread(target=produce_low_volume_steady, args=(producer, low_volume_topic, duration))

    print("🚀 Starting producers...")
    high_volume_thread.start()
    low_volume_thread.start()

    # Wait for completion
    high_volume_thread.join()
    low_volume_thread.join()

    print("✅ All producers finished successfully!")


if __name__ == "__main__":
    main()
