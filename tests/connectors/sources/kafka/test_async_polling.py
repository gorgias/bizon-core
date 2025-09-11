import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import Consumer, Message, TopicPartition

from bizon.connectors.sources.kafka.src.async_poller import (
    AsyncKafkaPoller,
    PartitionMessageQueue,
)
from bizon.connectors.sources.kafka.src.config import (
    KafkaAuthConfig,
    KafkaSourceConfig,
    TopicConfig,
)
from bizon.source.auth.authenticators.basic import BasicHttpAuthParams
from bizon.source.auth.config import AuthType


@pytest.fixture
def mock_consumer():
    """Mock Kafka consumer"""
    consumer = Mock(spec=Consumer)
    consumer.poll.return_value = None
    consumer.commit.return_value = None
    consumer.pause.return_value = None
    consumer.resume.return_value = None
    return consumer


@pytest.fixture
def kafka_config():
    """Sample Kafka configuration for testing"""
    auth_params = BasicHttpAuthParams(username="test_user", password="test_pass")

    auth_config = KafkaAuthConfig(type=AuthType.BASIC, params=auth_params)

    topics = [
        TopicConfig(name="high-volume-topic", destination_id="dest1"),
        TopicConfig(name="low-volume-topic", destination_id="dest2"),
    ]

    return KafkaSourceConfig(
        name="test-kafka-source",
        stream="topic",
        topics=topics,
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        batch_size=100,
        consumer_timeout=10,
        enable_async_polling=True,
        poll_interval_ms=50,
        max_poll_records=500,
        authentication=auth_config,
    )


@pytest.fixture
def async_poller(mock_consumer, kafka_config):
    """AsyncKafkaPoller instance for testing"""
    return AsyncKafkaPoller(mock_consumer, kafka_config)


class TestPartitionMessageQueue:
    """Test the PartitionMessageQueue class"""

    def test_partition_queue_basic_operations(self):
        queue = PartitionMessageQueue(max_size=5)

        # Create mock message
        mock_message = Mock()
        mock_message.offset.return_value = 100

        # Test basic put/get
        assert queue.put(mock_message) is True
        assert queue.size() == 1

        retrieved = queue.get(timeout=0.1)
        assert retrieved == mock_message
        assert queue.size() == 0

    def test_partition_queue_full_behavior(self):
        queue = PartitionMessageQueue(max_size=2)

        # Fill the queue
        for i in range(2):
            mock_msg = Mock()
            assert queue.put(mock_msg) is True

        # Next put should fail (queue full)
        mock_msg = Mock()
        assert queue.put(mock_msg) is False
        assert queue.size() == 2

    def test_partition_queue_paused_behavior(self):
        queue = PartitionMessageQueue(max_size=5)
        queue.paused = True

        mock_message = Mock()
        assert queue.put(mock_message) is False
        assert queue.size() == 0

    def test_offset_tracking(self):
        queue = PartitionMessageQueue()

        # Test inflight -> completed flow
        queue.mark_inflight(0)
        queue.mark_inflight(1)
        queue.mark_inflight(2)

        assert 0 in queue.inflight_offsets
        assert 1 in queue.inflight_offsets
        assert 2 in queue.inflight_offsets

        # Complete out of order
        queue.mark_completed(0)
        queue.mark_completed(2)

        # Should only advance to 0 since 1 is still inflight
        assert queue.get_safe_commit_offset() == 1  # 0 + 1

        # Complete 1
        queue.mark_completed(1)
        assert queue.get_safe_commit_offset() == 3  # 2 + 1


class TestAsyncKafkaPoller:
    """Test the AsyncKafkaPoller class"""

    def test_poller_initialization(self, async_poller, kafka_config):
        assert async_poller.config == kafka_config
        assert not async_poller.running
        assert async_poller.poll_thread is None
        assert len(async_poller.partition_queues) == 0

    def test_poller_start_stop(self, async_poller):
        # Test start
        async_poller.start()
        assert async_poller.running is True
        assert async_poller.poll_thread is not None
        assert async_poller.poll_thread.daemon is True

        # Give thread time to start
        time.sleep(0.1)

        # Test stop
        async_poller.stop()
        assert async_poller.running is False

    def test_message_handling(self, async_poller, mock_consumer):
        """Test that messages are properly queued per partition"""
        # Create mock messages from different partitions
        msg1 = Mock(spec=Message)
        msg1.topic.return_value = "test-topic"
        msg1.partition.return_value = 0
        msg1.offset.return_value = 100
        msg1.error.return_value = None

        msg2 = Mock(spec=Message)
        msg2.topic.return_value = "test-topic"
        msg2.partition.return_value = 1
        msg2.offset.return_value = 200
        msg2.error.return_value = None

        # Handle the messages
        async_poller._handle_message(msg1)
        async_poller._handle_message(msg2)

        # Check that partition queues were created
        assert len(async_poller.partition_queues) == 2
        assert ("test-topic", 0) in async_poller.partition_queues
        assert ("test-topic", 1) in async_poller.partition_queues

        # Check messages are in correct queues
        assert async_poller.partition_queues[("test-topic", 0)].size() == 1
        assert async_poller.partition_queues[("test-topic", 1)].size() == 1

    def test_fair_message_retrieval(self, async_poller):
        """Test that get_messages implements round-robin fairness and auto-marks as processed"""
        # Add messages to different partitions
        for partition in [0, 1]:
            for offset in range(5):  # 5 messages per partition
                msg = Mock(spec=Message)
                msg.topic.return_value = "test-topic"
                msg.partition.return_value = partition
                msg.offset.return_value = offset
                msg.error.return_value = None

                async_poller._handle_message(msg)

        # Get messages with limit
        messages = async_poller.get_messages(max_messages=6)

        # Should get 6 messages total
        assert len(messages) == 6

        # Should be alternating partitions (round-robin)
        partitions = [msg.partition() for msg in messages]

        # With round-robin, we should see both partitions represented
        assert 0 in partitions
        assert 1 in partitions

        # Messages should be marked as inflight (not completed yet)
        for partition in [0, 1]:
            partition_key = ("test-topic", partition)
            partition_queue = async_poller.partition_queues[partition_key]
            # Some offsets should be marked as inflight
            assert len(partition_queue.inflight_offsets) > 0

        # Simulate successful destination write
        async_poller.mark_messages_completed(messages)

        # Now messages should be completed
        for partition in [0, 1]:
            partition_key = ("test-topic", partition)
            partition_queue = async_poller.partition_queues[partition_key]
            assert len(partition_queue.completed_offsets) > 0

    def test_partition_pause_resume(self, async_poller, mock_consumer):
        """Test partition pausing when queue gets full"""
        # Fill up a partition queue beyond threshold
        test_partition = ("test-topic", 0)
        queue = PartitionMessageQueue(max_size=2)  # Small size for testing
        async_poller.partition_queues[test_partition] = queue

        # Create topic partition for pause/resume
        tp = TopicPartition("test-topic", 0)

        # Simulate queue getting full
        async_poller._pause_partition(tp)

        # Check that pause was called
        mock_consumer.pause.assert_called_once_with([tp])
        assert tp in async_poller.paused_partitions

        # Test resume
        async_poller._resume_partition(tp)
        mock_consumer.resume.assert_called_once_with([tp])
        assert tp not in async_poller.paused_partitions

    def test_message_inflight_tracking(self, async_poller):
        """Test that messages are marked as inflight when consumed, completed after destination write"""
        # Add a message
        msg = Mock(spec=Message)
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        msg.offset.return_value = 100
        msg.error.return_value = None

        async_poller._handle_message(msg)

        # Get the message (marks as inflight, NOT completed)
        messages = async_poller.get_messages(max_messages=1)
        assert len(messages) == 1

        # Check that it's marked as inflight, not completed
        partition_key = ("test-topic", 0)
        queue = async_poller.partition_queues[partition_key]
        assert 100 in queue.inflight_offsets
        assert 100 not in queue.completed_offsets

        # Mark as completed after "successful destination write"
        async_poller.mark_messages_completed(messages)

        # Now it should be completed and removed from inflight
        assert 100 not in queue.inflight_offsets
        assert 100 in queue.completed_offsets

    def test_commit_offsets_generation(self, async_poller):
        """Test generation of safe commit offsets"""
        # Set up some completed offsets
        partition_key = ("test-topic", 0)
        queue = PartitionMessageQueue()
        async_poller.partition_queues[partition_key] = queue

        # Mark some offsets as completed starting from 0
        queue.mark_completed(0)
        queue.mark_completed(1)
        queue.mark_completed(2)

        # Get commit offsets
        commit_offsets = async_poller.get_commit_offsets()

        # Should have one commit offset for the partition
        assert len(commit_offsets) == 1
        assert commit_offsets[0].topic == "test-topic"
        assert commit_offsets[0].partition == 0
        assert commit_offsets[0].offset == 3  # 2 + 1

    def test_rebalance_callbacks(self, async_poller, mock_consumer):
        """Test partition assignment and revocation callbacks"""
        # Test assignment
        partitions = [
            TopicPartition("topic1", 0),
            TopicPartition("topic1", 1),
        ]

        async_poller.on_assign(mock_consumer, partitions)

        # Check that partition queues were created
        assert ("topic1", 0) in async_poller.partition_queues
        assert ("topic1", 1) in async_poller.partition_queues
        assert len(async_poller.assigned_partitions) == 2

        # Test revocation with some commit offsets (need sequential from 0)
        async_poller.partition_queues[("topic1", 0)].mark_completed(0)
        async_poller.partition_queues[("topic1", 0)].mark_completed(1)

        async_poller.on_revoke(mock_consumer, partitions)

        # Should have tried to commit offsets
        mock_consumer.commit.assert_called_once()

        # Partitions should be removed from assigned set
        assert len(async_poller.assigned_partitions) == 0


class TestIntegration:
    """Integration tests for async polling"""

    @patch("time.sleep")  # Mock sleep to speed up tests
    def test_async_polling_prevents_starvation(self, mock_sleep, async_poller, mock_consumer):
        """Test that async polling provides fair access to all partitions"""

        # Simulate high volume topic (partition 0) and low volume topic (partition 1)
        high_volume_messages = []
        low_volume_messages = []

        # Create many messages for high volume partition
        for i in range(100):
            msg = Mock(spec=Message)
            msg.topic.return_value = "high-volume-topic"
            msg.partition.return_value = 0
            msg.offset.return_value = i
            msg.error.return_value = None
            high_volume_messages.append(msg)

        # Create few messages for low volume partition
        for i in range(5):
            msg = Mock(spec=Message)
            msg.topic.return_value = "low-volume-topic"
            msg.partition.return_value = 0
            msg.offset.return_value = i
            msg.error.return_value = None
            low_volume_messages.append(msg)

        # Add all messages to queues
        for msg in high_volume_messages:
            async_poller._handle_message(msg)
        for msg in low_volume_messages:
            async_poller._handle_message(msg)

        # Get messages in batches and verify fairness
        total_retrieved = 0
        high_volume_count = 0
        low_volume_count = 0

        # Retrieve messages in multiple batches
        while total_retrieved < 50 and (high_volume_count == 0 or low_volume_count == 0):
            messages = async_poller.get_messages(max_messages=10)
            if not messages:
                break

            for msg in messages:
                if msg.topic() == "high-volume-topic":
                    high_volume_count += 1
                elif msg.topic() == "low-volume-topic":
                    low_volume_count += 1
                total_retrieved += 1

        # Both topics should have been represented in the retrieved messages
        assert high_volume_count > 0, "High volume topic should be represented"
        assert low_volume_count > 0, "Low volume topic should be represented (no starvation)"

        # Low volume topic should get fair representation despite being smaller
        # (not necessarily equal, but should not be zero)
        fairness_ratio = low_volume_count / (high_volume_count + low_volume_count)
        assert fairness_ratio > 0.05, f"Low volume topic got {fairness_ratio:.2%} which indicates starvation"


if __name__ == "__main__":
    pytest.main([__file__])
