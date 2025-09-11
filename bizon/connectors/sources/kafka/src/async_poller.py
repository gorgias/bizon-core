import queue
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.cimpl import KafkaException as CimplKafkaException
from loguru import logger

from .config import KafkaSourceConfig


class PartitionMessageQueue:
    def __init__(self, max_size: int = 1000):
        self.messages: queue.Queue[Message] = queue.Queue(maxsize=max_size)
        self.paused = False
        self.last_processed_offset = -1
        self.inflight_offsets: Set[int] = set()
        self.completed_offsets: Set[int] = set()

    def put(self, message: Message) -> bool:
        if self.paused:
            return False
        try:
            self.messages.put_nowait(message)
            return True
        except queue.Full:
            return False

    def get(self, timeout: Optional[float] = None) -> Optional[Message]:
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def size(self) -> int:
        return self.messages.qsize()

    def mark_inflight(self, offset: int):
        self.inflight_offsets.add(offset)

    def mark_completed(self, offset: int):
        self.inflight_offsets.discard(offset)
        self.completed_offsets.add(offset)

    def get_safe_commit_offset(self) -> int:
        # Find the highest contiguous completed offset
        next_expected = self.last_processed_offset + 1
        while next_expected in self.completed_offsets:
            self.completed_offsets.remove(next_expected)
            self.last_processed_offset = next_expected
            next_expected += 1

        # Return the offset to commit (next offset after the highest processed)
        return self.last_processed_offset + 1


class AsyncKafkaPoller:
    def __init__(self, consumer: Consumer, config: KafkaSourceConfig):
        self.consumer = consumer
        self.config = config
        self.running = False
        self.poll_thread: Optional[threading.Thread] = None

        # Per-partition message queues - key: (topic, partition)
        self.partition_queues: Dict[Tuple[str, int], PartitionMessageQueue] = {}
        self.lock = threading.RLock()

        # Partition management
        self.assigned_partitions: Set[TopicPartition] = set()
        self.paused_partitions: Set[TopicPartition] = set()

        # Metrics
        self.poll_count = 0
        self.last_poll_time = time.time()

    def start(self):
        if self.running:
            return

        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.poll_thread.start()
        logger.info("AsyncKafkaPoller started")

    def stop(self):
        self.running = False
        if self.poll_thread:
            self.poll_thread.join(timeout=5.0)
        logger.info("AsyncKafkaPoller stopped")

    def _poll_loop(self):
        poll_timeout = self.config.poll_interval_ms / 1000.0

        while self.running:
            try:
                # Poll for messages with short timeout to maintain fairness
                message = self.consumer.poll(timeout=poll_timeout)

                if message is None:
                    continue

                if message.error():
                    logger.error(f"Kafka error in poll loop: {message.error()}")
                    continue

                self._handle_message(message)
                self.poll_count += 1

                # Check if we need to rebalance partition fairness
                if self.poll_count % 100 == 0:
                    self._rebalance_partitions()

            except CimplKafkaException as e:
                logger.error(f"Kafka exception in poll loop: {e}")
                time.sleep(1.0)  # Back off on errors
            except Exception as e:
                logger.error(f"Unexpected error in poll loop: {e}")
                time.sleep(1.0)

    def _handle_message(self, message: Message):
        partition_key = (message.topic(), message.partition())

        with self.lock:
            # Create partition queue if it doesn't exist
            if partition_key not in self.partition_queues:
                self.partition_queues[partition_key] = PartitionMessageQueue(
                    max_size=self.config.partition_pause_threshold
                )

            partition_queue = self.partition_queues[partition_key]

            # Try to add message to partition queue
            if not partition_queue.put(message):
                # Queue is full, pause this partition temporarily
                topic_partition = TopicPartition(message.topic(), message.partition())
                self._pause_partition(topic_partition)
                logger.warning(
                    f"Partition {message.topic()}:{message.partition()} queue full, "
                    f"pausing to prevent memory issues"
                )

    def _pause_partition(self, topic_partition: TopicPartition):
        if topic_partition not in self.paused_partitions:
            self.paused_partitions.add(topic_partition)
            try:
                self.consumer.pause([topic_partition])
                logger.debug(f"Paused partition {topic_partition.topic}:{topic_partition.partition}")
            except Exception as e:
                logger.error(f"Failed to pause partition {topic_partition}: {e}")

    def _resume_partition(self, topic_partition: TopicPartition):
        if topic_partition in self.paused_partitions:
            self.paused_partitions.remove(topic_partition)
            try:
                self.consumer.resume([topic_partition])
                logger.debug(f"Resumed partition {topic_partition.topic}:{topic_partition.partition}")
            except Exception as e:
                logger.error(f"Failed to resume partition {topic_partition}: {e}")

    def _rebalance_partitions(self):
        with self.lock:
            for partition_key, partition_queue in self.partition_queues.items():
                topic, partition = partition_key
                topic_partition = TopicPartition(topic, partition)

                # Resume partitions that have drained enough
                if (
                    topic_partition in self.paused_partitions
                    and partition_queue.size() < self.config.partition_pause_threshold // 2
                ):
                    self._resume_partition(topic_partition)
                    partition_queue.paused = False

    def get_messages(self, max_messages: int = None) -> List[Message]:
        if max_messages is None:
            max_messages = self.config.max_poll_records

        messages = []
        collected = 0

        with self.lock:
            # Round-robin through all partitions to ensure fairness
            partition_keys = list(self.partition_queues.keys())

            while collected < max_messages and partition_keys:
                for partition_key in list(partition_keys):
                    if collected >= max_messages:
                        break

                    partition_queue = self.partition_queues[partition_key]
                    message = partition_queue.get(timeout=0.001)  # Very short timeout

                    if message:
                        messages.append(message)
                        # Mark as inflight - NOT completed yet!
                        # Will be marked completed only after destination write succeeds
                        partition_queue.mark_inflight(message.offset())
                        collected += 1
                    else:
                        # Remove empty queues from round-robin
                        partition_keys.remove(partition_key)

        return messages

    def mark_messages_completed(self, messages: List[Message]):
        """Mark messages as completed after successful destination write"""
        with self.lock:
            for message in messages:
                partition_key = (message.topic(), message.partition())
                if partition_key in self.partition_queues:
                    partition_queue = self.partition_queues[partition_key]
                    partition_queue.mark_completed(message.offset())

    def get_commit_offsets(self) -> List[TopicPartition]:
        commit_offsets = []

        with self.lock:
            for partition_key, partition_queue in self.partition_queues.items():
                topic, partition = partition_key
                old_processed_offset = partition_queue.last_processed_offset
                safe_offset = partition_queue.get_safe_commit_offset()

                # Only commit if we have progressed beyond what was already committed
                if safe_offset > old_processed_offset + 1:
                    commit_offsets.append(TopicPartition(topic, partition, safe_offset))

        return commit_offsets

    def on_assign(self, consumer, partitions):
        logger.info(f"Partitions assigned: {[(p.topic, p.partition) for p in partitions]}")
        with self.lock:
            self.assigned_partitions = set(partitions)
            # Initialize queues for new partitions
            for partition in partitions:
                partition_key = (partition.topic, partition.partition)
                if partition_key not in self.partition_queues:
                    self.partition_queues[partition_key] = PartitionMessageQueue(
                        max_size=self.config.partition_pause_threshold
                    )

    def on_revoke(self, consumer, partitions):
        logger.info(f"Partitions revoked: {[(p.topic, p.partition) for p in partitions]}")

        # Get final commit offsets before revocation
        commit_offsets = self.get_commit_offsets()

        if commit_offsets:
            try:
                consumer.commit(offsets=commit_offsets, asynchronous=False)
                logger.info(f"Committed {len(commit_offsets)} offsets before revocation")
            except Exception as e:
                logger.error(f"Failed to commit offsets during revocation: {e}")

        with self.lock:
            # Clean up revoked partitions
            for partition in partitions:
                partition_key = (partition.topic, partition.partition)
                if partition_key in self.partition_queues:
                    # Don't delete the queue as it might have unprocessed messages
                    # Just mark the partition as no longer assigned
                    pass

            self.assigned_partitions.difference_update(partitions)
            self.paused_partitions.difference_update(partitions)

    def get_stats(self) -> Dict:
        with self.lock:
            queue_sizes = {
                f"{topic}:{partition}": queue.size() for (topic, partition), queue in self.partition_queues.items()
            }

            return {
                "poll_count": self.poll_count,
                "assigned_partitions": len(self.assigned_partitions),
                "paused_partitions": len(self.paused_partitions),
                "queue_sizes": queue_sizes,
                "total_queued": sum(queue_sizes.values()),
                "polls_per_second": self.poll_count / max(1, time.time() - self.last_poll_time),
            }
