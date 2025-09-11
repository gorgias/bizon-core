from enum import Enum
from typing import Any, List, Literal, Mapping

from pydantic import BaseModel, Field

from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig


class SchemaRegistryType(str, Enum):
    APICURIO = "apicurio"


class MessageEncoding(str, Enum):
    UTF_8 = "utf-8"
    AVRO = "avro"


class KafkaAuthConfig(AuthConfig):

    type: Literal[AuthType.BASIC] = AuthType.BASIC  # username and password authentication

    # Schema registry authentication
    schema_registry_type: SchemaRegistryType = Field(
        default=SchemaRegistryType.APICURIO, description="Schema registry type"
    )

    schema_registry_url: str = Field(default="", description="Schema registry URL with the format ")
    schema_registry_username: str = Field(default="", description="Schema registry username")
    schema_registry_password: str = Field(default="", description="Schema registry password")


def default_kafka_consumer_config():
    return {
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Turn off auto-commit for manual offset handling
        "enable.auto.offset.store": False,  # Manual offset storage for precise control
        "session.timeout.ms": 45000,
        "security.protocol": "SASL_SSL",
        "partition.assignment.strategy": "cooperative-sticky",  # Fair partition distribution
        "fetch.min.bytes": 1,  # Return small batches to prevent starvation
        "fetch.wait.max.ms": 100,  # Don't wait too long for small topics
        "queued.max.messages.kbytes": 262144,  # Limit prefetch to prevent hot partition dominance
    }


class TopicConfig(BaseModel):
    name: str = Field(..., description="Kafka topic name")
    destination_id: str = Field(..., description="Destination id")


class KafkaSourceConfig(SourceConfig):

    # Mandatory Kafka configuration
    topics: List[TopicConfig] = Field(..., description="Kafka topic, comma separated")
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    group_id: str = Field(default="bizon", description="Kafka group id")

    skip_message_empty_value: bool = Field(
        default=True, description="Skip messages with empty value (tombstone messages)"
    )

    # Kafka consumer configuration
    batch_size: int = Field(100, description="Kafka batch size, number of messages to fetch at once.")
    consumer_timeout: int = Field(10, description="Kafka consumer timeout in seconds, before returning batch.")

    consumer_config: Mapping[str, Any] = Field(
        default_factory=default_kafka_consumer_config,
        description="Kafka consumer configuration, as described in the confluent-kafka-python documentation",
    )

    message_encoding: str = Field(default=MessageEncoding.AVRO, description="Encoding to use to decode the message")

    # Fair polling configuration
    enable_async_polling: bool = Field(
        default=True, description="Enable async polling to prevent starvation of low-volume topics"
    )
    poll_interval_ms: int = Field(default=50, description="Polling interval in milliseconds for the async poller")
    max_poll_records: int = Field(default=500, description="Maximum records to consume per poll to maintain fairness")
    partition_pause_threshold: int = Field(
        default=1000, description="Pause hot partitions after this many unprocessed messages to ensure fairness"
    )

    authentication: KafkaAuthConfig = Field(..., description="Authentication configuration")
