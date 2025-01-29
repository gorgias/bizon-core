from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from pydantic import Field
from typing import Literal, Mapping, Any
from enum import Enum


class SchemaRegistryType(str, Enum):
    APICURIO = "apicurio"


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
        "session.timeout.ms": 45000,
        "security.protocol": "SASL_SSL",
    }


class KafkaSourceConfig(SourceConfig):

    # Mandatory Kafka configuration
    topic: str = Field(..., description="Kafka topic")
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

    # Schema ID header configuration
    nb_bytes_schema_id: Literal[4, 8] = Field(
        description="Number of bytes encode SchemaID in Kafka message. Standard is 4.",
        default=4,
    )

    authentication: KafkaAuthConfig = Field(..., description="Authentication configuration")
