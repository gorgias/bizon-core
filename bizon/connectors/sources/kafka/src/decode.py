import io
import struct
from functools import cache
from typing import Tuple, Union

import fastavro
from avro.schema import Schema
from confluent_kafka.serialization import SerializationError

# Constants for schema ID byte sizes
APICURIO_SCHEMA_ID_BYTES = 8
CONFLUENT_SCHEMA_ID_BYTES = 4
MAGIC_BYTE = 0


class Hashabledict(dict):
    """A hashable dictionary for caching purposes"""

    def __hash__(self):
        return hash(frozenset(self.items()))


@cache
def parse_global_id_from_serialized_message(message: bytes) -> Tuple[int, int]:
    """
    Parse the global id from the serialized message.

    Args:
        message: The serialized message bytes

    Returns:
        Tuple of (schema_id, number_of_bytes_used_for_schema_id)

    Raises:
        SerializationError: If the message is invalid or missing schema id
    """
    size = len(message)

    if size < CONFLUENT_SCHEMA_ID_BYTES + 1:
        raise SerializationError("Invalid message. Missing schema id")

    # Create BytesIO object for easier reading
    message_buffer = io.BytesIO(message)
    message_buffer.seek(0)
    magic_byte = message_buffer.read(1)

    if magic_byte != bytes([MAGIC_BYTE]):
        raise SerializationError(
            f"Unexpected magic byte {magic_byte}. This message was not produced with a Schema Registry serializer"
        )

    # Read Confluent schema ID (4 bytes + 1 magic byte)
    message_buffer.seek(0)
    schema_id = struct.unpack(">bI", message_buffer.read(CONFLUENT_SCHEMA_ID_BYTES + 1))[1]

    # If schema_id is 0, try reading as Apicurio format (8 bytes)
    if schema_id == 0:
        if size < APICURIO_SCHEMA_ID_BYTES + 1:
            raise SerializationError("Invalid Apicurio message. Missing schema id")
        message_buffer.seek(0)
        schema_id = struct.unpack(">bq", message_buffer.read(APICURIO_SCHEMA_ID_BYTES + 1))[1]
        return schema_id, APICURIO_SCHEMA_ID_BYTES
    else:
        return schema_id, CONFLUENT_SCHEMA_ID_BYTES


def decode_avro_message(message_value: bytes, nb_bytes_schema_id: int, avro_schema: Union[Schema, dict]) -> dict:
    """
    Decode an Avro message.

    Args:
        message_value: The raw message bytes
        nb_bytes_schema_id: Number of bytes used for schema ID
        avro_schema: The Avro schema (as Schema object or dict)

    Returns:
        Decoded message as a dictionary
    """
    # Create BytesIO from message bytes
    message_bytes = io.BytesIO(message_value)

    # Skip magic byte and schema ID bytes
    message_bytes.seek(nb_bytes_schema_id + 1)

    # Decode the message using fastavro
    if isinstance(avro_schema, Schema):
        schema_dict = avro_schema.to_json()
    else:
        schema_dict = avro_schema

    data = fastavro.schemaless_reader(message_bytes, schema_dict)

    return data
