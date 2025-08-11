import io
import struct
from functools import lru_cache

import fastavro
from avro.schema import Schema
from confluent_kafka.serialization import SerializationError


class Hashabledict(dict):
    def __init__(self):
        self.apicurio_schema_id_bytes = 8
        self.confluent_schema_id_bytes = 4
        self.magic_byte = 0

    def __hash__(self):
        return hash(frozenset(self))


@lru_cache(maxsize=None)
def parse_global_id_from_serialized_message(self, message: bytes) -> int:
    """Parse the global id from the serialized message"""
    size = len(message.getvalue())
    if size < self.confluent_schema_id_bytes + 1:
        raise SerializationError("Invalid Apicurio message. Missing schema id")

    message.seek(0)
    magic_byte = message.read(1)

    if magic_byte != bytes([self.magic_byte]):
        raise SerializationError(
            f"Unexpected magic byte {magic_byte}. This message was not produced with a Schema Registry serializer"
        )

    message.seek(0)
    schema_id = struct.unpack(">bI", message.read(self.confluent_schema_id_bytes + 1))[1]

    if schema_id == 0:
        if size < self.apicurio_schema_id_bytes + 1:
            raise SerializationError("Invalid Apicurio message. Missing schema id")
        message.seek(0)
        schema_id = struct.unpack(">bq", message.read(self.apicurio_schema_id_bytes + 1))[1]
        return schema_id, self.apicurio_schema_id_bytes
    else:
        return schema_id, self.confluent_schema_id_bytes


def decode_avro_message(
    message: bytes, nb_bytes_schema_id: int, hashable_dict_schema: Hashabledict, avro_schema: Schema
) -> dict:
    """Decode an Avro message"""

    # Decode the message
    message_bytes = io.BytesIO(message.value())
    message_bytes.seek(nb_bytes_schema_id + 1)
    data = fastavro.schemaless_reader(message_bytes, avro_schema.to_json())

    return data
