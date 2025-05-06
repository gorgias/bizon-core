import io
import struct
from functools import lru_cache

import fastavro
from avro.schema import Schema


class Hashabledict(dict):
    def __hash__(self):
        return hash(frozenset(self))


def get_header_bytes(nb_bytes_schema_id: int, message: bytes) -> bytes:
    """Get the header bytes from the message"""

    if nb_bytes_schema_id == 8:
        return message[:9]

    elif nb_bytes_schema_id == 4:
        return message[1:5]

    else:
        raise ValueError(f"Number of bytes for schema id {nb_bytes_schema_id} not supported")


@lru_cache(maxsize=None)
def parse_global_id_from_serialized_message(nb_bytes_schema_id: int, header_message_bytes: bytes) -> int:
    """Parse the global id from the serialized message"""

    if nb_bytes_schema_id == 8:
        return struct.unpack(">bq", header_message_bytes)[1]

    elif nb_bytes_schema_id == 4:
        return struct.unpack(">I", header_message_bytes)[0]

    raise ValueError(f"Number of bytes for schema id {nb_bytes_schema_id} not supported")


def decode_avro_message(
    message: bytes, nb_bytes_schema_id: int, hashable_dict_schema: Hashabledict, avro_schema: Schema
) -> dict:
    """Decode an Avro message"""

    # Decode the message
    message_bytes = io.BytesIO(message.value())
    message_bytes.seek(nb_bytes_schema_id + 1)
    data = fastavro.schemaless_reader(message_bytes, avro_schema.to_json())

    return data
