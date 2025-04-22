import io
import struct
from functools import lru_cache

import fastavro
import orjson
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


@lru_cache(maxsize=None)
def find_debezium_json_fields(hashable_schema: Hashabledict) -> list[str]:
    """Find the JSON fields in the Debezium schema"""

    json_fields = []

    for field in hashable_schema["fields"]:
        if field["name"] == "before":
            for before_types in field["type"]:
                if isinstance(before_types, dict) and before_types["type"] == "record":
                    debezium_columns = before_types["fields"]
                    for column in debezium_columns:
                        if (
                            isinstance(column.get("type"), dict)
                            and column.get("type").get("connect.name") == "io.debezium.data.Json"
                        ):
                            json_fields.append(column["name"])
    return json_fields


def parse_debezium_json_fields(data: dict, hashable_schema: Hashabledict) -> None:
    """Parse the JSON fields from the Debezium payload data in-place"""

    json_fields = find_debezium_json_fields(hashable_schema)

    for field in json_fields:
        for root_column in ["before", "after"]:
            if data.get(root_column) and data.get(root_column).get(field) is not None:
                # We JSON loads the field only if it's not empty, otherwise we return None
                data[root_column][field] = (
                    orjson.loads(data[root_column][field]) if len(data[root_column][field]) > 0 else None
                )


def decode_avro_message(
    message: bytes, nb_bytes_schema_id: int, hashable_dict_schema: Hashabledict, avro_schema: Schema
) -> dict:
    """Decode an Avro message"""

    # Decode the message
    message_bytes = io.BytesIO(message.value())
    message_bytes.seek(nb_bytes_schema_id + 1)
    data = fastavro.schemaless_reader(message_bytes, avro_schema.to_json())

    # Parse the JSON fields in-place
    parse_debezium_json_fields(data=data, hashable_schema=hashable_dict_schema)

    return data
