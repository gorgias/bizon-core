import io
import json
import os
import struct
from unittest.mock import patch

import fastavro
import pytest
from avro.schema import parse

from bizon.connectors.sources.kafka.src.decode import (
    Hashabledict,
    decode_avro_message,
    parse_global_id_from_serialized_message,
)


@pytest.fixture
def test_data():
    # Load test data
    current_dir = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(current_dir, "schema.json")) as f:
        schema_dict = json.load(f)

    with open(os.path.join(current_dir, "data.json")) as f:
        data = json.load(f)

    # Create a hashable dictionary from the schema
    hashable_schema = Hashabledict(schema_dict)

    # Parse schema for Avro
    avro_schema = parse(json.dumps(schema_dict))

    # Create a serialized message for testing
    # 1-byte magic byte + 8-byte schema ID + Avro data
    schema_id = 12345

    # Create Avro binary data
    avro_binary = io.BytesIO()
    fastavro.schemaless_writer(avro_binary, avro_schema.to_json(), data)
    avro_data = avro_binary.getvalue()

    # Create mock serialized message with 8-byte schema ID
    magic_byte = struct.pack("b", 0)
    schema_id_bytes = struct.pack(">q", schema_id)
    serialized_message_8 = magic_byte + schema_id_bytes + avro_data

    # Create mock serialized message with 4-byte schema ID
    magic_byte = struct.pack("b", 0)
    schema_id_bytes_4 = struct.pack(">I", schema_id)
    serialized_message_4 = magic_byte + schema_id_bytes_4 + avro_data

    return {
        "schema_dict": schema_dict,
        "data": data,
        "hashable_schema": hashable_schema,
        "avro_schema": avro_schema,
        "schema_id": schema_id,
        "serialized_message_8": serialized_message_8,
        "serialized_message_4": serialized_message_4,
    }


def test_parse_global_id_8_bytes(test_data):
    """Test parsing global ID from message with 8-byte schema ID"""
    # Parse directly from the full message (Apicurio format with 0 as schema ID triggers 8-byte read)
    schema_id, nb_bytes = parse_global_id_from_serialized_message(test_data["serialized_message_8"][:9])
    assert nb_bytes == 8


def test_parse_global_id_4_bytes(test_data):
    """Test parsing global ID from message with 4-byte schema ID"""
    # Parse directly from the full message (Confluent format)
    schema_id, nb_bytes = parse_global_id_from_serialized_message(test_data["serialized_message_4"])
    assert schema_id == test_data["schema_id"]
    assert nb_bytes == 4


def test_parse_global_id_invalid_message():
    """Test parsing global ID with invalid message"""
    from confluent_kafka.serialization import SerializationError

    with pytest.raises(SerializationError):
        # Too short message
        parse_global_id_from_serialized_message(b"123")


@patch("bizon.connectors.sources.kafka.src.decode.fastavro.schemaless_reader")
def test_decode_avro_message(mock_reader, test_data):
    """Test decoding an Avro message"""
    # Set up the mock to return our test data
    mock_reader.return_value = test_data["data"]

    # Call the decode function with raw bytes
    result = decode_avro_message(test_data["serialized_message_8"], 8, test_data["avro_schema"])

    # Verify the result
    assert result == test_data["data"]

    # Verify that the reader was called correctly
    mock_reader.assert_called_once()
