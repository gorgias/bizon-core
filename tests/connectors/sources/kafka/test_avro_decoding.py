import io
import json
import os
import struct
from unittest.mock import MagicMock, patch

import fastavro
import pytest
from avro.schema import parse

from bizon.connectors.sources.kafka.src.decode import (
    Hashabledict,
    decode_avro_message,
    find_debezium_json_fields,
    get_header_bytes,
    parse_debezium_json_fields,
    parse_global_id_from_serialized_message,
)


@pytest.fixture
def test_data():
    # Load test data
    current_dir = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(current_dir, "schema.json"), "r") as f:
        schema_dict = json.load(f)

    with open(os.path.join(current_dir, "data.json"), "r") as f:
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


def test_get_header_bytes_8(test_data):
    """Test getting header bytes from a message with 8-byte schema ID"""
    header_bytes = get_header_bytes(8, test_data["serialized_message_8"])

    # Should return the first 9 bytes (1 magic byte + 8 schema ID bytes)
    assert len(header_bytes) == 9
    assert header_bytes == test_data["serialized_message_8"][:9]


def test_get_header_bytes_4(test_data):
    """Test getting header bytes from a message with 4-byte schema ID"""
    header_bytes = get_header_bytes(4, test_data["serialized_message_4"])

    # Should return bytes 1-5 (the 4-byte schema ID after magic byte)
    assert len(header_bytes) == 4
    assert header_bytes == test_data["serialized_message_4"][1:5]


def test_get_header_bytes_unsupported(test_data):
    """Test getting header bytes with unsupported schema ID byte length"""
    with pytest.raises(ValueError):
        get_header_bytes(6, test_data["serialized_message_8"])


def test_parse_global_id_8_bytes(test_data):
    """Test parsing global ID from message with 8-byte schema ID"""
    header_bytes = get_header_bytes(8, test_data["serialized_message_8"])
    global_id = parse_global_id_from_serialized_message(8, header_bytes)
    assert global_id == test_data["schema_id"]


def test_parse_global_id_4_bytes(test_data):
    """Test parsing global ID from message with 4-byte schema ID"""
    header_bytes = get_header_bytes(4, test_data["serialized_message_4"])
    global_id = parse_global_id_from_serialized_message(4, header_bytes)
    assert global_id == test_data["schema_id"]


def test_parse_global_id_unsupported():
    """Test parsing global ID with unsupported schema ID byte length"""
    with pytest.raises(ValueError):
        parse_global_id_from_serialized_message(6, b"123456")


def test_find_debezium_json_fields(test_data):
    """Test finding JSON fields in Debezium schema"""
    json_fields = find_debezium_json_fields(test_data["hashable_schema"])

    # Based on the provided schema, we expect "children" to be a JSON field
    assert "children" in json_fields


def test_parse_debezium_json_fields(test_data):
    """Test parsing JSON fields from Debezium data"""
    # Create a copy of data to test modification in-place
    data_copy = test_data["data"].copy()

    # Before parsing, the "children" field should be a string
    assert isinstance(data_copy["after"]["children"], str)

    # Parse the JSON fields
    parse_debezium_json_fields(data_copy, test_data["hashable_schema"])

    # After parsing, the "children" field should be parsed into a list/dict
    assert isinstance(data_copy["after"]["children"], list)


def test_parse_debezium_json_fields_with_emtpy_string(test_data):
    """Test parsing JSON fields from Debezium data with a null value"""
    # Create a copy of data to test modification in-place
    data_copy = test_data["data"].copy()

    # Set the "children" field to an empty string
    data_copy["after"]["children"] = ""

    # Before parsing, the "children" field should be a string
    assert isinstance(data_copy["after"]["children"], str)

    # Parse the JSON fields
    parse_debezium_json_fields(data_copy, test_data["hashable_schema"])

    # After parsing, the "children" field should be parsed as None
    assert data_copy["after"]["children"] == None


@patch("bizon.connectors.sources.kafka.src.decode.fastavro.schemaless_reader")
@patch("bizon.connectors.sources.kafka.src.decode.parse_debezium_json_fields")
def test_decode_avro_message(mock_parse_json, mock_reader, test_data):
    """Test decoding an Avro message"""
    # Set up the mock to return our test data
    mock_reader.return_value = test_data["data"]

    # Create a mock message object
    mock_message = MagicMock()
    mock_message.value.return_value = test_data["serialized_message_8"]

    # Call the decode function
    result = decode_avro_message(mock_message, 8, test_data["hashable_schema"], test_data["avro_schema"])

    # Verify the result
    assert result == test_data["data"]

    # Verify that the JSON fields were parsed
    mock_parse_json.assert_called_once_with(data=test_data["data"], hashable_schema=test_data["hashable_schema"])

    # Verify that the reader was called correctly
    mock_reader.assert_called_once()
