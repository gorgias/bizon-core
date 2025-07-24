import pytest
from google.cloud.bigquery import SchemaField
from google.protobuf.json_format import ParseError
from google.protobuf.message import EncodeError

from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import (
    BigQueryStreamingV2Destination,
)
from bizon.connectors.destinations.bigquery_streaming_v2.src.proto_utils import (
    get_proto_schema_and_class,
)


def test_get_proto_schema_and_class():
    bq_schema = [
        SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
        SchemaField(name="age", field_type="INTEGER", mode="REQUIRED"),
    ]
    proto_schema, table_row_class = get_proto_schema_and_class(bq_schema)
    assert proto_schema is not None


def test_to_protobuf_serialization():
    # Test to_protobuf_serialization
    bq_schema = [
        SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
        SchemaField(name="age", field_type="INTEGER", mode="REQUIRED"),
    ]

    proto_schema, table_row_class = get_proto_schema_and_class(bq_schema)

    data = {
        "name": "John",
        "age": 30,
    }

    serialized_record = BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, data)

    assert serialized_record is not None


def test_to_protobuf_serialization_error_mismatch_schema():
    # Test to_protobuf_serialization
    bq_schema = [
        SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
        SchemaField(name="age", field_type="INTEGER", mode="REQUIRED"),
        SchemaField(name="email", field_type="STRING", mode="REQUIRED"),
    ]

    proto_schema, table_row_class = get_proto_schema_and_class(bq_schema)

    data = {
        "name": "John",
        "age": 30,
    }

    with pytest.raises(EncodeError):
        BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, data)


def test_to_protobuf_serialization_error_mismatch_schema_parse_error():
    # Test to_protobuf_serialization
    bq_schema = [
        SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
        SchemaField(name="email", field_type="STRING", mode="REQUIRED"),
    ]

    proto_schema, table_row_class = get_proto_schema_and_class(bq_schema)

    data = {
        "not_in_schema": "John",
        "name": "John",
        "age": 30,
    }

    with pytest.raises(ParseError):
        BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, data)
