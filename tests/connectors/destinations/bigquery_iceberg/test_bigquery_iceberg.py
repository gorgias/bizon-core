"""Tests for BigQuery Iceberg destination connector."""

import os
from datetime import datetime
from unittest.mock import Mock, patch

import polars as pl
import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery_iceberg.src.config import (
    BigQueryIcebergConfigDetails,
    DestinationTableConfig,
    IcebergFieldConfig,
    IcebergTableFormat,
    TimePartitionConfig,
    TimePartitionType,
)
from bizon.connectors.destinations.bigquery_iceberg.src.destination import (
    BigQueryIcebergDestination,
)

# Constant for the destination module path to avoid long lines
DEST_PATH = "bizon.connectors.destinations.bigquery_iceberg.src.destination"


class TestBigQueryIcebergConfig:
    """Test configuration classes."""

    def test_config_creation(self):
        """Test that config can be created with required fields."""
        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
        )
        assert config.project_id == "test-project"
        assert config.dataset_id == "test_dataset"
        assert config.gcs_warehouse_bucket == "test-bucket"
        assert config.gcs_warehouse_path == "iceberg"  # default value
        assert config.table_format == IcebergTableFormat.PARQUET  # default value
        assert config.dataset_location == "US"  # default value
        assert config.biglake_connection_id == "us.biglake-connection"  # default value

        # Check default catalog config
        assert "default" in config.catalog_config
        assert config.catalog_config["default"]["type"] == "sql"
        assert "postgresql+psycopg2://" in config.catalog_config["default"]["uri"]
        assert config.catalog_config["default"]["init_catalog_tables"] is False

    def test_config_with_custom_values(self):
        """Test config with custom values."""
        custom_catalog_config = {
            "my_catalog": {"type": "rest", "uri": "http://localhost:8181", "warehouse": "gs://my-bucket/warehouse"}
        }

        config = BigQueryIcebergConfigDetails(
            project_id="custom-project",
            dataset_id="custom_dataset",
            gcs_warehouse_bucket="custom-bucket",
            gcs_warehouse_path="custom/path",
            table_format=IcebergTableFormat.ORC,
            dataset_location="EU",
            biglake_connection_id="eu.custom-connection",
            target_file_size_mb=256,
            write_batch_size=2000,
            catalog_config=custom_catalog_config,
        )

        assert config.gcs_warehouse_path == "custom/path"
        assert config.table_format == IcebergTableFormat.ORC
        assert config.dataset_location == "EU"
        assert config.biglake_connection_id == "eu.custom-connection"
        assert config.target_file_size_mb == 256
        assert config.write_batch_size == 2000

        # Check custom catalog config
        assert "my_catalog" in config.catalog_config
        assert config.catalog_config["my_catalog"]["type"] == "rest"
        assert config.catalog_config["my_catalog"]["uri"] == "http://localhost:8181"

    def test_config_with_destination_table_config(self):
        """Test configuration with destination_table_config."""
        destination_configs = [
            DestinationTableConfig(
                destination_id="orders",
                clustering_keys=["customer_id", "region"],
                iceberg_schema={
                    "order_id": IcebergFieldConfig(target_field="order_id", iceberg_type="string"),
                    "customer_id": IcebergFieldConfig(target_field="customer_id", iceberg_type="long"),
                    "amount": IcebergFieldConfig(target_field="amount", iceberg_type="double"),
                },
            ),
            DestinationTableConfig(destination_id="customers", clustering_keys=["country", "segment"]),
        ]

        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=destination_configs,
        )

        assert len(config.destination_table_config) == 2
        assert config.destination_table_config[0].destination_id == "orders"
        assert config.destination_table_config[0].clustering_keys == ["customer_id", "region"]

        # Check iceberg_schema for orders
        orders_schema = config.destination_table_config[0].iceberg_schema
        assert "order_id" in orders_schema
        assert orders_schema["order_id"].target_field == "order_id"
        assert orders_schema["order_id"].iceberg_type == "string"
        assert orders_schema["customer_id"].iceberg_type == "long"
        assert orders_schema["amount"].iceberg_type == "double"

        assert config.destination_table_config[1].destination_id == "customers"
        assert config.destination_table_config[1].clustering_keys == ["country", "segment"]

    def test_config_with_single_destination_table_config(self):
        """Test configuration with single destination table config."""
        destination_config = DestinationTableConfig(destination_id="events", clustering_keys=["event_type", "user_id"])

        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        assert len(config.destination_table_config) == 1
        assert config.destination_table_config[0].destination_id == "events"
        assert config.destination_table_config[0].clustering_keys == ["event_type", "user_id"]

    def test_config_with_partition_fields(self):
        """Test configuration with partition fields."""
        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            partition_fields=["region"],
        )

        assert config.partition_fields == ["region"]

    def test_config_with_time_partitioning(self):
        """Test configuration with time partitioning."""
        time_partition = TimePartitionConfig(field="created_at", type=TimePartitionType.DAY)
        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            time_partitioning=[time_partition],
        )

        assert len(config.time_partitioning) == 1
        assert config.time_partitioning[0].field == "created_at"
        assert config.time_partitioning[0].type == TimePartitionType.DAY

    def test_config_with_combined_partitioning(self):
        """Test configuration with both time and identity partitioning."""
        time_partition = TimePartitionConfig(field="created_at", type=TimePartitionType.HOUR)
        config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            time_partitioning=[time_partition],
            partition_fields=["region", "domain"],
        )

        assert len(config.time_partitioning) == 1
        assert config.time_partitioning[0].field == "created_at"
        assert config.time_partitioning[0].type == TimePartitionType.HOUR
        assert config.partition_fields == ["region", "domain"]

    def test_iceberg_namespace_config(self):
        """Test iceberg_namespace configuration parameter."""
        # Test default namespace
        config = BigQueryIcebergConfigDetails(
            project_id="test-project", dataset_id="test_dataset", gcs_warehouse_bucket="test-bucket"
        )
        assert config.iceberg_namespace == "bizon"  # default value

        # Test custom namespace
        config_custom = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            iceberg_namespace="analytics",
        )
        assert config_custom.iceberg_namespace == "analytics"


class TestBigQueryIcebergDestination:
    """Test the BigQueryIcebergDestination class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
        )

        self.sync_metadata = Mock(spec=SyncMetadata)
        self.sync_metadata.source_name = "test_source"
        self.sync_metadata.stream_name = "test_stream"
        self.sync_metadata.sync_mode = "FULL_REFRESH"

        self.backend = Mock()
        self.source_callback = Mock()
        self.monitor = Mock()

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_destination_initialization(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test destination initialization."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        assert destination.project_id == "test-project"
        assert destination.dataset_id == "test_dataset"
        assert destination.dataset_location == "US"
        assert destination.destination_id is None  # No destination_table_config

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_destination_with_custom_config(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test destination with custom configuration."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        # Add custom destination config
        destination_config = DestinationTableConfig(
            destination_id="custom_events",
            clustering_keys=["user_id", "event_type"],
            iceberg_schema={
                "user_id": IcebergFieldConfig(target_field="user_id", iceberg_type="long"),
                "event_type": IcebergFieldConfig(target_field="event_type", iceberg_type="string"),
            },
        )

        config_with_custom = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
            iceberg_namespace="custom_analytics",
        )

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_custom,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        assert destination.destination_id == "custom_events"
        assert destination.config.iceberg_namespace == "custom_analytics"
        assert "custom_events" in destination.table_id
        assert "custom_analytics.custom_events" in destination.iceberg_table_name

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_get_clustering_keys_for_destination(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test clustering keys retrieval."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination_config = DestinationTableConfig(
            destination_id="events",
            clustering_keys=["user_id", "event_type"],
        )

        config_with_clustering = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_clustering,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        clustering_keys = destination.get_clustering_keys_for_destination()
        assert clustering_keys == ["user_id", "event_type"]

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_get_iceberg_schema_for_destination(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test Iceberg schema mapping retrieval."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination_config = DestinationTableConfig(
            destination_id="events",
            clustering_keys=["user_id"],
            iceberg_schema={
                "user_id": IcebergFieldConfig(target_field="user_id", iceberg_type="long"),
                "event_type": IcebergFieldConfig(target_field="event_type", iceberg_type="string"),
            },
        )

        config_with_schema = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_schema,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        schema_mapping = destination.get_iceberg_schema_for_destination()
        assert schema_mapping["user_id"] == "user_id"
        assert schema_mapping["event_type"] == "event_type"

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_get_mapped_field_name(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test field name mapping."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination_config = DestinationTableConfig(
            destination_id="events",
            iceberg_schema={
                "user_id": IcebergFieldConfig(target_field="user_id", iceberg_type="long"),
                "event_type": IcebergFieldConfig(target_field="event_type", iceberg_type="string"),
            },
        )

        config_with_mapping = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_mapping,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test custom mapping
        assert destination.get_mapped_field_name("user_id") == "user_id"
        assert destination.get_mapped_field_name("event_type") == "event_type"

        # Test default Bizon mapping
        assert destination.get_mapped_field_name("bizon_extracted_at") == "_bizon_extracted_at"
        assert destination.get_mapped_field_name("bizon_id") == "_bizon_id"

        # Test unmapped fields
        assert destination.get_mapped_field_name("unknown_field") == "unknown_field"

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_table_id_generation(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test table ID generation logic."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        # Test without destination_id
        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        expected_table_id = "test-project.test_dataset.test_source_test_stream"
        assert destination.table_id == expected_table_id

        # Test with destination_id
        destination_config = DestinationTableConfig(destination_id="custom_table")
        config_with_dest = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        destination_with_dest = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_dest,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        expected_custom_table_id = "test-project.test_dataset.custom_table"
        assert destination_with_dest.table_id == expected_custom_table_id

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_iceberg_table_name_generation(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test Iceberg table name generation."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        # Test without destination_id
        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        expected_iceberg_name = "bizon.test_source_test_stream"
        assert destination.iceberg_table_name == expected_iceberg_name

        # Test with custom namespace and destination_id
        destination_config = DestinationTableConfig(destination_id="custom_table")
        config_custom = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
            iceberg_namespace="analytics",
        )

        destination_custom = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_custom,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        expected_custom_iceberg_name = "analytics.custom_table"
        assert destination_custom.iceberg_table_name == expected_custom_iceberg_name

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_check_connection(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test connection checking."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        # Mock BigQuery client
        mock_bq_instance = Mock()
        mock_bq_client.return_value = mock_bq_instance

        # Test existing dataset
        mock_bq_instance.get_dataset.return_value = Mock()

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        result = destination.check_connection()
        assert result is True
        mock_bq_instance.get_dataset.assert_called_once()

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_finalize(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test finalize method."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test different sync modes
        for sync_mode in ["FULL_REFRESH", "INCREMENTAL", "STREAM"]:
            self.sync_metadata.sync_mode = sync_mode
            result = destination.finalize()
            assert result is True

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_flatten_nested_structures(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test the _flatten_nested_structures method."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test with nested struct columns
        test_df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "nested_data": [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
            }
        )

        # Convert to struct type to simulate nested data
        test_df = test_df.with_columns([pl.col("nested_data").cast(pl.Struct({"key1": pl.String, "key2": pl.String}))])

        result_df = destination._flatten_nested_structures(test_df)

        # Verify that nested struct was converted to string
        assert result_df["nested_data"].dtype == pl.Utf8
        assert "key1" in str(result_df["nested_data"][0])
        assert "key2" in str(result_df["nested_data"][0])

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_group_records_by_destination(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test the _group_records_by_destination method."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test with destination_id column
        test_df = pl.DataFrame({"destination_id": ["dest1", "dest1", "dest2"], "data": ["value1", "value2", "value3"]})

        result = destination._group_records_by_destination(test_df)

        assert "dest1" in result
        assert "dest2" in result
        assert len(result["dest1"]) == 2
        assert len(result["dest2"]) == 1

        # Test without destination_id column (single destination)
        test_df_no_dest = pl.DataFrame({"data": ["value1", "value2"]})

        result_no_dest = destination._group_records_by_destination(test_df_no_dest)

        assert "default" in result_no_dest
        assert len(result_no_dest["default"]) == 2

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_create_iceberg_schema_from_polars(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test the _create_iceberg_schema_from_polars method."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=self.config,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test with basic data types
        test_df = pl.DataFrame(
            {
                "string_col": ["test"],
                "int_col": [1],
                "float_col": [1.5],
                "bool_col": [True],
                "datetime_col": [datetime.now()],
            }
        )

        schema = destination._create_iceberg_schema_from_polars(test_df)

        # Verify schema was created
        assert schema is not None
        assert len(schema.fields) == 5

        # Verify field names
        field_names = [field.name for field in schema.fields]
        assert "string_col" in field_names
        assert "int_col" in field_names
        assert "float_col" in field_names
        assert "bool_col" in field_names
        assert "datetime_col" in field_names

    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.bigquery.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.storage.Client")
    @patch("bizon.connectors.destinations.bigquery_iceberg.src.destination.load_catalog")
    def test_create_iceberg_schema_with_custom_mapping(self, mock_load_catalog, mock_storage_client, mock_bq_client):
        """Test schema creation with custom field mapping."""
        mock_catalog = Mock()
        mock_load_catalog.return_value = mock_catalog

        # Create config with custom schema
        destination_config = DestinationTableConfig(
            destination_id="custom_events",
            iceberg_schema={
                "user_id": IcebergFieldConfig(target_field="user_id", iceberg_type="long"),
                "event_type": IcebergFieldConfig(target_field="event_type", iceberg_type="string"),
                "timestamp": IcebergFieldConfig(target_field="timestamp", iceberg_type="timestamp"),
            },
        )

        config_with_schema = BigQueryIcebergConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_warehouse_bucket="test-bucket",
            destination_table_config=[destination_config],
        )

        destination = BigQueryIcebergDestination(
            sync_metadata=self.sync_metadata,
            config=config_with_schema,
            backend=self.backend,
            source_callback=self.source_callback,
            monitor=self.monitor,
        )

        # Test with data that matches custom schema
        test_df = pl.DataFrame(
            {
                "user_id": [123],
                "event_type": ["click"],
                "timestamp": [datetime.now()],
                "other_field": ["value"],  # This should use default mapping
            }
        )

        schema = destination._create_iceberg_schema_from_polars(test_df)

        # Verify schema was created
        assert schema is not None
        assert len(schema.fields) == 4

        # Verify field names
        field_names = [field.name for field in schema.fields]
        assert "user_id" in field_names
        assert "event_type" in field_names
        assert "timestamp" in field_names
        assert "other_field" in field_names


if __name__ == "__main__":
    pytest.main([__file__])
