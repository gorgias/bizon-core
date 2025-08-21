from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
)


class IcebergTableFormat(str, Enum):
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"


class TimePartitionType(str, Enum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"


class TimePartitionConfig(BaseModel):
    field: str = Field(..., description="Field name to partition by time")
    type: TimePartitionType = Field(..., description="Time partition granularity")


class IcebergFieldConfig(BaseModel):
    # New simplified format
    name: Optional[str] = Field(default=None, description="Field name in Iceberg table")
    type: Optional[str] = Field(default=None, description="Iceberg data type (string, long, timestamp, etc.)")

    # Legacy format for backward compatibility
    target_field: Optional[str] = Field(default=None, description="Legacy: Target field name (use 'name' instead)")
    iceberg_type: Optional[str] = Field(default=None, description="Legacy: Iceberg data type (use 'type' instead)")

    @property
    def field_name(self) -> str:
        """Get the field name, supporting both new and legacy formats"""
        return self.name if self.name else (self.target_field or "")

    @property
    def field_type(self) -> str:
        """Get the field type, supporting both new and legacy formats"""
        return self.type if self.type else (self.iceberg_type or "string")

    def __init__(self, **data):
        super().__init__(**data)
        # Ensure at least one format is provided
        if not (self.name or self.target_field):
            raise ValueError("Either 'name' or 'target_field' must be provided")
        if not (self.type or self.iceberg_type):
            raise ValueError("Either 'type' or 'iceberg_type' must be provided")


class UpsertConfig(BaseModel):
    enabled: bool = Field(default=False, description="Enable upsert operation instead of append")
    join_cols: Optional[List[str]] = Field(
        default=None, description="Columns to join on for upsert. If not provided, uses table's identifier fields"
    )
    when_matched_update_all: bool = Field(default=True, description="Update matched rows during upsert")
    when_not_matched_insert_all: bool = Field(default=True, description="Insert new rows during upsert")
    case_sensitive: bool = Field(default=True, description="Whether matching is case-sensitive")


class DestinationTableConfig(BaseModel):
    destination_id: str = Field(..., description="Destination table identifier")
    # New list-based format (preferred)
    iceberg_schema: Optional[Union[List[IcebergFieldConfig], Dict[str, IcebergFieldConfig]]] = Field(
        default=None,
        description="Field configuration for Iceberg schema. Can be a list of field configs or dict (legacy)",
    )
    upsert: Optional[UpsertConfig] = Field(default=None, description="Upsert configuration for merge operations")

    def get_iceberg_schema_dict(self) -> Dict[str, IcebergFieldConfig]:
        """Convert iceberg_schema to dictionary format for internal use"""
        if self.iceberg_schema is None:
            # Return default Bizon field mapping
            return {
                "bizon_extracted_at": IcebergFieldConfig(target_field="_bizon_extracted_at", iceberg_type="timestamp"),
                "bizon_id": IcebergFieldConfig(target_field="_bizon_id", iceberg_type="string"),
                "bizon_loaded_at": IcebergFieldConfig(target_field="_bizon_loaded_at", iceberg_type="timestamp"),
                "source_record_id": IcebergFieldConfig(target_field="_source_record_id", iceberg_type="string"),
                "source_timestamp": IcebergFieldConfig(target_field="_source_timestamp", iceberg_type="timestamp"),
                "source_data": IcebergFieldConfig(target_field="_source_data", iceberg_type="string"),
            }
        elif isinstance(self.iceberg_schema, list):
            # Convert list format to dict format for backward compatibility
            # In the new format, the field name is both source and target (no mapping)
            return {field.field_name: field for field in self.iceberg_schema}
        else:
            # Already in dict format
            return self.iceberg_schema


class BigQueryIcebergAuthentication(BaseModel):
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be inferred from environment",
        default="",
    )


class BigQueryIcebergConfigDetails(AbstractDestinationDetailsConfig):
    # BigQuery configuration
    project_id: str = Field(..., description="BigQuery Project ID")
    dataset_id: str = Field(..., description="BigQuery Dataset ID")
    dataset_location: str = Field(default="US", description="BigQuery Dataset location")

    # GCS configuration for Iceberg data
    gcs_warehouse_bucket: str = Field(..., description="GCS bucket for Iceberg warehouse")
    gcs_warehouse_path: str = Field(default="iceberg", description="Path within bucket for Iceberg warehouse")

    # Iceberg table configuration
    table_format: IcebergTableFormat = Field(
        default=IcebergTableFormat.PARQUET, description="Iceberg table file format"
    )
    iceberg_namespace: str = Field(default="bizon", description="Iceberg namespace (schema) for organizing tables")

    # BigLake configuration
    biglake_connection_id: str = Field(
        default="us.biglake-connection", description="BigLake connection ID for external table access"
    )

    # Iceberg catalog configuration
    catalog_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "default": {
                "type": "sql",
                "uri": "postgresql+psycopg2://username:password@localhost/mydatabase",
                "init_catalog_tables": False,
            }
        },
        description="Iceberg catalog configuration dictionary",
    )

    # Performance settings
    target_file_size_mb: int = Field(default=128, description="Target file size in MB for Iceberg files")
    write_batch_size: int = Field(default=1000, description="Batch size for writing records to Iceberg")

    # Retry configuration
    max_retry_attempts: int = Field(default=3, description="Maximum number of retry attempts for Iceberg operations")
    retry_backoff_base: float = Field(default=2.0, description="Base multiplier for exponential backoff (seconds)")
    retry_backoff_max: int = Field(default=60, description="Maximum backoff time between retries (seconds)")

    # Partitioning configuration
    time_partitioning: Optional[List[TimePartitionConfig]] = Field(
        default=None, description="Time-based partitioning configuration with explicit transform types"
    )

    # Destination table configuration for multi-table routing
    destination_table_config: Optional[List[DestinationTableConfig]] = Field(
        default=None, description="Configuration for multiple destination tables"
    )

    # Global upsert configuration (can be overridden per destination table)
    upsert: Optional[UpsertConfig] = Field(default=None, description="Global upsert configuration for merge operations")

    authentication: Optional[BigQueryIcebergAuthentication] = None


class BigQueryIcebergConfig(AbstractDestinationConfig):
    name: Literal["bigquery_iceberg"] = Field(default="bigquery_iceberg")
    alias: str = "bigquery_iceberg"
    buffer_size: Optional[int] = 400
    config: BigQueryIcebergConfigDetails
