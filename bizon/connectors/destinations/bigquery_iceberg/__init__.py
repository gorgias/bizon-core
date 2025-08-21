"""BigQuery Iceberg destination connector package."""

from .src.config import (
    BigQueryIcebergConfig,
    BigQueryIcebergConfigDetails,
    DestinationTableConfig,
    IcebergFieldConfig,
    TimePartitionConfig,
    TimePartitionType,
)

# Note: BigQueryIcebergDestination not imported here to avoid circular imports
# It will be imported dynamically in the destination factory
from .src.iceberg_utils import (
    create_biglake_iceberg_external_table,
    create_iceberg_schema_from_polars,
    create_partition_spec,
    ensure_iceberg_namespace_exists,
    ensure_iceberg_table_exists,
    get_latest_iceberg_metadata_uri,
    setup_iceberg_catalog,
    update_biglake_iceberg_metadata_uri,
    write_polars_to_iceberg,
)

__all__ = [
    "BigQueryIcebergConfig",
    "BigQueryIcebergConfigDetails",
    "DestinationTableConfig",
    "IcebergFieldConfig",
    "TimePartitionConfig",
    "TimePartitionType",
    "create_biglake_iceberg_external_table",
    "create_iceberg_schema_from_polars",
    "create_partition_spec",
    "ensure_iceberg_namespace_exists",
    "ensure_iceberg_table_exists",
    "get_latest_iceberg_metadata_uri",
    "setup_iceberg_catalog",
    "update_biglake_iceberg_metadata_uri",
    "write_polars_to_iceberg",
]
