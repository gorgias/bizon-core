import logging
from typing import Any, Dict, List, Optional

import polars as pl
import pyarrow as pa
import requests
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

logger = logging.getLogger(__name__)


# Polars to Iceberg type mappings for data writing utilities
POLARS_TO_ICEBERG_TYPE_MAPPING = {
    pl.String: StringType(),
    pl.Int32: IntegerType(),
    pl.Int64: LongType(),
    pl.Float64: DoubleType(),
    pl.Boolean: BooleanType(),
    pl.Datetime: TimestampType(),
}


def create_biglake_iceberg_external_table(
    project_id: str,
    dataset_id: str,
    table_name: str,
    iceberg_metadata_uri: str,
    connection_id: str = "us.biglake-connection",
    table_description: Optional[str] = None,
) -> bigquery.Table:
    """
    Creates a BigLake external table that references Iceberg data stored in GCS.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the external table to create
        iceberg_metadata_uri: GCS URI to the Iceberg metadata file (e.g., 'gs://bucket/table/metadata/metadata.json')
        connection_id: BigLake connection ID (default: 'us.biglake-connection')
        table_description: Optional table description

    Returns:
        Created BigQuery Table object (external table)

    Raises:
        Exception: If table creation fails

    Example:
        table = create_biglake_iceberg_external_table(
            project_id="my-project",
            dataset_id="my_dataset",
            table_name="my_external_iceberg_table",
            iceberg_metadata_uri="gs://my-bucket/iceberg/my_table/metadata/metadata.json",
            connection_id="us.biglake-connection",
            table_description="External Iceberg table"
        )
    """
    client = bigquery.Client(project=project_id)

    # Build table reference
    table_ref = client.dataset(dataset_id).table(table_name)

    # Create external data configuration for Iceberg
    external_config = bigquery.ExternalConfig("ICEBERG")
    external_config.source_uris = [iceberg_metadata_uri]
    external_config.connection_id = connection_id

    # Create table object
    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    if table_description:
        table.description = table_description

    try:
        # Create the external table
        table = client.create_table(table)
        logger.info(f"Successfully created BigLake Iceberg external table: {project_id}.{dataset_id}.{table_name}")
        return table

    except Exception as e:
        logger.error(f"Failed to create BigLake Iceberg external table: {e}")
        raise


def update_biglake_iceberg_metadata_uri(
    project_id: str, dataset_id: str, table_name: str, new_metadata_uri: str
) -> dict:
    """
    Updates the metadata URI for an existing BigLake Iceberg external table using REST API.
    This uses the tables.patch method with autodetect_schema=true.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the external table to update
        new_metadata_uri: New GCS URI to the Iceberg metadata file

    Returns:
        API response as dictionary

    Example:
        response = update_biglake_iceberg_metadata_uri(
            project_id="my-project",
            dataset_id="my_dataset",
            table_name="my_external_iceberg_table",
            new_metadata_uri="gs://my-bucket/iceberg/my_table/metadata/new_metadata.json"
        )
    """
    # Get default credentials
    creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # Refresh credentials if needed
    if not creds.valid:
        request = Request()
        creds.refresh(request)

    # Build the API URL with autodetect_schema=true
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_name}?autodetect_schema=true"  # noqa: E501

    # Request payload
    payload = {
        "externalDataConfiguration": {"sourceFormat": "ICEBERG", "sourceUris": [new_metadata_uri]},
        "schema": None,
    }

    # Headers
    headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}

    try:
        # Make the PATCH request
        response = requests.patch(url, json=payload, headers=headers)
        response.raise_for_status()

        logger.info(f"Successfully updated metadata URI for table: {project_id}.{dataset_id}.{table_name}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update metadata URI for table {project_id}.{dataset_id}.{table_name}: {e}")
        raise


def get_latest_iceberg_metadata_uri(pyiceberg_table) -> str:
    """
    Helper function to get the latest metadata URI from a PyIceberg table object.

    Args:
        pyiceberg_table: PyIceberg table object

    Returns:
        Latest metadata file URI

    Example:
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog("default", {...})
        iceberg_table = catalog.load_table("namespace.table_name")

        latest_metadata_uri = get_latest_iceberg_metadata_uri(iceberg_table)
    """
    return max(pyiceberg_table.metadata.metadata_log, key=lambda entry: entry.timestamp_ms).metadata_file


def setup_iceberg_catalog(catalog_config: Dict[str, Any], warehouse_path: str = "") -> Any:
    """
    Set up and return an Iceberg catalog instance from configuration dictionary.

    Args:
        catalog_config: Dictionary containing catalog configurations
        warehouse_path: Optional GCS path for the warehouse (e.g., 'gs://bucket/warehouse')

    Returns:
        Configured Iceberg catalog instance

    Example:
        config = {
            "default": {
                "type": "sql",
                "uri": "postgresql+psycopg2://username:password@localhost/mydatabase",
                "init_catalog_tables": False
            }
        }
        catalog = setup_iceberg_catalog(config)
    """
    # Get the first catalog config (usually "default")
    catalog_name = list(catalog_config.keys())[0]
    catalog_properties = catalog_config[catalog_name].copy()

    # Add warehouse path if not already specified and provided
    if warehouse_path and "warehouse" not in catalog_properties:
        catalog_properties["warehouse"] = warehouse_path

    return load_catalog(catalog_name, **catalog_properties)


def create_iceberg_schema_from_polars(df: pl.DataFrame) -> Schema:
    """
    Create an Iceberg schema from a Polars DataFrame.

    Args:
        df: Polars DataFrame to extract schema from

    Returns:
        Iceberg Schema object

    Example:
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        schema = create_iceberg_schema_from_polars(df)
    """
    fields = []
    field_id = 1

    for col_name, polars_type in df.schema.items():
        iceberg_type = POLARS_TO_ICEBERG_TYPE_MAPPING.get(polars_type, StringType())
        field = NestedField(field_id=field_id, name=col_name, field_type=iceberg_type, required=False)
        fields.append(field)
        field_id += 1

    return Schema(*fields)


def create_partition_spec(
    schema: Schema,
    time_partitioning: Optional[List[Dict[str, str]]] = None,
) -> Optional[PartitionSpec]:
    """
    Create an Iceberg partition specification from time partition configurations.

    Args:
        schema: Iceberg schema object
        time_partitioning: List of time partition configs with field and type

    Returns:
        PartitionSpec object or None if no partitioning

    Example:
        time_partitions = [{"field": "created_at", "type": "DAY"}]
        partition_spec = create_partition_spec(schema, time_partitions)
    """
    if not time_partitioning:
        return PartitionSpec()  # Return empty partition spec for no partitioning

    # Log partition configuration for debugging
    available_fields = [field.name for field in schema.fields]
    logger.debug(f"Available schema fields: {available_fields}")
    logger.debug(f"Configuring time partition fields: {[tp['field'] for tp in time_partitioning]}")

    partition_spec_fields = []
    field_id = 1000  # Start partition field IDs at 1000 to avoid conflicts

    # Handle time-based partitioning with explicit transforms
    for time_config in time_partitioning:
        field_name = time_config["field"]
        partition_type = time_config["type"]

        # Find the field in the schema
        schema_field = None
        for field in schema.fields:
            if field.name == field_name:
                schema_field = field
                break

        if not schema_field:
            logger.warning(f"Time partition field '{field_name}' not found in schema, skipping")
            continue

        # Choose transform based on partition type
        if partition_type == "YEAR":
            transform = YearTransform()
        elif partition_type == "MONTH":
            transform = MonthTransform()
        elif partition_type == "DAY":
            transform = DayTransform()
        elif partition_type == "HOUR":
            transform = HourTransform()
        else:
            logger.warning(f"Unknown partition type '{partition_type}', using day transform")
            transform = DayTransform()

        logger.info(f"Using {partition_type} partitioning for field: {field_name}")

        partition_field = PartitionField(
            source_id=schema_field.field_id,
            field_id=field_id,
            transform=transform,
            name=f"{field_name}_{partition_type.lower()}",
        )
        partition_spec_fields.append(partition_field)
        field_id += 1

    if not partition_spec_fields:
        logger.warning("No valid partition fields found - creating table without partitioning")
        return PartitionSpec()  # Return empty partition spec instead of None

    logger.info(f"Created partition spec with {len(partition_spec_fields)} partition fields")
    return PartitionSpec(*partition_spec_fields)


def write_polars_to_iceberg(iceberg_table: IcebergTable, df: pl.DataFrame, batch_size: int = 1000) -> None:
    """
    Write Polars DataFrame data to an Iceberg table.

    Args:
        iceberg_table: The Iceberg table to write to
        df: Polars DataFrame containing the data
        batch_size: Number of records to write per batch

    Example:
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        write_polars_to_iceberg(iceberg_table, df, batch_size=1000)
    """
    # Convert DataFrame to list of records
    records = df.to_dicts()

    # Process records for Iceberg compatibility
    processed_records = []
    for record in records:
        processed_record = {}
        for key, value in record.items():
            # Handle None values and data type conversions
            if value is None:
                processed_record[key] = None
            elif isinstance(value, (int, float, str, bool)):
                processed_record[key] = value
            else:
                # Convert other types to string representation
                processed_record[key] = str(value)
        processed_records.append(processed_record)

    # Write data in batches using PyArrow tables
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]  # noqa: E203

        # Convert batch to PyArrow table
        arrow_table = pa.Table.from_pylist(batch)
        iceberg_table.append(arrow_table)
        logger.info(f"Wrote batch {i//batch_size + 1} ({len(batch)} records) to Iceberg table")


def ensure_iceberg_namespace_exists(catalog: Any, namespace: str) -> None:
    """
    Ensure an Iceberg namespace exists, create if it doesn't.
    Args:
        catalog: Iceberg catalog instance
        namespace: Namespace name (e.g., "bizon", "analytics")
    """
    try:
        # Check if namespace exists
        catalog.load_namespace_properties(namespace)
        logger.info(f"Iceberg namespace '{namespace}' already exists")
    except Exception:
        # Namespace doesn't exist, create it
        try:
            catalog.create_namespace(namespace)
            logger.info(f"Created Iceberg namespace '{namespace}'")
        except Exception as e:
            logger.error(f"Failed to create Iceberg namespace '{namespace}': {e}")
            raise


def ensure_iceberg_table_exists(
    catalog: Any,
    table_name: str,
    schema: Schema,
    table_format: str = "parquet",
    target_file_size_mb: int = 128,
    time_partitioning: Optional[List[Dict[str, str]]] = None,
) -> IcebergTable:
    """
    Ensure an Iceberg table exists, create if it doesn't.

    Args:
        catalog: Iceberg catalog instance
        table_name: Full table name (e.g., "namespace.table")
        schema: Iceberg schema for the table
        table_format: File format for the table ("parquet", "orc", "avro")
        target_file_size_mb: Target file size in MB
        time_partitioning: List of time partition configs with field and type

    Returns:
        The Iceberg table instance

    Example:
        schema = create_iceberg_schema_from_polars(df)
        time_partitions = [{"field": "created_at", "type": "DAY"}]
        table = ensure_iceberg_table_exists(
            catalog, "default.my_table", schema,
            time_partitioning=time_partitions
        )
    """
    try:
        # Try to load existing table
        iceberg_table = catalog.load_table(table_name)
        logger.info(f"Loaded existing Iceberg table: {table_name}")
        return iceberg_table
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Creating new Iceberg table: {table_name}")

        # Create partition specification if time partitioning is provided
        partition_spec = create_partition_spec(schema, time_partitioning)
        if partition_spec and len(partition_spec.fields) > 0:
            logger.info(f"Creating partitioned table with {len(partition_spec.fields)} partition fields")
        else:
            # Use the empty partition spec returned by create_partition_spec
            if partition_spec is None:
                partition_spec = PartitionSpec()
            logger.info("Creating unpartitioned table")

        # Create the table with specified format and partitioning
        table_properties = {
            "write.format.default": table_format,
            "write.target-file-size-bytes": str(target_file_size_mb * 1024 * 1024),
        }

        iceberg_table = catalog.create_table(
            table_name, schema=schema, partition_spec=partition_spec, properties=table_properties
        )
        logger.info(f"Created Iceberg table: {table_name}")
        return iceberg_table
