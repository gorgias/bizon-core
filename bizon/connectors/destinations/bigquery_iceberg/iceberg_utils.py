import logging
from typing import Dict, List, Optional

import requests
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import bigquery

logger = logging.getLogger(__name__)


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
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_name}?autodetect_schema=true"

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


# # Example usage
# if __name__ == "__main__":
#     # Example: Create external table pointing to Iceberg data
#     table = create_biglake_iceberg_external_table(
#         project_id="gorgias-growth-production",
#         dataset_id="airbyte",
#         table_name="test_external_iceberg_table",
#         iceberg_metadata_uri="gs://test-biglake-warehouse/default.db/my_table/metadata/metadata.json",
#         connection_id="us.biglake-connection",
#         table_description="External BigLake table for Iceberg data"
#     )

#     print(f"Created external table: {table.full_table_id}")

#     # Example: Update metadata URI (e.g., after new data is added to Iceberg table)
#     # updated_table = update_biglake_iceberg_metadata_uri(
#     #     project_id="gorgias-growth-production",
#     #     dataset_id="airbyte",
#     #     table_name="test_external_iceberg_table",
#     #     new_metadata_uri="gs://test-biglake-warehouse/default.db/my_table/metadata/new_metadata.json"
#     # )
