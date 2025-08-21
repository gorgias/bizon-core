import os
import signal
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import polars as pl
from google.api_core.exceptions import NotFound, ServerError, ServiceUnavailable
from google.cloud import bigquery, storage
from google.cloud.bigquery import DatasetReference
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import NestedField, Schema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from requests.exceptions import ConnectionError, SSLError, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.config import SourceSyncModes
from bizon.source.source import AbstractSourceCallback

from .config import BigQueryIcebergConfigDetails, UpsertConfig
from .iceberg_utils import (
    create_biglake_iceberg_external_table,
    ensure_iceberg_namespace_exists,
    ensure_iceberg_table_exists,
    get_latest_iceberg_metadata_uri,
    update_biglake_iceberg_metadata_uri,
)

# Mapping from Polars data types to Iceberg types
POLARS_TO_ICEBERG_TYPE_MAPPING = {
    pl.String: StringType(),
    pl.Int32: IntegerType(),
    pl.Int64: LongType(),
    pl.Float64: DoubleType(),
    pl.Boolean: BooleanType(),
    pl.Datetime: TimestampType(),
}


class OperationTimeoutError(Exception):
    """Raised when an Iceberg operation times out"""

    pass


def run_with_timeout(func, timeout_seconds=600, *args, **kwargs):
    """
    Run a function with a timeout. If the function doesn't complete within
    the specified time, raise OperationTimeoutError.
    """
    result = [None]
    exception = [None]

    def target():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout_seconds)

    if thread.is_alive():
        # Thread is still running, operation timed out
        logger.error(f"Operation timed out after {timeout_seconds}s - thread may be hanging")
        raise OperationTimeoutError(f"Operation timed out after {timeout_seconds} seconds")

    if exception[0]:
        raise exception[0]

    return result[0]


class BigQueryIcebergDestination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryIcebergConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        super().__init__(sync_metadata, config, backend, source_callback, monitor)
        self.config: BigQueryIcebergConfigDetails = config

        # Set up authentication
        if config.authentication and config.authentication.service_account_key:
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.authentication.service_account_key.encode())
                temp_file_path = temp.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

        self.project_id = config.project_id
        self.bq_client = bigquery.Client(project=self.project_id)
        self.gcs_client = storage.Client(project=self.project_id)
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location

        # Initialize destination_id for routing (similar to bigquery_streaming_v2)
        self._setup_destination_id()

        # Initialize Iceberg catalog
        self._setup_iceberg_catalog()

    def _setup_iceberg_catalog(self):
        """Set up the Iceberg catalog configuration"""
        catalog_name = list(self.config.catalog_config.keys())[0]
        catalog_properties = self.config.catalog_config[catalog_name].copy()

        # Add warehouse path if not already specified and using file-based catalog
        if "warehouse" not in catalog_properties:
            warehouse_path = f"gs://{self.config.gcs_warehouse_bucket}/{self.config.gcs_warehouse_path}"
            catalog_properties["warehouse"] = warehouse_path

        # Convert boolean values to strings for pyiceberg compatibility
        for key, value in catalog_properties.items():
            if isinstance(value, bool):
                catalog_properties[key] = str(value).lower()

        self.catalog = load_catalog(catalog_name, **catalog_properties)

    def _setup_destination_id(self):
        """Set up destination_id routing logic using destination_table_config"""
        # If we have destination_table_config with exactly one destination, use it
        if self.config.destination_table_config and len(self.config.destination_table_config) == 1:
            self.destination_id = self.config.destination_table_config[0].destination_id
        # For multiple destinations, destination_id will be set dynamically during processing
        # Otherwise, destination_id remains None and we use the default table naming

    def get_iceberg_schema_for_destination(self) -> Optional[Dict[str, str]]:
        """Get Iceberg schema mapping for the current destination_id"""
        if not self.config.destination_table_config:
            return None

        # Find the destination config that matches our destination_id
        for dest_config in self.config.destination_table_config:
            if dest_config.destination_id == self.destination_id:
                # Use the new method to get schema dict regardless of format
                schema_dict = dest_config.get_iceberg_schema_dict()
                if schema_dict:
                    # Convert IcebergFieldConfig to simple mapping dict
                    return {source_field: field_config.field_name for source_field, field_config in schema_dict.items()}
        return None

    def get_mapped_field_name(self, source_field_name: str) -> str:
        """Get the mapped target field name for a source field, or return original if no mapping"""
        schema_mapping = self.get_iceberg_schema_for_destination()
        if schema_mapping and source_field_name in schema_mapping:
            return schema_mapping[source_field_name]
        # Check default Bizon mapping
        default_mapping = {
            "bizon_extracted_at": "_bizon_extracted_at",
            "bizon_id": "_bizon_id",
            "bizon_loaded_at": "_bizon_loaded_at",
            "source_record_id": "_source_record_id",
            "source_timestamp": "_source_timestamp",
            "source_data": "_source_data",
        }
        return default_mapping.get(source_field_name, source_field_name)

    def get_upsert_config_for_destination(self) -> Optional[UpsertConfig]:
        """Get upsert configuration for the current destination_id"""
        # First check if there's a destination-specific upsert config
        if self.config.destination_table_config:
            for dest_config in self.config.destination_table_config:
                if dest_config.destination_id == self.destination_id:
                    if dest_config.upsert:
                        return dest_config.upsert

        # Fall back to global upsert config
        return self.config.upsert

    @property
    def table_id(self) -> str:
        """Generate BigQuery table ID with destination_id routing"""
        if self.destination_id:
            # If destination_id is already a full table path (project.dataset.table), use it directly
            if "." in self.destination_id and len(self.destination_id.split(".")) == 3:
                return self.destination_id
            # Otherwise append to our project and dataset
            return f"{self.project_id}.{self.dataset_id}.{self.destination_id}"
        # Default table naming
        table_name = f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    @property
    def iceberg_table_name(self) -> str:
        """Generate Iceberg table name with destination_id routing"""
        if self.destination_id:
            # Extract table name from destination_id
            if "." in self.destination_id and len(self.destination_id.split(".")) == 3:
                table_name = self.destination_id.split(".")[-1]
            else:
                table_name = self.destination_id
        else:
            # Default table naming
            table_name = f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return f"{self.config.iceberg_namespace}.{table_name}"

    def get_table_name_for_metadata(self) -> str:
        """Get table name for metadata operations (without project/dataset prefix)"""
        if self.destination_id:
            if "." in self.destination_id and len(self.destination_id.split(".")) == 3:
                return self.destination_id.split(".")[-1]
            else:
                return self.destination_id
        return f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"

    def _create_iceberg_schema_from_polars(self, df: pl.DataFrame) -> Schema:
        """Convert Polars DataFrame schema to Iceberg schema using custom schema configuration"""
        fields = []
        field_id = 1

        # Get custom schema mapping if available
        custom_schema_dict = None
        if self.config.destination_table_config:
            for dest_config in self.config.destination_table_config:
                if dest_config.destination_id == self.destination_id:
                    custom_schema_dict = dest_config.get_iceberg_schema_dict()
                    break

        for col_name, polars_type in df.schema.items():
            # Check if this field has a custom type defined
            if custom_schema_dict and col_name in custom_schema_dict:
                field_config = custom_schema_dict[col_name]
                custom_type = field_config.field_type.lower()

                # Map custom type strings to Iceberg types
                if custom_type == "timestamp":
                    iceberg_type = TimestampType()
                elif custom_type == "string":
                    iceberg_type = StringType()
                elif custom_type == "long":
                    iceberg_type = LongType()
                elif custom_type == "integer":
                    iceberg_type = IntegerType()
                elif custom_type == "double":
                    iceberg_type = DoubleType()
                elif custom_type == "boolean":
                    iceberg_type = BooleanType()
                else:
                    # Fallback to string for unknown custom types
                    logger.warning(f"Unknown custom type '{custom_type}' for field '{col_name}', using string")
                    iceberg_type = StringType()

            else:
                # Use default polars-to-iceberg mapping
                iceberg_type = POLARS_TO_ICEBERG_TYPE_MAPPING.get(polars_type, StringType())

            field = NestedField(field_id=field_id, name=col_name, field_type=iceberg_type, required=False)
            fields.append(field)
            field_id += 1

        return Schema(*fields)

    def _ensure_iceberg_table_exists(self, df: pl.DataFrame) -> IcebergTable:
        """Ensure the Iceberg table exists, create if it doesn't"""
        # Ensure namespace exists first
        ensure_iceberg_namespace_exists(self.catalog, self.config.iceberg_namespace)

        table_name = self.iceberg_table_name
        schema = self._create_iceberg_schema_from_polars(df)

        time_partitioning_dict = None
        if self.config.time_partitioning:
            time_partitioning_dict = [
                {"field": self.get_mapped_field_name(tp.field), "type": tp.type.value}
                for tp in self.config.time_partitioning
            ]

        return ensure_iceberg_table_exists(
            catalog=self.catalog,
            table_name=table_name,
            schema=schema,
            table_format=self.config.table_format.value,
            target_file_size_mb=self.config.target_file_size_mb,
            time_partitioning=time_partitioning_dict,
        )

    def _ensure_biglake_external_table_exists(self, iceberg_table: IcebergTable):
        """Ensure the BigLake external table exists, create if it doesn't"""
        # Double-check that table has snapshots before creating BigLake table
        if iceberg_table.current_snapshot() is None:
            logger.warning(
                f"Skipping BigLake external table creation - no snapshots in Iceberg table: {self.iceberg_table_name}"
            )
            return

        table_name = self.get_table_name_for_metadata()

        try:
            # Check if external table exists
            self.bq_client.get_table(self.table_id)
            logger.info(f"BigLake external table already exists: {self.table_id}")
        except NotFound:
            # External table doesn't exist, create it
            logger.info(f"Creating BigLake external table: {self.table_id}")
            metadata_uri = get_latest_iceberg_metadata_uri(iceberg_table)

            create_biglake_iceberg_external_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_name=table_name,
                iceberg_metadata_uri=metadata_uri,
                connection_id=self.config.biglake_connection_id,
                table_description=f"External table for Iceberg data: {self.iceberg_table_name}",
            )

    def check_connection(self) -> bool:
        """Check connection to BigQuery and create dataset if needed"""
        dataset_ref = DatasetReference(self.project_id, self.dataset_id)

        try:
            self.bq_client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.dataset_location
            dataset = self.bq_client.create_dataset(dataset)
            logger.info(f"Created BigQuery dataset: {self.dataset_id}")

        return True

    def _flatten_nested_structures(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten nested structures that might cause PyArrow/Iceberg schema issues"""
        try:
            # Convert nested structs to JSON strings to avoid schema complexity
            # This handles cases where Polars unnest creates deeply nested structures
            flattening_expressions = []

            for col_name, col_type in df.schema.items():
                if isinstance(col_type, pl.Struct):
                    # Convert struct columns to JSON strings using cast which is more efficient
                    flattening_expressions.append(pl.col(col_name).cast(pl.Utf8).alias(col_name))
                else:
                    # Keep non-struct columns as-is
                    flattening_expressions.append(pl.col(col_name))

            if flattening_expressions:
                df = df.with_columns(flattening_expressions)

            return df
        except Exception as e:
            logger.warning(f"Failed to flatten nested structures: {e}")
            # Return original DataFrame if flattening fails
            return df

    @retry(
        retry=retry_if_exception_type(
            (
                ServerError,
                ServiceUnavailable,
                ConnectionError,
                SSLError,
                Timeout,
                OSError,
                OperationTimeoutError,
            )
        ),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        stop=stop_after_attempt(5),  # Use config value or default to 5 attempts
        before_sleep=lambda retry_state: logger.warning(
            f"Iceberg write attempt {retry_state.attempt_number} failed. "
            f"Retrying in {retry_state.next_action.sleep} seconds..."
        ),
    )
    def _write_to_iceberg_with_retry(
        self, iceberg_table: IcebergTable, arrow_table, upsert_config: Optional[UpsertConfig] = None
    ) -> dict:
        """Write data to Iceberg table with upsert support and comprehensive retry strategy"""
        operation_start = time.time()
        operation_type = "upsert" if (upsert_config and upsert_config.enabled) else "append"

        logger.info(f"Starting Iceberg {operation_type} with {arrow_table.num_rows} rows")

        try:
            # Pre-operation validation
            if arrow_table.num_rows == 0:
                logger.warning("Attempted to write empty arrow table - skipping operation")
                return {"success": True, "rows_processed": 0, "operation": operation_type, "skipped": True}

            # Check if table is empty (no snapshots) - upsert requires existing data
            table_is_empty = iceberg_table.current_snapshot() is None

            # Track operation metrics
            result_metrics = {
                "success": False,
                "rows_processed": 0,
                "operation": operation_type,
                "duration_seconds": 0,
                "table_was_empty": table_is_empty,
                "skipped": False,
            }

            if upsert_config and upsert_config.enabled and not table_is_empty:
                # Perform upsert operation on non-empty table
                logger.info(f"Executing upsert with join_cols: {upsert_config.join_cols}")

                # Monitor for silent failures - upsert should return a result
                upsert_start = time.time()
                logger.info("Starting upsert operation with timeout protection...")

                def upsert_operation():
                    return iceberg_table.upsert(
                        df=arrow_table,
                        join_cols=upsert_config.join_cols,
                        when_matched_update_all=upsert_config.when_matched_update_all,
                        when_not_matched_insert_all=upsert_config.when_not_matched_insert_all,
                        case_sensitive=upsert_config.case_sensitive,
                    )

                # Use timeout protection (10 minutes for upsert operations)
                result = run_with_timeout(upsert_operation, timeout_seconds=600)
                upsert_duration = time.time() - upsert_start

                # Validate upsert result
                if result is None:
                    raise RuntimeError("Upsert operation returned None - possible silent failure")

                rows_updated = getattr(result, "rows_updated", 0) if result else 0
                rows_inserted = getattr(result, "rows_inserted", 0) if result else 0
                total_affected = rows_updated + rows_inserted

                # Check for suspicious results
                if total_affected == 0 and arrow_table.num_rows > 0:
                    logger.warning(
                        f"Upsert processed {arrow_table.num_rows} input rows but affected 0 rows - "
                        f"possible data issue or join key mismatch"
                    )

                logger.info(
                    f"Upsert completed in {upsert_duration:.2f}s - "
                    f"updated: {rows_updated}, inserted: {rows_inserted}, total affected: {total_affected}"
                )

                result_metrics.update(
                    {
                        "success": True,
                        "rows_processed": total_affected,
                        "rows_updated": rows_updated,
                        "rows_inserted": rows_inserted,
                        "duration_seconds": upsert_duration,
                    }
                )

            elif upsert_config and upsert_config.enabled and table_is_empty:
                # First write to empty table - use append instead of upsert
                logger.info("Table is empty, performing initial append instead of upsert with timeout protection...")

                append_start = time.time()

                def append_operation():
                    return iceberg_table.append(arrow_table)

                # Use timeout protection (5 minutes for append operations)
                run_with_timeout(append_operation, timeout_seconds=300)
                append_duration = time.time() - append_start

                logger.info(f"Initial append completed in {append_duration:.2f}s - {arrow_table.num_rows} records")

                result_metrics.update(
                    {
                        "success": True,
                        "rows_processed": arrow_table.num_rows,
                        "duration_seconds": append_duration,
                        "operation": "initial_append",
                    }
                )

            else:
                # Perform regular append operation
                logger.info("Performing regular append with timeout protection...")
                append_start = time.time()

                def append_operation():
                    return iceberg_table.append(arrow_table)

                # Use timeout protection (5 minutes for append operations)
                run_with_timeout(append_operation, timeout_seconds=300)
                append_duration = time.time() - append_start

                logger.info(f"Append completed in {append_duration:.2f}s - {arrow_table.num_rows} records")

                result_metrics.update(
                    {"success": True, "rows_processed": arrow_table.num_rows, "duration_seconds": append_duration}
                )

            # Final validation
            total_duration = time.time() - operation_start
            result_metrics["total_duration_seconds"] = total_duration

            # Check for abnormally long operations
            if total_duration > 300:  # 5 minutes
                logger.warning(f"Operation took unusually long: {total_duration:.2f}s")

            return result_metrics

        except Exception as e:
            duration = time.time() - operation_start
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "operation": operation_type,
                "duration_seconds": duration,
                "input_rows": arrow_table.num_rows,
                "table_was_empty": table_is_empty if "table_is_empty" in locals() else None,
            }

            logger.error(f"Iceberg {operation_type} failed after {duration:.2f}s: {error_details}")

            # Re-raise with enriched context
            raise RuntimeError(f"Iceberg {operation_type} operation failed: {e}") from e

    def _group_records_by_destination(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Group DataFrame records by destination_id for multi-destination processing"""
        destination_groups = {}

        # Check if DataFrame has a destination_id column (from explicit routing)
        if "destination_id" in df.columns:
            # Group by destination_id column if it exists
            for dest_id in df["destination_id"].unique():
                dest_df = df.filter(pl.col("destination_id") == dest_id)
                destination_groups[dest_id] = dest_df
        else:
            # Single destination case - use current destination_id
            # This is the most common case with Kafka source
            destination_groups[self.destination_id or "default"] = df

        return destination_groups

    def _update_biglake_if_ready(self, iceberg_table: IcebergTable, context: str = "") -> None:
        """Update BigLake external table if Iceberg table has snapshots.

        Args:
            iceberg_table: The Iceberg table instance
            context: Optional context string for logging
        """
        try:
            current_snapshot = iceberg_table.current_snapshot()
            logger.debug(f"Iceberg table snapshot {context}: {current_snapshot}")

            if current_snapshot is not None:
                self._ensure_biglake_external_table_exists(iceberg_table)
                self._update_biglake_metadata(iceberg_table)
                logger.info(f"BigLake external table updated {context}")
            else:
                logger.warning(f"No snapshots yet, skipping BigLake creation {context}")
        except Exception as e:
            logger.error(f"Error updating BigLake {context}: {e}")
            logger.warning(f"Skipping BigLake operations {context}")

    def _write_to_destination(self, dest_id: str, dest_df: pl.DataFrame) -> None:
        """Write records to a specific destination with full processing pipeline"""
        batch_start = time.time()
        record_count = len(dest_df)

        # Temporarily set destination_id for this batch
        original_dest_id = self.destination_id
        self.destination_id = dest_id

        try:
            # Create/verify Iceberg table
            table_start = time.time()
            iceberg_table = self._ensure_iceberg_table_exists(dest_df)
            table_time = time.time() - table_start
            logger.info(f"Table creation/verification for {dest_id} completed in {table_time:.2f}s")

            # Get upsert configuration
            upsert_config = self.get_upsert_config_for_destination()

            # Handle batching for large datasets
            batch_size = self.config.write_batch_size or 1000
            logger.info(
                f"Using batch size: {batch_size} for {'upsert' if (upsert_config and upsert_config.enabled) else 'append'} operation"
            )

            if record_count > batch_size:
                num_batches = (record_count + batch_size - 1) // batch_size
                logger.info(f"Processing {record_count} records in {num_batches} sequential batches")

                for batch_idx in range(num_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, record_count)
                    batch_df = dest_df[start_idx:end_idx]

                    # Process batch
                    arrow_start = time.time()
                    arrow_batch = batch_df.to_arrow()
                    arrow_time = time.time() - arrow_start
                    logger.info(f"Batch {batch_idx + 1}/{num_batches} arrow conversion completed in {arrow_time:.2f}s")

                    write_start = time.time()
                    result = self._write_to_iceberg_with_retry(iceberg_table, arrow_batch, upsert_config)
                    write_time = time.time() - write_start

                    # Monitor for abrupt stops or issues
                    if not result.get("success", False):
                        error_msg = f"Batch {batch_idx + 1}/{num_batches} failed unexpectedly"
                        logger.error(error_msg)
                        raise RuntimeError(error_msg)

                    if result.get("skipped", False):
                        logger.warning(f"Batch {batch_idx + 1}/{num_batches} was skipped - empty data")

                    rows_processed = result.get("rows_processed", 0)
                    expected_rows = end_idx - start_idx

                    # For upsert, rows_processed might be less than input if duplicates exist
                    if upsert_config and upsert_config.enabled:
                        logger.info(
                            f"Batch {batch_idx + 1}/{num_batches} upsert completed in {write_time:.2f}s - "
                            f"input: {expected_rows}, affected: {rows_processed} rows"
                        )
                    else:
                        # For append, expect all rows to be processed
                        if rows_processed != expected_rows and not result.get("skipped", False):
                            logger.warning(
                                f"Batch {batch_idx + 1}/{num_batches} processed {rows_processed} rows "
                                f"but expected {expected_rows} - possible data loss"
                            )
                        logger.info(
                            f"Batch {batch_idx + 1}/{num_batches} append completed in {write_time:.2f}s ({rows_processed} records)"
                        )
            else:
                # Small dataset - process directly
                arrow_start = time.time()
                arrow_table = dest_df.to_arrow()
                arrow_time = time.time() - arrow_start
                logger.info(f"Arrow conversion completed in {arrow_time:.2f}s for {record_count} records")

                write_start = time.time()
                result = self._write_to_iceberg_with_retry(iceberg_table, arrow_table, upsert_config)
                write_time = time.time() - write_start

                # Monitor for abrupt stops or issues
                if not result.get("success", False):
                    error_msg = f"Single batch write failed unexpectedly for {record_count} records"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                if result.get("skipped", False):
                    logger.warning("Single batch was skipped - empty data")

                rows_processed = result.get("rows_processed", 0)

                # For upsert, rows_processed might be different from input
                if upsert_config and upsert_config.enabled:
                    logger.info(
                        f"Single batch upsert completed in {write_time:.2f}s - "
                        f"input: {record_count}, affected: {rows_processed} rows"
                    )
                else:
                    # For append, expect all rows to be processed
                    if rows_processed != record_count and not result.get("skipped", False):
                        logger.warning(
                            f"Single batch processed {rows_processed} rows "
                            f"but expected {record_count} - possible data loss"
                        )
                    logger.info(f"Single batch append completed in {write_time:.2f}s for {rows_processed} records")

            # Update BigLake table
            biglake_start = time.time()
            self._update_biglake_if_ready(iceberg_table, f"for {dest_id}")
            biglake_time = time.time() - biglake_start
            if biglake_time > 0.1:
                logger.info(f"BigLake update for {dest_id} completed in {biglake_time:.2f}s")

            batch_time = time.time() - batch_start
            logger.info(f"Successfully wrote {record_count} records to destination {dest_id} in {batch_time:.2f}s")

        finally:
            # Restore original destination_id
            self.destination_id = original_dest_id

    @retry(
        retry=retry_if_exception_type(
            (
                ServerError,
                ServiceUnavailable,
                ConnectionError,
                SSLError,
                Timeout,
                OSError,
            )
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),  # Shorter backoff for metadata ops
        stop=stop_after_attempt(3),  # Fewer attempts for non-critical operations
        before_sleep=lambda retry_state: logger.warning(
            f"BigLake metadata update attempt {retry_state.attempt_number} failed. "
            f"Retrying in {retry_state.next_action.sleep} seconds..."
        ),
    )
    def _update_biglake_metadata(self, iceberg_table) -> None:
        """Update BigLake external table metadata URI with retry protection"""
        latest_metadata_uri = get_latest_iceberg_metadata_uri(iceberg_table)
        table_name = self.get_table_name_for_metadata()

        try:
            update_biglake_iceberg_metadata_uri(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_name=table_name,
                new_metadata_uri=latest_metadata_uri,
            )
            logger.info(f"Updated BigLake external table metadata URI: {self.table_id}")
        except Exception as e:
            logger.error(f"BigLake metadata update failed: {type(e).__name__}: {e}")
            raise  # Let retry decorator handle this

    def _preprocess_records_for_schema_mapping(self, df_destination_records: pl.DataFrame) -> pl.DataFrame:
        """Extract and preprocess business data from source_data column when custom schema mapping is configured"""
        preprocessing_start = time.time()

        # Pre-process JSON to convert complex schema objects to strings
        import json

        def preprocess_json_for_polars(json_str):
            """Pre-process JSON to handle complex Debezium schema objects"""
            try:
                data = json.loads(json_str)
                # Convert the __schema field to JSON string if it's complex
                if "__schema" in data and isinstance(data["__schema"], dict):
                    data["__schema"] = json.dumps(data["__schema"])
                return json.dumps(data)
            except Exception:
                return json_str  # Return original if parsing fails

        # Pre-process the source_data to handle complex schemas
        json_preprocess_start = time.time()
        df_destination_records = df_destination_records.with_columns(
            [
                pl.col("source_data")
                .map_elements(preprocess_json_for_polars, return_dtype=pl.Utf8)
                .alias("preprocessed_data")
            ]
        )
        json_preprocess_time = time.time() - json_preprocess_start
        logger.info(
            f"JSON pre-processing completed in {json_preprocess_time:.2f}s for {len(df_destination_records)} records"
        )

        # Parse the preprocessed JSON with Polars
        json_parse_start = time.time()
        df_destination_records = (
            df_destination_records.with_columns([pl.col("preprocessed_data").str.json_decode().alias("parsed_json")])
            .unnest("parsed_json")
            .drop(
                [
                    "source_data",
                    "preprocessed_data",
                    "bizon_extracted_at",
                    "bizon_id",
                    "bizon_loaded_at",
                    "source_record_id",
                    "source_timestamp",
                ]
            )
        )
        json_parse_time = time.time() - json_parse_start
        logger.info(f"JSON parsing completed in {json_parse_time:.2f}s")

        # Flatten any nested structures to avoid schema mismatch issues
        flatten_start = time.time()
        df_destination_records = self._flatten_nested_structures(df_destination_records)
        flatten_time = time.time() - flatten_start
        if flatten_time > 0.1:  # Only log if significant time
            logger.info(f"Flattening nested structures completed in {flatten_time:.2f}s")

        # Convert timestamp strings to proper timestamp objects for Iceberg compatibility
        timestamp_columns = []

        # Get schema configuration once
        custom_schema_dict = None
        if self.config.destination_table_config:
            for dest_config in self.config.destination_table_config:
                if dest_config.destination_id == self.destination_id:
                    custom_schema_dict = dest_config.get_iceberg_schema_dict()
                    break

        # Identify timestamp columns
        if custom_schema_dict:
            for col_name in df_destination_records.columns:
                if col_name in custom_schema_dict:
                    field_config = custom_schema_dict[col_name]
                    if field_config.field_type.lower() == "timestamp":
                        timestamp_columns.append(col_name)

        # Convert timestamp string columns to datetime objects in a single operation
        if timestamp_columns:
            timestamp_start = time.time()
            # Convert all timestamp columns in one with_columns operation for better performance
            timestamp_expressions = []
            for ts_col in timestamp_columns:
                if ts_col in df_destination_records.columns:
                    timestamp_expressions.append(pl.col(ts_col).str.to_datetime().alias(ts_col))

            if timestamp_expressions:
                df_destination_records = df_destination_records.with_columns(timestamp_expressions)

            timestamp_time = time.time() - timestamp_start
            logger.info(f"Timestamp conversion completed in {timestamp_time:.2f}s for {len(timestamp_columns)} columns")

        total_preprocessing_time = time.time() - preprocessing_start
        logger.info(f"Total preprocessing completed in {total_preprocessing_time:.2f}s")

        return df_destination_records

    def _process_records_to_destinations(self, df_destination_records: pl.DataFrame) -> None:
        """Process and write records to appropriate destinations"""
        if self.config.destination_table_config and len(self.config.destination_table_config) > 1:
            # Multi-destination processing: group by destination_id and process in parallel
            destination_groups = self._group_records_by_destination(df_destination_records)

            with ThreadPoolExecutor(max_workers=min(len(destination_groups), 4)) as executor:
                future_to_dest = {
                    executor.submit(self._write_to_destination, dest_id, dest_df): dest_id
                    for dest_id, dest_df in destination_groups.items()
                }

                # Collect results as they complete
                for future in as_completed(future_to_dest):
                    dest_id = future_to_dest[future]
                    try:
                        future.result()  # This will raise any exceptions
                    except Exception as e:
                        logger.error(f"Failed to process destination {dest_id}: {e}")
                        raise
        else:
            # Single destination processing
            dest_id = self.destination_id or "default"
            self._write_to_destination(dest_id, df_destination_records)

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        """Write records to Iceberg table and update BigLake external table using direct conversion"""
        start_time = time.time()
        record_count = len(df_destination_records)
        logger.info(f"Starting write_records for {record_count} records to destination: {self.destination_id}")

        try:
            # Re-setup destination_id in case destination_table_config was updated
            self._setup_destination_id()

            # Check if we have custom schema mapping configured
            # When destination_table_config is present, we should extract business data from source_data
            has_destination_config = bool(self.config.destination_table_config)
            schema_mapping = self.get_iceberg_schema_for_destination()

            if has_destination_config:
                # When schema mapping is configured, extract data from source_data column
                logger.info("Custom schema mapping detected - extracting data from source_data column")
                df_destination_records = self._preprocess_records_for_schema_mapping(df_destination_records)

            else:
                # No custom schema mapping - use default Bizon field mapping
                logger.info("No custom schema mapping - using default Bizon field mapping")
                schema_mapping = {
                    "bizon_extracted_at": "_bizon_extracted_at",
                    "bizon_id": "_bizon_id",
                    "bizon_loaded_at": "_bizon_loaded_at",
                    "source_record_id": "_source_record_id",
                    "source_timestamp": "_source_timestamp",
                    "source_data": "_source_data",
                }
                # Rename fields to match expected schema
                df_destination_records = df_destination_records.rename(schema_mapping)

            # Process records to appropriate destinations
            self._process_records_to_destinations(df_destination_records)
            total_time = time.time() - start_time
            logger.info(f"Completed processing {record_count} records in {total_time:.2f}s")
            return True, ""

        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Error writing data to Iceberg after {total_time:.2f}s: {e}")
            logger.error(traceback.format_exc())
            return False, str(e)

    def finalize(self) -> bool:
        """Finalize the destination operations"""
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            logger.info("Full refresh completed - Iceberg table updated")
            return True
        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            logger.info("Incremental sync completed - Iceberg table updated")
            return True
        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            logger.info("Stream sync completed - Iceberg table updated")
            return True

        return True
