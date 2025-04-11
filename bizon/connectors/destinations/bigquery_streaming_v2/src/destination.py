import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Tuple, Type

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import DatasetReference, TimePartitioning
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    ProtoRows,
    ProtoSchema,
)
from google.protobuf.json_format import ParseDict
from google.protobuf.message import Message
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.source.callback import AbstractSourceCallback

from .config import BigQueryStreamingV2ConfigDetails
from .proto_utils import get_proto_schema_and_class


class BigQueryStreamingV2Destination(AbstractDestination):

    # Add constants for limits
    MAX_ROWS_PER_REQUEST = 5000  # 5000 (max is 10000)
    MAX_REQUEST_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (max is 10MB)
    MAX_ROW_SIZE_BYTES = 0.9 * 1024 * 1024  # 1 MB

    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryStreamingV2ConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
    ):  # type: ignore
        super().__init__(sync_metadata, config, backend, source_callback)
        self.config: BigQueryStreamingV2ConfigDetails = config

        if config.authentication and config.authentication.service_account_key:
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.authentication.service_account_key.encode())
                temp_file_path = temp.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

        self.project_id = config.project_id
        self.bq_client = bigquery.Client(project=self.project_id)
        self.bq_storage_client = bigquery_storage_v1.BigQueryWriteClient()
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location
        self.bq_max_rows_per_request = config.bq_max_rows_per_request

    @property
    def table_id(self) -> str:
        tabled_id = f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return self.destination_id or f"{self.project_id}.{self.dataset_id}.{tabled_id}"

    def get_bigquery_schema(self) -> List[bigquery.SchemaField]:

        if self.config.unnest:
            if len(list(self.record_schemas.keys())) == 1:
                self.destination_id = list(self.record_schemas.keys())[0]

            return [
                bigquery.SchemaField(
                    name=col.name,
                    field_type=col.type,
                    mode=col.mode,
                    description=col.description,
                    default_value_expression=col.default_value_expression,
                )
                for col in self.record_schemas[self.destination_id]
            ]

        # Case we don't unnest the data
        else:
            return [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"
                ),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

    def check_connection(self) -> bool:
        dataset_ref = DatasetReference(self.project_id, self.dataset_id)

        try:
            self.bq_client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.dataset_location
            dataset = self.bq_client.create_dataset(dataset)
        return True

    def append_rows_to_stream(
        self,
        write_client: bigquery_storage_v1.BigQueryWriteClient,
        stream_name: str,
        proto_schema: ProtoSchema,
        serialized_rows: List[bytes],
    ):
        request = AppendRowsRequest(
            write_stream=stream_name,
            proto_rows=AppendRowsRequest.ProtoData(
                rows=ProtoRows(serialized_rows=serialized_rows),
                writer_schema=proto_schema,
            ),
        )
        response = write_client.append_rows(iter([request]))
        return response.code().name

    def safe_cast_record_values(self, row: dict):
        for col in self.record_schemas[self.destination_id]:
            if col.type in ["TIMESTAMP", "DATETIME"] and col.default_value_expression is None:
                if isinstance(row[col.name], int):
                    if row[col.name] > datetime(9999, 12, 31).timestamp():
                        row[col.name] = datetime.fromtimestamp(row[col.name] / 1_000_000).strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        )
                    else:
                        try:
                            row[col.name] = datetime.fromtimestamp(row[col.name]).strftime("%Y-%m-%d %H:%M:%S.%f")
                        except ValueError:
                            error_message = (
                                f"Error casting timestamp for destination '{self.destination_id}' column '{col.name}'. "
                                f"Invalid timestamp value: {row[col.name]} ({type(row[col.name])}). "
                                "Consider using a transformation."
                            )
                            logger.error(error_message)
                            raise ValueError(error_message)
        return row

    @staticmethod
    def to_protobuf_serialization(TableRowClass: Type[Message], row: dict) -> bytes:
        """Convert a row to a Protobuf serialization."""
        record = ParseDict(row, TableRowClass())
        return record.SerializeToString()

    def load_to_bigquery_via_streaming(self, df_destination_records: pl.DataFrame) -> str:

        # TODO: for now no clustering keys
        clustering_keys = []

        # Create table if it doesnt exist
        schema = self.get_bigquery_schema()
        table = bigquery.Table(self.table_id, schema=schema)
        time_partitioning = TimePartitioning(
            field=self.config.time_partitioning.field, type_=self.config.time_partitioning.type
        )
        table.time_partitioning = time_partitioning

        # Override bigquery client with project's destination id
        if self.destination_id:
            project, dataset, table_name = self.destination_id.split(".")
            self.bq_client = bigquery.Client(project=project)

        table = self.bq_client.create_table(table, exists_ok=True)

        # Create the stream
        if self.destination_id:
            project, dataset, table_name = self.destination_id.split(".")
            write_client = bigquery_storage_v1.BigQueryWriteClient()
            parent = write_client.table_path(project, dataset, table_name)
        else:
            write_client = self.bq_storage_client
            parent = write_client.table_path(self.project_id, self.dataset_id, self.destination_id)

        stream_name = f"{parent}/_default"

        # Generating the protocol buffer representation of the message descriptor.
        proto_schema, TableRow = get_proto_schema_and_class(schema, clustering_keys)

        if self.config.unnest:
            serialized_rows = [
                self.to_protobuf_serialization(TableRowClass=TableRow, row=self.safe_cast_record_values(row))
                for row in df_destination_records["source_data"].str.json_decode(infer_schema_length=None).to_list()
            ]
        else:
            df_destination_records = df_destination_records.with_columns(
                pl.col("bizon_extracted_at").dt.strftime("%Y-%m-%d %H:%M:%S").alias("bizon_extracted_at"),
                pl.col("bizon_loaded_at").dt.strftime("%Y-%m-%d %H:%M:%S").alias("bizon_loaded_at"),
                pl.col("source_timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("source_timestamp"),
            )
            df_destination_records = df_destination_records.rename(
                {
                    "bizon_id": "_bizon_id",
                    "bizon_extracted_at": "_bizon_extracted_at",
                    "bizon_loaded_at": "_bizon_loaded_at",
                    "source_record_id": "_source_record_id",
                    "source_timestamp": "_source_timestamp",
                    "source_data": "_source_data",
                }
            )

            serialized_rows = [
                self.to_protobuf_serialization(TableRowClass=TableRow, row=row)
                for row in df_destination_records.iter_rows(named=True)
            ]

        results = []
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.append_rows_to_stream, write_client, stream_name, proto_schema, batch_rows)
                for batch_rows in self.batch(serialized_rows)
            ]
            for future in futures:
                results.append(future.result())

        assert all([r == "OK" for r in results]) is True, "Failed to append rows to stream"

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        self.load_to_bigquery_via_streaming(df_destination_records=df_destination_records)
        return True, ""

    def batch(self, iterable):
        """
        Yield successive batches respecting both row count and size limits.
        """
        current_batch = []
        current_batch_size = 0
        large_rows = []

        for item in iterable:
            # Estimate the size of the item (as JSON)
            item_size = len(str(item).encode("utf-8"))

            # If adding this item would exceed either limit, yield current batch and start new one
            if (
                len(current_batch) >= self.MAX_ROWS_PER_REQUEST
                or current_batch_size + item_size > self.MAX_REQUEST_SIZE_BYTES
            ):
                logger.debug(f"Yielding batch of {len(current_batch)} rows, size: {current_batch_size/1024/1024:.2f}MB")
                yield {"stream_batch": current_batch, "json_batch": large_rows}
                current_batch = []
                current_batch_size = 0
                large_rows = []

            if item_size > self.MAX_ROW_SIZE_BYTES:
                large_rows.append(item)
                logger.debug(f"Large row detected: {item_size} bytes")
            else:
                current_batch.append(item)
                current_batch_size += item_size

        # Yield the last batch
        if current_batch:
            logger.debug(
                f"Yielding streaming batch of {len(current_batch)} rows, size: {current_batch_size/1024/1024:.2f}MB"
            )
            logger.debug(f"Yielding large rows batch of {len(large_rows)} rows")
            yield {"stream_batch": current_batch, "json_batch": large_rows}
