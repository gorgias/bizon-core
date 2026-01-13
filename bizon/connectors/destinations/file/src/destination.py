import os
import shutil
from typing import Tuple

import orjson
import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback
from bizon.source.config import SourceSyncModes

from .config import FileDestinationDetailsConfig


class FileDestination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: FileDestinationDetailsConfig,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        super().__init__(sync_metadata, config, backend, source_callback, monitor)
        self.config: FileDestinationDetailsConfig = config

    @property
    def file_path(self) -> str:
        """Main output file path."""
        return f"{self.destination_id}.json"

    @property
    def temp_file_path(self) -> str:
        """Temp file path for FULL_REFRESH mode."""
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
            return f"{self.destination_id}_temp.json"
        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
            return f"{self.destination_id}_incremental.json"
        return self.file_path

    @property
    def write_path(self) -> str:
        """Get the path to write to based on sync mode."""
        if self.sync_metadata.sync_mode in [
            SourceSyncModes.FULL_REFRESH.value,
            SourceSyncModes.INCREMENTAL.value,
        ]:
            return self.temp_file_path
        return self.file_path

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        if self.config.unnest:
            schema_keys = set([column.name for column in self.record_schemas[self.destination_id]])

            with open(self.write_path, "a") as f:
                for value in [orjson.loads(data) for data in df_destination_records["source_data"].to_list()]:
                    assert set(value.keys()) == schema_keys, "Keys do not match the schema"

                    # Unnest the source_data column
                    row = {}
                    for column in self.record_schemas[self.destination_id]:
                        row[column.name] = value[column.name]

                    f.write(f"{orjson.dumps(row).decode('utf-8')}\n")

        else:
            # Append mode for incremental, overwrite for full refresh on first write
            with open(self.write_path, "a") as f:
                for record in df_destination_records.iter_rows(named=True):
                    f.write(f"{orjson.dumps(record).decode('utf-8')}\n")

        return True, ""

    def finalize(self) -> bool:
        """Finalize the sync by moving temp file to main file based on sync mode."""
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
            # Replace main file with temp file
            if os.path.exists(self.temp_file_path):
                logger.info(f"File destination: Moving {self.temp_file_path} to {self.file_path}")
                shutil.move(self.temp_file_path, self.file_path)
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
            # Append temp file contents to main file
            if os.path.exists(self.temp_file_path):
                logger.info(f"File destination: Appending {self.temp_file_path} to {self.file_path}")
                with open(self.file_path, "a") as main_file:
                    with open(self.temp_file_path) as temp_file:
                        main_file.write(temp_file.read())
                os.remove(self.temp_file_path)
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM.value:
            # Direct writes, no finalization needed
            logger.info("File destination: STREAM sync batch completed")
            return True

        return True
