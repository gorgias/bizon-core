from typing import Tuple

import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback
from bizon.source.config import SourceSyncModes

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: LoggerDestinationConfig,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        super().__init__(
            sync_metadata=sync_metadata,
            config=config,
            backend=backend,
            source_callback=source_callback,
            monitor=monitor,
        )

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        sync_mode_label = f"[{self.sync_metadata.sync_mode}]" if self.sync_metadata.sync_mode else ""
        for record in df_destination_records.iter_rows(named=True):
            logger.info(f"{sync_mode_label} {record['source_data']}")
        return True, ""

    def finalize(self) -> bool:
        """Finalize the sync - logs completion message based on sync mode."""
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
            logger.info("Logger destination: FULL_REFRESH sync completed")
        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
            logger.info("Logger destination: INCREMENTAL sync completed (records appended)")
        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM.value:
            logger.info("Logger destination: STREAM sync batch completed")
        return True
