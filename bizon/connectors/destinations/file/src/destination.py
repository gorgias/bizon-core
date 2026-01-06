from typing import Tuple

import orjson
import polars as pl

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback

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

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        if self.config.unnest:
            schema_keys = set([column.name for column in self.record_schemas[self.destination_id]])

            with open(f"{self.destination_id}.json", "a") as f:
                for value in [orjson.loads(data) for data in df_destination_records["source_data"].to_list()]:
                    assert set(value.keys()) == schema_keys, "Keys do not match the schema"

                    # Unnest the source_data column
                    row = {}
                    for column in self.record_schemas[self.destination_id]:
                        row[column.name] = value[column.name]

                    f.write(f"{orjson.dumps(row).decode('utf-8')}\n")

        else:
            df_destination_records.write_ndjson(f"{self.destination_id}.json")

        return True, ""
