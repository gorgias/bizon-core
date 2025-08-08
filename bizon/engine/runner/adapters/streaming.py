import os
import time
from datetime import datetime
from typing import List

import polars as pl
import simplejson as json
from loguru import logger
from pytz import UTC

from bizon.common.models import BizonConfig, SyncMetadata
from bizon.destination.models import transform_to_df_destination_records
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.runner.config import RunnerStatus
from bizon.engine.runner.runner import AbstractRunner
from bizon.source.models import SourceRecord, source_record_schema


class StreamingRunner(AbstractRunner):
    def __init__(self, config: BizonConfig):
        super().__init__(config)

    @staticmethod
    def convert_source_records(records: List[SourceRecord]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [record.id for record in records],
                "data": [json.dumps(record.data, ensure_ascii=False) for record in records],
                "timestamp": [record.timestamp for record in records],
                "destination_id": [record.destination_id for record in records],
            },
            schema=source_record_schema,
        )

    @staticmethod
    def convert_to_destination_records(df_source_records: pl.DataFrame, extracted_at: datetime) -> pl.DataFrame:
        return transform_to_df_destination_records(df_source_records=df_source_records, extracted_at=extracted_at)

    def run(self) -> RunnerStatus:
        job = self.init_job(bizon_config=self.bizon_config, config=self.config)
        backend = self.get_backend(bizon_config=self.bizon_config)
        source = self.get_source(bizon_config=self.bizon_config, config=self.config)

        sync_metadata = SyncMetadata.from_bizon_config(job_id=job.id, config=self.bizon_config)
        monitor = self.get_monitoring_client(sync_metadata=sync_metadata, bizon_config=self.bizon_config)

        destination = self.get_destination(
            bizon_config=self.bizon_config,
            backend=backend,
            job_id=job.id,
            source_callback=None,
            monitor=monitor,
        )

        transform = self.get_transform(bizon_config=self.bizon_config)

        destination.buffer.buffer_size = 0  # force buffer to be flushed immediately
        iteration = 0

        while True:

            if source.config.max_iterations and iteration > source.config.max_iterations:
                logger.info(f"Max iterations {source.config.max_iterations} reached, terminating stream ...")
                break

            with monitor.trace(operation_name="bizon.stream.iteration"):
                source_iteration = source.get()

                destination_id_indexed_records = {}

                if len(source_iteration.records) == 0:
                    logger.info("No new records found, stopping iteration")
                    time.sleep(2)
                    monitor.track_pipeline_status(PipelineReturnStatus.SUCCESS)
                    iteration += 1
                    continue

                for record in source_iteration.records:
                    if destination_id_indexed_records.get(record.destination_id):
                        destination_id_indexed_records[record.destination_id].append(record)
                    else:
                        destination_id_indexed_records[record.destination_id] = [record]

                for destination_id, records in destination_id_indexed_records.items():
                    df_source_records = StreamingRunner.convert_source_records(records)

                    dsm_headers = monitor.track_source_iteration(records=records)

                    # Apply transformation
                    df_source_records = transform.apply_transforms(df_source_records=df_source_records)

                    df_destination_records = StreamingRunner.convert_to_destination_records(
                        df_source_records, datetime.now(tz=UTC)
                    )
                    # Override destination_id
                    destination.destination_id = destination_id
                    destination.write_or_buffer_records(
                        df_destination_records=df_destination_records,
                        iteration=iteration,
                        pagination=None,
                    )
                    monitor.track_records_synced(
                        num_records=len(df_destination_records),
                        destination_id=destination_id,
                        extra_tags={"destination_id": destination_id},
                        headers=dsm_headers,
                    )

                if os.getenv("ENVIRONMENT") == "production":
                    try:
                        source.commit()
                    except Exception as e:
                        logger.error(f"Error committing source: {e}")
                        monitor.track_pipeline_status(PipelineReturnStatus.ERROR)
                        return RunnerStatus(stream=PipelineReturnStatus.ERROR)

                iteration += 1

                monitor.track_pipeline_status(PipelineReturnStatus.SUCCESS)

        return RunnerStatus(stream=PipelineReturnStatus.SUCCESS)  # return when max iterations is reached
