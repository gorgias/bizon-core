import os
from typing import Dict

from datadog import initialize, statsd
from loguru import logger

from bizon.common.models import BizonConfig
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.monitor import AbstractMonitor


class DatadogMonitor(AbstractMonitor):
    def __init__(self, pipeline_config: BizonConfig):
        super().__init__(pipeline_config)

        # In Kubernetes, set the host dynamically
        try:
            datadog_host_from_env_var = os.getenv(pipeline_config.monitoring.config.datadog_host_env_var)
            if datadog_host_from_env_var:
                initialize(
                    statsd_host=datadog_host_from_env_var,
                    statsd_port=pipeline_config.monitoring.config.datadog_agent_port,
                )
            else:
                initialize(
                    statsd_host=pipeline_config.monitoring.config.datadog_agent_host,
                    statsd_port=pipeline_config.monitoring.config.datadog_agent_port,
                )
        except Exception as e:
            logger.info(f"Failed to initialize Datadog agent: {e}")

        self.pipeline_monitor_status = "bizon_pipeline.status"
        self.tags = [
            f"pipeline_name:{self.pipeline_config.name}",
            f"pipeline_stream:{self.pipeline_config.source.stream}",
            f"pipeline_source:{self.pipeline_config.source.name}",
            f"pipeline_destination:{self.pipeline_config.destination.name}",
        ] + [f"{key}:{value}" for key, value in self.pipeline_config.monitoring.config.tags.items()]

        self.pipeline_active_pipelines = "bizon_pipeline.active_pipelines"
        self.pipeline_records_synced = "bizon_pipeline.records_synced"

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """

        statsd.increment(
            self.pipeline_monitor_status,
            tags=self.tags
            + [f"pipeline_status:{pipeline_status}"]
            + [f"{key}:{value}" for key, value in extra_tags.items()],
        )

    def track_records_synced(self, num_records: int, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the number of records synced in the pipeline.

        Args:
            num_records (int): Number of records synced in this batch
        """
        statsd.increment(
            self.pipeline_records_synced,
            value=num_records,
            tags=self.tags + [f"{key}:{value}" for key, value in extra_tags.items()],
        )
