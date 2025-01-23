import os

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
            f"name:{self.pipeline_config.name}",
            f"stream:{self.pipeline_config.source.stream}",
            f"source:{self.pipeline_config.source.name}",
            f"destination:{self.pipeline_config.destination.name}",
        ]

        self.pipeline_active_pipelines = "bizon_pipeline.active_pipelines"

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """

        statsd.increment(
            self.pipeline_monitor_status,
            tags=self.tags + [f"status:{pipeline_status}"],
        )
