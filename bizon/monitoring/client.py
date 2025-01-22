import logging

from datadog import initialize, statsd

from bizon.common.models import BizonConfig
from bizon.engine.pipeline.models import PipelineReturnStatus


class Monitor:
    def __init__(self, pipeline_config: BizonConfig):
        self.pipeline_config = pipeline_config

        # In Kubernetes, set the host dynamically
        try:
            initialize(
                statsd_host=pipeline_config.monitoring.datadog_agent_host,
                statsd_port=pipeline_config.monitoring.datadog_agent_port,
            )
        except Exception as e:
            logging.info(f"Failed to initialize Datadog agent: {e}")

        self.pipeline_monitor_status = "bizon_pipeline.status"
        self.tags = [
            f"name:{self.pipeline_config.name}",
            f"stream:{self.pipeline_config.source.stream}",
            f"source:{self.pipeline_config.source.name}",
            f"destination:{self.pipeline_config.destination.name}",
        ]

        self.pipeline_active_pipelines = "bizon_pipeline.active_pipelines"

    def report_pipeline_status(self, pipeline_status: PipelineReturnStatus) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """

        statsd.increment(
            self.pipeline_monitor_status,
            tags=self.tags + [f"status:{pipeline_status}"],
        )
