from abc import ABC, abstractmethod
from typing import Dict

from bizon.common.models import BizonConfig
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import MonitorType


class AbstractMonitor(ABC):
    def __init__(self, pipeline_config: BizonConfig):
        self.pipeline_config = pipeline_config
        # Initialize the monitor

    @abstractmethod
    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """
        pass

    def track_records_synced(self, num_records: int, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the number of records synced in the pipeline.
        """
        pass


class MonitorFactory:
    @staticmethod
    def get_monitor(pipeline_config: BizonConfig) -> AbstractMonitor:
        if pipeline_config.monitoring is None:
            from bizon.monitoring.noop.monitor import NoOpMonitor

            return NoOpMonitor(pipeline_config)

        if pipeline_config.monitoring.type == MonitorType.DATADOG:
            from bizon.monitoring.datadog.monitor import DatadogMonitor

            return DatadogMonitor(pipeline_config)
