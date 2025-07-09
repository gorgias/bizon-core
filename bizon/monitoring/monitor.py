from abc import ABC, abstractmethod
from typing import Dict, List, Union

from bizon.common.models import SyncMetadata
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import MonitoringConfig, MonitorType
from bizon.source.models import SourceRecord


class AbstractMonitor(ABC):
    def __init__(self, sync_metadata: SyncMetadata, monitoring_config: MonitoringConfig):
        self.sync_metadata = sync_metadata
        self.monitoring_config = monitoring_config

    @abstractmethod
    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """
        pass

    def track_source_iteration(self, records: List[SourceRecord], headers: Dict[str, str] = {}) -> None:
        """
        Run a process that tracks the source iteration.
        """
        pass

    def track_records_synced(
        self, num_records: int, destination_id: str, extra_tags: Dict[str, str] = {}, headers: Dict[str, str] = {}
    ) -> None:
        """
        Track the number of records synced in the pipeline.
        """
        pass

    def trace(self, operation_name: str, resource: str = None, extra_tags: Dict[str, str] = None):
        """
        Create a trace span for monitoring.

        Args:
            operation_name (str): The name of the operation being traced
            resource (str): The resource being operated on (e.g., topic name, table name)
            extra_tags (Dict[str, str]): Additional tags for the trace

        Returns:
            A context manager that can be used with 'with' statement
        """
        pass

    def track_large_records_synced(self, num_records: int, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the number of large records synced in the destination system. This aims at helping to identify the source of the large records.
        """
        pass


class MonitorFactory:
    @staticmethod
    def get_monitor(sync_metadata: SyncMetadata, monitoring_config: Union[MonitoringConfig, None]) -> AbstractMonitor:
        if monitoring_config is None:
            from bizon.monitoring.noop.monitor import NoOpMonitor

            return NoOpMonitor(sync_metadata, monitoring_config)

        if monitoring_config.type == MonitorType.DATADOG:
            from bizon.monitoring.datadog.monitor import DatadogMonitor

            return DatadogMonitor(sync_metadata, monitoring_config)
