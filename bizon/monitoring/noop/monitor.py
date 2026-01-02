from contextlib import contextmanager
from typing import Dict

from bizon.common.models import SyncMetadata
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import MonitoringConfig
from bizon.monitoring.monitor import AbstractMonitor


class NoOpMonitor(AbstractMonitor):
    def __init__(self, sync_metadata: SyncMetadata, monitoring_config: MonitoringConfig):
        super().__init__(sync_metadata, monitoring_config)

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus) -> None:
        pass

    @contextmanager
    def trace(self, operation_name: str, resource: str = None, extra_tags: Dict[str, str] = None):
        """
        No-op trace implementation.

        Args:
            operation_name (str): The name of the operation being traced
            resource (str): The resource being operated on (e.g., topic name, table name)
            extra_tags (Dict[str, str]): Additional tags for the trace

        Yields:
            None (no-op implementation)
        """
        yield None
