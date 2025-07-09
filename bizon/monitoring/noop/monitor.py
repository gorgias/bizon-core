from bizon.common.models import SyncMetadata
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import MonitoringConfig
from bizon.monitoring.monitor import AbstractMonitor


class NoOpMonitor(AbstractMonitor):
    def __init__(self, sync_metadata: SyncMetadata, monitoring_config: MonitoringConfig):
        super().__init__(sync_metadata, monitoring_config)

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus) -> None:
        pass
