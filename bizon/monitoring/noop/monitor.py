from bizon.common.models import BizonConfig
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.monitor import AbstractMonitor


class NoOpMonitor(AbstractMonitor):
    def __init__(self, pipeline_config: BizonConfig):
        super().__init__(pipeline_config)

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus) -> None:
        pass
