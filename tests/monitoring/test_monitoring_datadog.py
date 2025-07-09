from bizon.common.models import SyncMetadata
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import DatadogConfig, MonitoringConfig, MonitorType
from bizon.monitoring.datadog.monitor import DatadogMonitor
from bizon.monitoring.monitor import MonitorFactory
from bizon.monitoring.noop.monitor import NoOpMonitor

sync_metadata = SyncMetadata(
    job_id="123",
    name="pipeline_test",
    source_name="source_test",
    stream_name="stream_test",
    destination_name="destination_test",
    destination_alias="destination_test",
    sync_mode="full_refresh",
)


def test_datadog_monitor():
    dd_monitor = MonitorFactory.get_monitor(
        sync_metadata=sync_metadata,
        monitoring_config=MonitoringConfig(
            type=MonitorType.DATADOG, config=DatadogConfig(datadog_agent_host="localhost", datadog_agent_port=8125)
        ),
    )

    assert type(dd_monitor) == DatadogMonitor
    assert dd_monitor.monitoring_config.type == MonitorType.DATADOG


def test_datadog_track_records_synced():
    dd_monitor = MonitorFactory.get_monitor(
        sync_metadata=sync_metadata,
        monitoring_config=MonitoringConfig(
            type=MonitorType.DATADOG, config=DatadogConfig(datadog_agent_host="localhost", datadog_agent_port=8125)
        ),
    )
    dd_monitor.track_records_synced(num_records=10, destination_id="123")


def test_datadog_track_pipeline_status():
    dd_monitor = MonitorFactory.get_monitor(
        sync_metadata=sync_metadata,
        monitoring_config=MonitoringConfig(
            type=MonitorType.DATADOG, config=DatadogConfig(datadog_agent_host="localhost", datadog_agent_port=8125)
        ),
    )
    dd_monitor.track_pipeline_status(PipelineReturnStatus.SUCCESS)


def test_datadog_track_large_records_synced():
    dd_monitor = MonitorFactory.get_monitor(
        sync_metadata=sync_metadata,
        monitoring_config=MonitoringConfig(
            type=MonitorType.DATADOG, config=DatadogConfig(datadog_agent_host="localhost", datadog_agent_port=8125)
        ),
    )
    dd_monitor.track_large_records_synced(num_records=10)


def test_no_op_monitor():
    no_op_monitor = MonitorFactory.get_monitor(sync_metadata=sync_metadata, monitoring_config=None)

    assert type(no_op_monitor) == NoOpMonitor
    assert no_op_monitor.monitoring_config is None
