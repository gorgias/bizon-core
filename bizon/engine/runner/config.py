from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from bizon.engine.pipeline.models import PipelineReturnStatus


class RunnerTypes(str, Enum):
    THREAD = "thread"
    PROCESS = "process"
    STREAM = "stream"


class LoggerLevel(str, Enum):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class RunnerFuturesConfig(BaseModel):
    max_workers: Optional[int] = Field(
        description="Number of workers to use for the runner",
        default=2,
    )
    consumer_start_delay: Optional[int] = Field(
        description="Duration in seconds to wait before starting the consumer thread",
        default=2,
    )
    is_alive_check_interval: Optional[int] = Field(
        description="Duration in seconds to wait between checking if the producer and consumer threads are still running",
        default=2,
    )


class RunnerConfig(BaseModel):

    type: RunnerTypes = Field(
        description="Runner to use for the pipeline",
        default=RunnerTypes.THREAD,
    )

    config: Optional[RunnerFuturesConfig] = Field(
        description="Runner configuration",
        default=RunnerFuturesConfig(),
    )

    log_level: LoggerLevel = Field(
        description="Logging level",
        default=LoggerLevel.INFO,
    )


class RunnerStatus(BaseModel):
    producer: PipelineReturnStatus
    consumer: PipelineReturnStatus

    @property
    def is_success(self):
        if self.producer == PipelineReturnStatus.SUCCESS and self.consumer == PipelineReturnStatus.SUCCESS:
            return True
        else:
            return False

    def to_string(self):
        return (
            f"Pipeline finished with status {'Success' if self.is_success else 'Failure'} "
            f"(Producer: {self.producer.value}, Consumer: {self.consumer.value})"
        )
