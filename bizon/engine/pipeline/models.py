from enum import Enum


class PipelineReturnStatus(str, Enum):
    """Producer error types"""

    SUCCESS = "success"
    ERROR = "error"
    KILLED_BY_RUNNER = "killed_by_runner"
    QUEUE_ERROR = "queue_error"
    SOURCE_ERROR = "source_error"
    BACKEND_ERROR = "backend_error"
    TRANSFORM_ERROR = "transform_error"
    DESTINATION_ERROR = "destination_error"
