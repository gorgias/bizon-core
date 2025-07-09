from enum import Enum
from typing import Dict, Optional

from pydantic import BaseModel, Field


class MonitorType(str, Enum):
    DATADOG = "datadog"


class BaseMonitoringConfig(BaseModel):
    enable_tracing: bool = Field(default=False, description="Enable tracing for the monitor")


class DatadogConfig(BaseMonitoringConfig):
    datadog_agent_host: Optional[str] = None
    datadog_host_env_var: Optional[str] = None
    datadog_agent_port: int = 8125
    tags: Optional[Dict[str, str]] = Field(default={}, description="Key-value pairs to add to the monitor as tags")

    @property
    def host_is_configured(self) -> bool:
        return bool(self.datadog_agent_host or self.datadog_host_env_var)

    def __init__(self, **data):
        super().__init__(**data)
        if not self.host_is_configured:
            raise ValueError("Either datadog_agent_host or datadog_host_env_var must be specified")

    class Config:
        extra = "forbid"


class MonitoringConfig(BaseMonitoringConfig):
    type: MonitorType
    config: Optional[DatadogConfig] = None

    class Config:
        extra = "forbid"
