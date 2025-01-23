from enum import Enum
from typing import Optional

from pydantic import BaseModel


class MonitorType(str, Enum):
    DATADOG = "datadog"


class DatadogConfig(BaseModel):
    datadog_agent_host: str
    datadog_agent_port: int = 8125


class MonitoringConfig(BaseModel):
    type: MonitorType
    config: Optional[DatadogConfig] = None
