from pydantic import BaseModel


class MonitoringConfig(BaseModel):
    datadog_agent_host: str
    datadog_agent_port: int = 8125
