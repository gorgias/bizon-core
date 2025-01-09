from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel

from bizon.alerting.slack.config import SlackConfig


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AlertMethod(str, Enum):
    """Alerting methods"""

    SLACK = "slack"


class AlertingConfig(BaseModel):
    """Alerting configuration model"""

    type: AlertMethod
    log_levels: Optional[List[LogLevel]] = [LogLevel.ERROR]
    config: Union[SlackConfig]
