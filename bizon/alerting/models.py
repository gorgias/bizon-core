from enum import Enum
from typing import Union

from pydantic import BaseModel

from bizon.alerting.slack.config import SlackConfig


class AlertMethod(str, Enum):
    """Alerting methods"""

    SLACK = "slack"


class AlertingConfig(BaseModel):
    """Alerting configuration model"""

    type: AlertMethod
    config: Union[SlackConfig]
