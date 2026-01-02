from abc import ABC, abstractmethod
from typing import Dict, List

from loguru import logger

from bizon.alerting.models import AlertingConfig, AlertMethod, LogLevel


class AbstractAlert(ABC):
    def __init__(self, type: AlertMethod, config: AlertingConfig, log_levels: List[LogLevel] = [LogLevel.ERROR]):
        self.type = type
        self.config = config
        self.log_levels = log_levels

    @abstractmethod
    def handler(self, message: Dict) -> None:
        pass

    def add_handlers(self) -> None:
        levels = [level.value for level in self.log_levels]
        for level in levels:
            logger.add(self.handler, level=level, format="{message}")
