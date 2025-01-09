from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List

from loguru import logger

from bizon.alerting.models import AlertingConfig, AlertMethod


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AbstractAlert(ABC):

    def __init__(self, type: AlertMethod, config: AlertingConfig):
        self.type = type
        self.config = config

    @abstractmethod
    def handler(self, message: Dict) -> None:
        pass

    def add_handlers(self, levels: List[LogLevel] = [LogLevel.ERROR]) -> None:
        for level in levels:
            logger.add(self.handler, level=level, format="{message}")
