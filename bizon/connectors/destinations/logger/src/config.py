from typing import Literal, Optional

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class LoggerDestinationConfig(AbstractDestinationDetailsConfig):
    dummy: Optional[str] = "bizon"


class LoggerConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.LOGGER]
    alias: str = "logger"
    config: LoggerDestinationConfig
