from enum import Enum
from typing import Literal

from pydantic import Field

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class FileFormat(str, Enum):
    JSON = "json"


class FileDestinationDetailsConfig(AbstractDestinationDetailsConfig):
    format: FileFormat = Field(default=FileFormat.JSON, description="Format of the file")


class FileDestinationConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.FILE]
    alias: str = "file"
    config: FileDestinationDetailsConfig
