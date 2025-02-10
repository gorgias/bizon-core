from typing import Literal, Optional

from pydantic import Field

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class FileDestinationDetailsConfig(AbstractDestinationDetailsConfig):
    dummy: str = "dummy"


class FileDestinationConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.FILE]
    config: FileDestinationDetailsConfig
