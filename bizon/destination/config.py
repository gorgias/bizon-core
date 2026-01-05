from abc import ABC
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    BIGQUERY_STREAMING = "bigquery_streaming"
    BIGQUERY_STREAMING_V2 = "bigquery_streaming_v2"
    LOGGER = "logger"
    FILE = "file"


class DestinationColumn(BaseModel, ABC):
    name: str = Field(..., description="Name of the column")
    type: str = Field(..., description="Type of the column")
    description: Optional[str] = Field(None, description="Description of the column")


class RecordSchemaConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    destination_id: str = Field(..., description="Destination ID")
    record_schema: list[DestinationColumn] = Field(..., description="Record schema")


class AbstractDestinationDetailsConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    buffer_size: int = Field(
        default=50,
        description="Buffer size in Mb for the destination. Set to 0 to disable and write directly to the destination.",
    )

    buffer_flush_timeout: int = Field(
        default=600,
        description="Maximum time in seconds for buffering after which the records will be written to the destination. Set to 0 to deactivate the timeout buffer check.",  # noqa
    )

    max_concurrent_threads: int = Field(
        default=10,
        description="Maximum number of concurrent threads to use for writing to the destination.",
    )

    record_schemas: Optional[list[RecordSchemaConfig]] = Field(
        default=None, description="Schemas for the records. Required if unnest is set to true."
    )

    unnest: bool = Field(
        default=False,
        description="Unnest the data before writing to the destination. Schema should be provided in the model_config.",
    )

    authentication: Optional[BaseModel] = Field(
        description="Authentication configuration for the destination, if needed", default=None
    )

    destination_id: Optional[str] = Field(
        description="Destination ID, identifier to use to store the records in the destination", default=None
    )

    @field_validator("unnest", mode="before")
    def validate_record_schema_if_unnest(cls, value, values):
        if bool(value) and not values.data.get("record_schemas", []):
            raise ValueError("At least one `record_schemas` must be provided if `unnest` is set to True.")
        return value


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    alias: str = Field(
        ...,
        description="Alias of the destination, used for tracking the system name (ie bigquery for bigquery_streaming)",
    )
    config: AbstractDestinationDetailsConfig = Field(..., description="Configuration for the destination")
