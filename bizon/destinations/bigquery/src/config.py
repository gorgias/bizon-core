from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class TimePartitioning(str, Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"


class BigQueryColumnType(str, Enum):
    BOOLEAN = "BOOLEAN"
    BYTES = "BYTES"
    DATE = "DATE"
    DATETIME = "DATETIME"
    FLOAT = "FLOAT"
    GEOGRAPHY = "GEOGRAPHY"
    INTEGER = "INTEGER"
    RECORD = "RECORD"
    STRING = "STRING"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"


class BigQueryColumnMode(str, Enum):
    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"


class BigQueryColumn(BaseModel):
    name: str = Field(..., description="Name of the column")
    type: BigQueryColumnType = Field(..., description="Type of the column")
    mode: BigQueryColumnMode = Field(..., description="Mode of the column")
    description: Optional[str] = Field(..., description="Description of the column")


class BigQueryAuthentication(BaseModel):
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryConfigDetails(AbstractDestinationDetailsConfig):

    # Table details
    project_id: str = Field(..., description="BigQuery Project ID")
    dataset_id: str = Field(..., description="BigQuery Dataset ID")
    table_id: Optional[str] = Field(
        default=None,
        description="Table ID, if not provided it will be inferred from source name",
    )

    dataset_location: str = Field(default="US", description="BigQuery Dataset location")

    # GCS Buffer
    gcs_buffer_bucket: str = Field(..., description="GCS Buffer bucket")
    gcs_buffer_format: GCSBufferFormat = Field(default=GCSBufferFormat.PARQUET, description="GCS Buffer format")

    # Time partitioning
    time_partitioning: TimePartitioning = Field(
        default=TimePartitioning.DAY, description="BigQuery Time partitioning type"
    )

    # Schema for unnesting
    record_schema: Optional[list[BigQueryColumn]] = Field(
        default=None, description="Schema for the records. Required if unnest is set to true."
    )

    authentication: Optional[BigQueryAuthentication] = None


class BigQueryConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY]
    buffer_size: Optional[int] = 2000
    config: BigQueryConfigDetails
