from enum import Enum

from pydantic import Field

from bizon.source.config import SourceConfig


class MetabaseStreams(str, Enum):
    """Available streams for the Metabase API."""

    USERS = "users"
    CARDS = "cards"
    DASHBOARDS = "dashboards"
    PERMISSIONS_GROUPS = "permissions_groups"


class MetabaseSourceConfig(SourceConfig):
    """Configuration for the Metabase source connector."""

    stream: MetabaseStreams

    base_url: str = Field(
        ...,
        description="Base URL of your Metabase instance (e.g., https://metabase.example.com)",
    )

    page_size: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Number of records per page for paginated endpoints (users). Default: 50",
    )
