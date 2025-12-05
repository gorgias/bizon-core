from enum import Enum
from typing import List, Optional

from pydantic import Field

from bizon.source.config import SourceConfig


class NotionStreams(str, Enum):
    DATABASES = "databases"
    DATA_SOURCES = "data_sources"
    PAGES = "pages"
    BLOCKS = "blocks"
    BLOCKS_MARKDOWN = "blocks_markdown"
    USERS = "users"
    # Streams that fetch all accessible content (no database_ids/page_ids required)
    ALL_PAGES = "all_pages"
    ALL_DATABASES = "all_databases"
    ALL_BLOCKS_MARKDOWN = "all_blocks_markdown"


class NotionSourceConfig(SourceConfig):
    stream: NotionStreams

    database_ids: List[str] = Field(
        default_factory=list,
        description="List of Notion database IDs to fetch. Required for databases, data_sources, pages, and blocks streams.",
    )
    page_ids: List[str] = Field(
        default_factory=list,
        description="List of Notion page IDs to fetch. Used for pages and blocks streams.",
    )
    fetch_blocks_recursively: bool = Field(
        default=True,
        description="Whether to fetch nested blocks recursively. Only applies to blocks stream.",
    )
    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Number of results per page (max 100)",
    )
    max_workers: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of concurrent workers for fetching blocks. Keep low to respect rate limits.",
    )
