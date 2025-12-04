from typing import Any, List, Optional, Tuple

from loguru import logger
from requests import Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

from .config import NotionSourceConfig, NotionStreams

NOTION_API_VERSION = "2025-09-03"
BASE_URL = "https://api.notion.com/v1"


class NotionSource(AbstractSource):
    def __init__(self, config: NotionSourceConfig):
        super().__init__(config)
        self.config: NotionSourceConfig = config

    def get_session(self) -> Session:
        """Create a session with retry logic and required Notion headers."""
        session = Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.headers.update(
            {
                "Notion-Version": NOTION_API_VERSION,
                "Content-Type": "application/json",
            }
        )
        return session

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value in [AuthType.API_KEY, AuthType.BEARER]:
            return AuthBuilder.token(params=self.config.authentication.params)
        return None

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in NotionStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return NotionSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Test connection by listing users."""
        try:
            response = self.session.get(f"{BASE_URL}/users")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    # ==================== USERS STREAM ====================

    def get_users(self, pagination: dict = None) -> SourceIteration:
        """Fetch all users accessible to the integration."""
        params = {"page_size": self.config.page_size}
        if pagination and pagination.get("start_cursor"):
            params["start_cursor"] = pagination["start_cursor"]

        response = self.session.get(f"{BASE_URL}/users", params=params)
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=user["id"], data=user) for user in data.get("results", [])]

        next_pagination = {"start_cursor": data.get("next_cursor")} if data.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== DATABASES STREAM ====================

    def get_database(self, database_id: str) -> dict:
        """Fetch a single database by ID."""
        response = self.session.get(f"{BASE_URL}/databases/{database_id}")
        response.raise_for_status()
        return response.json()

    def get_databases(self, pagination: dict = None) -> SourceIteration:
        """Fetch databases for the configured database_ids."""
        if not self.config.database_ids:
            logger.warning("No database_ids configured, returning empty results")
            return SourceIteration(records=[], next_pagination={})

        # Track progress through database_ids list
        if pagination:
            remaining_ids = pagination.get("remaining_ids", [])
        else:
            remaining_ids = list(self.config.database_ids)

        if not remaining_ids:
            return SourceIteration(records=[], next_pagination={})

        # Process one database at a time
        database_id = remaining_ids[0]
        remaining_ids = remaining_ids[1:]

        try:
            database_data = self.get_database(database_id)
            records = [SourceRecord(id=database_data["id"], data=database_data)]
        except Exception as e:
            logger.error(f"Failed to fetch database {database_id}: {e}")
            records = []

        next_pagination = {"remaining_ids": remaining_ids} if remaining_ids else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== DATA SOURCES STREAM ====================

    def get_data_sources(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch data sources from databases.
        In the 2025-09-03 API, databases contain a data_sources array.
        """
        if not self.config.database_ids:
            logger.warning("No database_ids configured, returning empty results")
            return SourceIteration(records=[], next_pagination={})

        if pagination:
            remaining_ids = pagination.get("remaining_ids", [])
        else:
            remaining_ids = list(self.config.database_ids)

        if not remaining_ids:
            return SourceIteration(records=[], next_pagination={})

        database_id = remaining_ids[0]
        remaining_ids = remaining_ids[1:]

        records = []
        try:
            database_data = self.get_database(database_id)
            data_sources = database_data.get("data_sources", [])

            for ds in data_sources:
                # Enrich data source with parent database info
                ds_record = {
                    **ds,
                    "parent_database_id": database_id,
                    "parent_database_title": self._extract_title(database_data),
                }
                records.append(SourceRecord(id=ds["id"], data=ds_record))

        except Exception as e:
            logger.error(f"Failed to fetch data sources from database {database_id}: {e}")

        next_pagination = {"remaining_ids": remaining_ids} if remaining_ids else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== PAGES STREAM ====================

    def query_data_source(self, data_source_id: str, start_cursor: str = None) -> dict:
        """Query a data source for its pages."""
        payload = {"page_size": self.config.page_size}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        response = self.session.post(f"{BASE_URL}/data_sources/{data_source_id}/query", json=payload)
        response.raise_for_status()
        return response.json()

    def get_page(self, page_id: str) -> dict:
        """Fetch a single page by ID."""
        response = self.session.get(f"{BASE_URL}/pages/{page_id}")
        response.raise_for_status()
        return response.json()

    def get_pages(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch pages from data sources (querying databases) and/or specific page IDs.
        """
        records = []

        if pagination:
            # Continue previous pagination state
            remaining_data_sources = pagination.get("remaining_data_sources", [])
            current_data_source = pagination.get("current_data_source")
            data_source_cursor = pagination.get("data_source_cursor")
            remaining_page_ids = pagination.get("remaining_page_ids", [])
            data_sources_loaded = pagination.get("data_sources_loaded", False)
        else:
            remaining_data_sources = []
            current_data_source = None
            data_source_cursor = None
            remaining_page_ids = list(self.config.page_ids)
            data_sources_loaded = False

        # First, load all data sources from databases if not done
        if not data_sources_loaded and self.config.database_ids:
            for db_id in self.config.database_ids:
                try:
                    db_data = self.get_database(db_id)
                    for ds in db_data.get("data_sources", []):
                        remaining_data_sources.append(ds["id"])
                except Exception as e:
                    logger.error(f"Failed to get data sources from database {db_id}: {e}")
            data_sources_loaded = True

        # Process current data source if we have one with a cursor
        if current_data_source and data_source_cursor:
            try:
                result = self.query_data_source(current_data_source, data_source_cursor)
                for page in result.get("results", []):
                    records.append(SourceRecord(id=page["id"], data=page))

                if result.get("has_more"):
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": current_data_source,
                            "data_source_cursor": result.get("next_cursor"),
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )
            except Exception as e:
                logger.error(f"Failed to query data source {current_data_source}: {e}")

        # Process next data source
        if remaining_data_sources:
            ds_id = remaining_data_sources[0]
            remaining_data_sources = remaining_data_sources[1:]

            try:
                result = self.query_data_source(ds_id)
                for page in result.get("results", []):
                    records.append(SourceRecord(id=page["id"], data=page))

                if result.get("has_more"):
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": ds_id,
                            "data_source_cursor": result.get("next_cursor"),
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )

                # If there are more data sources, continue
                if remaining_data_sources:
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": None,
                            "data_source_cursor": None,
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )
            except Exception as e:
                logger.error(f"Failed to query data source {ds_id}: {e}")
                # Continue with remaining data sources
                if remaining_data_sources:
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": None,
                            "data_source_cursor": None,
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )

        # Process individual page IDs
        if remaining_page_ids:
            page_id = remaining_page_ids[0]
            remaining_page_ids = remaining_page_ids[1:]

            try:
                page_data = self.get_page(page_id)
                records.append(SourceRecord(id=page_data["id"], data=page_data))
            except Exception as e:
                logger.error(f"Failed to fetch page {page_id}: {e}")

            if remaining_page_ids:
                return SourceIteration(
                    records=records,
                    next_pagination={
                        "remaining_data_sources": [],
                        "current_data_source": None,
                        "data_source_cursor": None,
                        "remaining_page_ids": remaining_page_ids,
                        "data_sources_loaded": True,
                    },
                )

        return SourceIteration(records=records, next_pagination={})

    # ==================== BLOCKS STREAM ====================

    def get_block_children(self, block_id: str, start_cursor: str = None) -> dict:
        """Fetch children blocks of a block."""
        params = {"page_size": self.config.page_size}
        if start_cursor:
            params["start_cursor"] = start_cursor

        response = self.session.get(f"{BASE_URL}/blocks/{block_id}/children", params=params)
        response.raise_for_status()
        return response.json()

    def fetch_blocks_recursively(self, block_id: str) -> List[dict]:
        """
        Fetch all blocks under a block_id recursively.
        Returns a flat list of all blocks with parent_id added.
        """
        all_blocks = []
        cursor = None

        while True:
            result = self.get_block_children(block_id, cursor)

            for block in result.get("results", []):
                block["parent_block_id"] = block_id
                all_blocks.append(block)

                # Recursively fetch children if block has children and recursive is enabled
                if block.get("has_children") and self.config.fetch_blocks_recursively:
                    child_blocks = self.fetch_blocks_recursively(block["id"])
                    all_blocks.extend(child_blocks)

            if result.get("has_more"):
                cursor = result.get("next_cursor")
            else:
                break

        return all_blocks

    def get_blocks(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch blocks from pages (from databases and page_ids).
        Blocks are fetched recursively if fetch_blocks_recursively is True.
        """
        if pagination:
            page_ids_to_process = pagination.get("page_ids_to_process", [])
            pages_loaded = pagination.get("pages_loaded", False)
        else:
            page_ids_to_process = []
            pages_loaded = False

        # First, collect all page IDs from databases and config
        if not pages_loaded:
            # Add configured page_ids
            page_ids_to_process.extend(self.config.page_ids)

            # Collect pages from databases
            for db_id in self.config.database_ids:
                try:
                    db_data = self.get_database(db_id)
                    for ds in db_data.get("data_sources", []):
                        cursor = None
                        while True:
                            result = self.query_data_source(ds["id"], cursor)
                            for page in result.get("results", []):
                                page_ids_to_process.append(page["id"])
                            if result.get("has_more"):
                                cursor = result.get("next_cursor")
                            else:
                                break
                except Exception as e:
                    logger.error(f"Failed to collect pages from database {db_id}: {e}")

            pages_loaded = True

        if not page_ids_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process one page at a time
        page_id = page_ids_to_process[0]
        page_ids_to_process = page_ids_to_process[1:]

        records = []
        try:
            blocks = self.fetch_blocks_recursively(page_id)
            for block in blocks:
                block["source_page_id"] = page_id
                records.append(SourceRecord(id=block["id"], data=block))
            logger.info(f"Fetched {len(blocks)} blocks from page {page_id}")
        except Exception as e:
            logger.error(f"Failed to fetch blocks from page {page_id}: {e}")

        next_pagination = (
            {"page_ids_to_process": page_ids_to_process, "pages_loaded": True} if page_ids_to_process else {}
        )

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== HELPERS ====================

    def _extract_title(self, database_data: dict) -> str:
        """Extract plain text title from database object."""
        title_parts = database_data.get("title", [])
        return "".join(part.get("plain_text", "") for part in title_parts)

    # ==================== MAIN DISPATCH ====================

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == NotionStreams.USERS:
            return self.get_users(pagination)
        elif self.config.stream == NotionStreams.DATABASES:
            return self.get_databases(pagination)
        elif self.config.stream == NotionStreams.DATA_SOURCES:
            return self.get_data_sources(pagination)
        elif self.config.stream == NotionStreams.PAGES:
            return self.get_pages(pagination)
        elif self.config.stream == NotionStreams.BLOCKS:
            return self.get_blocks(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Notion")
