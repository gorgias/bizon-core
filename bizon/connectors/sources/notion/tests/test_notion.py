"""
Quick test script for Notion source intermediate functions.

Usage:
    # Set your token
    export NOTION_TOKEN="your_notion_integration_token"

    # Run interactively
    python -i test_notion.py

    # Then test functions:
    >>> page = source.get_page("page-id-here")
    >>> blocks = source.fetch_blocks_recursively("page-id-here")
    >>> for b in blocks[:5]:
    ...     print(source._block_to_markdown(b))
"""

import os

from bizon.connectors.sources.notion.src.config import NotionSourceConfig, NotionStreams
from bizon.connectors.sources.notion.src.source import NotionSource
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.config import AuthConfig, AuthType


def create_notion_source(
    token: str = None,
    page_ids: list = None,
    database_ids: list = None,
    stream: NotionStreams = NotionStreams.BLOCKS,
) -> NotionSource:
    """Create a NotionSource instance for testing."""
    token = token or os.environ.get("NOTION_TOKEN")
    if not token:
        raise ValueError("Provide token or set NOTION_TOKEN environment variable")

    config = NotionSourceConfig(
        name="notion",
        stream=stream,
        page_ids=page_ids or [],
        database_ids=database_ids or [],
        authentication=AuthConfig(
            type=AuthType.BEARER,
            params=TokenAuthParams(token=token),
        ),
        init_pipeline=False,
        max_recursion_depth=30,
    )
    return NotionSource(config)


# ==================== HELPER FUNCTIONS ====================


def get_block(source: NotionSource, block_id: str) -> dict:
    """Fetch a single block by ID."""
    response = source.session.get(f"https://api.notion.com/v1/blocks/{block_id}")
    response.raise_for_status()
    return response.json()


def get_page_markdown(source: NotionSource, page_id: str) -> str:
    """Fetch all blocks from a page and return combined markdown."""
    blocks = source.fetch_blocks_recursively(page_id, source_page_id=page_id)
    lines = []
    for block in blocks:
        md = source._block_to_markdown(block)
        if md:
            # Add indentation based on depth
            indent = "  " * block.get("depth", 0)
            lines.append(f"{indent}{md}")
    return "\n".join(lines)


def inspect_blocks(source: NotionSource, page_id: str, max_blocks: int = 10):
    """Fetch and print block details for inspection."""
    blocks = source.fetch_blocks_recursively(page_id, source_page_id=page_id)
    print(f"Found {len(blocks)} blocks")
    for i, block in enumerate(blocks[:max_blocks]):
        print(f"\n--- Block {i} ({block.get('type')}) ---")
        print(f"ID: {block.get('id')}")
        print(f"Depth: {block.get('depth')}, Order: {block.get('page_order')}")
        print(f"Markdown: {source._block_to_markdown(block)}")


def list_pages_in_database(source: NotionSource, database_id: str) -> list:
    """List all page IDs in a database."""
    return source.get_pages_from_database(database_id, apply_filter=False)


# ==================== MAIN ====================

if __name__ == "__main__":
    # Create source if token is available
    token = os.environ.get("NOTION_TOKEN")
    if token:
        source = create_notion_source(token=token)
        print("NotionSource created and available as 'source'")
        print("\nAvailable functions:")
        print("  source.get_page(page_id)")
        print("  source.get_database(database_id)")
        print("  source.get_block_children(block_id)")
        print("  source.fetch_blocks_recursively(page_id)")
        print("  source._block_to_markdown(block)")
        print("  source.search()")
        print("\nHelper functions:")
        print("  get_block(source, block_id)")
        print("  get_page_markdown(source, page_id)")
        print("  inspect_blocks(source, page_id)")
        print("  list_pages_in_database(source, database_id)")
    else:
        print("Set NOTION_TOKEN env var or call:")
        print("  source = create_notion_source(token='your_token')")
