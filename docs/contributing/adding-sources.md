# Adding a New Source Connector

Sources extract data from APIs and produce `SourceRecord` objects. Bizon **auto-discovers** sources via AST parsing - no registration needed!

## Directory Structure

```
bizon/connectors/sources/{source_name}/
├── config/
│   └── {source_name}.example.yml   # Example config for testing
└── src/
    ├── __init__.py                 # Empty file (required)
    ├── config.py                   # Optional: separate config classes
    └── source.py                   # Main implementation
```

## Required Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `streams()` | `List[str]` | Available stream names |
| `get_config_class()` | `SourceConfig` | Config class for this source |
| `get_authenticator()` | `AuthBase` or `None` | Auth handler |
| `check_connection()` | `Tuple[bool, str\|None]` | Test connectivity |
| `get_total_records_count()` | `int\|None` | Total records (optional) |
| `get(pagination)` | `SourceIteration` | Fetch records |

## Minimal Example

```python
# bizon/connectors/sources/myapi/src/source.py
from typing import List, Tuple
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class MyApiSourceConfig(SourceConfig):
    # stream is inherited from SourceConfig
    pass


class MyApiSource(AbstractSource):
    def __init__(self, config: MyApiSourceConfig):
        super().__init__(config)
        self.config = config

    @staticmethod
    def streams() -> List[str]:
        return ["users", "orders"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return MyApiSourceConfig

    def get_authenticator(self) -> AuthBase:
        return AuthBuilder.token(
            params=TokenAuthParams(token=self.config.authentication.params.token)
        )

    def check_connection(self) -> Tuple[bool, str | None]:
        # Test API connectivity
        return True, None

    def get_total_records_count(self) -> int | None:
        return None  # Unknown

    def get(self, pagination: dict = None) -> SourceIteration:
        # Fetch from API
        cursor = pagination.get("cursor") if pagination else None

        # Make API call (implement your logic here)
        response = self._fetch_page(cursor)

        # Parse response
        records = [
            SourceRecord(id=item["id"], data=item)
            for item in response.get("items", [])
        ]

        # Handle pagination - CRITICAL: return {} when done
        next_cursor = response.get("next_cursor")
        next_pagination = {"cursor": next_cursor} if next_cursor else {}

        return SourceIteration(
            records=records,
            next_pagination=next_pagination,
        )

    def _fetch_page(self, cursor: str = None) -> dict:
        # Your API call implementation
        pass
```

## Example Config

```yaml
# bizon/connectors/sources/myapi/config/myapi.example.yml
name: myapi_to_logger

source:
  name: myapi
  stream: users
  authentication:
    type: api_key
    params:
      token: <YOUR_TOKEN>

destination:
  name: logger
  config:
    dummy: dummy
```

## Pagination Patterns

| Pattern | `next_pagination` Example |
|---------|--------------------------|
| Cursor-based | `{"cursor": "abc123"}` |
| Offset-based | `{"offset": 100}` |
| Page-based | `{"page": 2}` |
| URL-based | `{"next_url": "https://..."}` |
| **Done** | `{}` (empty dict!) |

## Verification

```bash
# Format code
uv run ruff format bizon/connectors/sources/myapi

# Run tests
uv run pytest tests/connectors/sources/myapi -v

# Verify discovery
uv run bizon source list            # Should show 'myapi'
uv run bizon stream list myapi      # Should show your streams

# Test end-to-end
uv run bizon run bizon/connectors/sources/myapi/config/myapi.example.yml
```

## Checklist

- [ ] `src/__init__.py` exists (can be empty)
- [ ] Config class extends `SourceConfig`
- [ ] Source class extends `AbstractSource`
- [ ] All required methods implemented
- [ ] `next_pagination` returns `{}` when no more pages
- [ ] Each record has unique `id` field
- [ ] Example config created
- [ ] Tests pass

## Incremental Sync Support

To support incremental sync, implement the `get_records_after()` method.

### Required Method

| Method | Return Type | Description |
|--------|-------------|-------------|
| `get_records_after(source_state, pagination)` | `SourceIteration` | Fetch records updated after `source_state.last_run` |

### SourceIncrementalState Model

```python
from bizon.source.models import SourceIncrementalState

class SourceIncrementalState(BaseModel):
    last_run: datetime      # Timestamp of last successful sync (from previous job's created_at)
    state: dict = {}        # Optional additional state (currently unused)
    cursor_field: str|None  # Field name configured by user (e.g., "updated_at")
```

### Implementation Example

```python
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Fetch records updated after source_state.last_run."""

    # Convert datetime to API format (usually ISO string)
    last_run_iso = source_state.last_run.isoformat()

    # Build request with timestamp filter
    params = {
        "updated_after": last_run_iso,  # API-specific param name
        "page_size": self.config.page_size,
    }
    if pagination and pagination.get("cursor"):
        params["cursor"] = pagination["cursor"]

    response = self.session.get(f"{BASE_URL}/records", params=params)
    response.raise_for_status()
    data = response.json()

    records = [
        SourceRecord(id=r["id"], data=r)
        for r in data.get("results", [])
    ]

    # Pagination - same pattern as get()
    next_pagination = {"cursor": data["next_cursor"]} if data.get("has_more") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

### Client-Side Filtering (When API Lacks Timestamp Filter)

Some APIs don't support timestamp filtering. In this case, fetch all records and filter client-side:

```python
def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Fetch all records, filter by timestamp client-side."""

    # Use regular get() to fetch records
    iteration = self.get(pagination)

    # Filter records where timestamp > last_run
    filtered_records = []
    for record in iteration.records:
        record_timestamp = record.data.get(source_state.cursor_field)
        if record_timestamp and record_timestamp > source_state.last_run.isoformat():
            filtered_records.append(record)

    return SourceIteration(
        records=filtered_records,
        next_pagination=iteration.next_pagination,
    )
```

### Multi-Stream Dispatch

For sources with multiple streams, create stream-specific methods and dispatch:

```python
def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Dispatch to stream-specific incremental method."""
    stream = self.config.stream

    if stream == MyStreams.USERS:
        return self.get_users_after(source_state, pagination)
    elif stream == MyStreams.ORDERS:
        return self.get_orders_after(source_state, pagination)
    else:
        # Fallback: use regular get() for unsupported streams
        logger.warning(f"Stream {stream} doesn't support incremental, using full refresh")
        return self.get(pagination)
```

### Example Config (YAML)

```yaml
source:
  name: myapi
  stream: users
  sync_mode: incremental
  cursor_field: updated_at  # Field name in your data
  authentication:
    type: api_key
    params:
      token: <YOUR_TOKEN>
```

### Testing Incremental Sync

```bash
# First run - fetches all data (no previous job)
uv run bizon run config.yml

# Edit a record in the source system...

# Second run - only fetches records updated after first run
uv run bizon run config.yml
```

### Incremental Sync Checklist

- [ ] Implement `get_records_after(source_state, pagination)` method
- [ ] Handle `source_state.last_run` as datetime
- [ ] Use `source_state.cursor_field` if needed for client-side filtering
- [ ] Return empty `next_pagination={}` when done
- [ ] Test with two consecutive runs
- [ ] Create example config with `sync_mode: incremental`

## Advanced Patterns

For authentication types, pagination strategies, and production patterns, see:
- [AI Connector Guide](../ai-connector-guide.md) - Templates and decision trees
- [Reference Connector](../reference-connector.md) - Fully annotated example
