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

## Advanced Patterns

For authentication types, pagination strategies, and production patterns, see:
- [AI Connector Guide](../ai-connector-guide.md) - Templates and decision trees
- [Reference Connector](../reference-connector.md) - Fully annotated example
