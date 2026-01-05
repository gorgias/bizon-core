---
description: Scaffold a new source connector for Bizon
---

# New Source Connector

You are helping create a new Bizon source connector. Sources are auto-discovered - no registration needed!

## Workflow

### Step 1: Gather Requirements

Ask the user:
1. **Source name** (e.g., "stripe", "hubspot", "salesforce")
2. **Streams to sync** (e.g., "users", "orders", "contacts")
3. **API documentation URL** (optional but helpful)
4. **Authentication type**: API key, OAuth, Basic, or none

### Step 2: Read the Templates

Before generating code, read these files for patterns:
- `docs/ai-connector-guide.md` - Templates and decision trees
- `docs/reference-connector.md` - Annotated production example
- `bizon/connectors/sources/dummy/src/source.py` - Simple reference

### Step 3: Create Directory Structure

```bash
mkdir -p bizon/connectors/sources/{source_name}/src
mkdir -p bizon/connectors/sources/{source_name}/config
touch bizon/connectors/sources/{source_name}/src/__init__.py
```

### Step 4: Generate Files

Create these files:

1. **`src/source.py`** - Main implementation with:
   - Config class extending `SourceConfig`
   - Source class extending `AbstractSource`
   - All required methods: `streams()`, `get_config_class()`, `get_authenticator()`, `check_connection()`, `get_total_records_count()`, `get()`

2. **`config/{source_name}.example.yml`** - Example YAML config

### Step 5: Key Implementation Details

- `streams()` returns `List[str]` of available stream names
- `get()` returns `SourceIteration(records=[...], next_pagination={...})`
- `next_pagination` must be `{}` (empty dict) when no more pages
- Each record needs unique `id`: `SourceRecord(id=item["id"], data=item)`

### Step 6: Verify

```bash
# Format
uv run ruff format bizon/connectors/sources/{source_name}

# Verify discovery
uv run bizon source list
uv run bizon stream list {source_name}

# Test
uv run bizon run bizon/connectors/sources/{source_name}/config/{source_name}.example.yml
```

## Quick Reference

| Method | Returns | Purpose |
|--------|---------|---------|
| `streams()` | `List[str]` | Available streams |
| `get_config_class()` | `SourceConfig` | Config class |
| `get_authenticator()` | `AuthBase` or `None` | Auth handler |
| `check_connection()` | `Tuple[bool, str\|None]` | Test connectivity |
| `get_total_records_count()` | `int\|None` | Total records |
| `get(pagination)` | `SourceIteration` | Fetch records |

## Remember

- Sources are **auto-discovered** - no registration needed
- Empty `__init__.py` is required in `src/`
- Return `{}` for `next_pagination` when done paginating
