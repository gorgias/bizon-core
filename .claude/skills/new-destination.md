---
description: Scaffold a new destination connector for Bizon
---

# New Destination Connector

You are helping create a new Bizon destination connector. Destinations require **registration in 3 places**.

## Workflow

### Step 1: Gather Requirements

Ask the user:
1. **Destination name** (e.g., "postgres", "s3", "snowflake")
2. **Connection parameters** needed (host, credentials, etc.)
3. **Write method**: batch insert, streaming, file upload

### Step 2: Read the Templates

Before generating code, read these files:
- `docs/ai-destination-guide.md` - Templates and placeholders
- `bizon/connectors/destinations/logger/src/` - Simple reference
- `bizon/destination/config.py` - See existing `DestinationTypes` enum

### Step 3: Create Directory Structure

```bash
mkdir -p bizon/connectors/destinations/{dest_name}/src
touch bizon/connectors/destinations/{dest_name}/src/__init__.py
```

### Step 4: Generate Files

Create these files:

1. **`src/config.py`**:
   - Details config extending `AbstractDestinationDetailsConfig`
   - Main config extending `AbstractDestinationConfig` with `Literal[DestinationTypes.X]`

2. **`src/destination.py`**:
   - Destination class extending `AbstractDestination`
   - Implement `check_connection()`, `write_records()`, optionally `finalize()`

### Step 5: Register in 3 Places (CRITICAL!)

#### 5.1 Add Enum Value
**File**: `bizon/destination/config.py`
```python
class DestinationTypes(str, Enum):
    ...
    {DEST_ENUM} = "{dest_name}"  # ADD THIS
```

#### 5.2 Update Union Type
**File**: `bizon/common/models.py`
- Add import at top: `from bizon.connectors.destinations.{dest_name}.src.config import {DestName}Config`
- Add to `destination: Union[...]` field

#### 5.3 Update Factory
**File**: `bizon/destination/destination.py`
- Add `elif config.name == DestinationTypes.{DEST_ENUM}:` branch in `get_destination()`

### Step 6: Verify

```bash
# Format all modified files
uv run ruff format bizon/connectors/destinations/{dest_name}
uv run ruff format bizon/destination/config.py
uv run ruff format bizon/common/models.py
uv run ruff format bizon/destination/destination.py

# Run tests
uv run pytest tests/ -v
```

## Quick Reference

| Method | Returns | Purpose |
|--------|---------|---------|
| `check_connection()` | `bool` | Test connectivity |
| `write_records(df)` | `Tuple[bool, str\|None]` | Write DataFrame |
| `finalize()` | `bool` | Optional cleanup |

## DataFrame Columns

The `df_destination_records` DataFrame contains:
- `bizon_id`: str - Unique record ID
- `bizon_extracted_at`: datetime
- `bizon_loaded_at`: datetime
- `source_data`: str - JSON string of record
- `source_record_id`: str

## Registration Checklist

Before finishing, verify all 3 registrations:
- [ ] `DestinationTypes` enum has new value
- [ ] `BizonConfig.destination` Union includes new config
- [ ] `DestinationFactory.get_destination()` has elif branch
- [ ] All 3 use exactly the same name string
