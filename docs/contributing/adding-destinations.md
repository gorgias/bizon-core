# Adding a New Destination Connector

Destinations receive Polars DataFrames and write them to target systems. Unlike sources, destinations require **manual registration in 3 places**.

## Directory Structure

```
bizon/connectors/destinations/{dest_name}/
└── src/
    ├── __init__.py       # Empty file (required)
    ├── config.py         # Configuration classes
    └── destination.py    # Main implementation
```

## Required Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `check_connection()` | `bool` | Test connectivity |
| `write_records(df)` | `Tuple[bool, str\|None]` | Write DataFrame, return success + error |
| `finalize()` | `bool` | Optional cleanup after all writes |

## Step 1: Create Config

```python
# bizon/connectors/destinations/mydest/src/config.py
from typing import Literal
from pydantic import Field
from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class MyDestDetailsConfig(AbstractDestinationDetailsConfig):
    """Connection settings for MyDest."""
    connection_string: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Target table")


class MyDestConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.MY_DEST]  # Must match enum exactly
    alias: str = "mydest"
    config: MyDestDetailsConfig
```

## Step 2: Implement Destination

```python
# bizon/connectors/destinations/mydest/src/destination.py
from typing import Tuple, Union
import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback

from .config import MyDestDetailsConfig


class MyDestDestination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: MyDestDetailsConfig,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        super().__init__(
            sync_metadata=sync_metadata,
            config=config,
            backend=backend,
            source_callback=source_callback,
            monitor=monitor,
        )
        # Initialize client here if needed

    def check_connection(self) -> bool:
        # Test connectivity
        return True

    def write_records(
        self, df_destination_records: pl.DataFrame
    ) -> Tuple[bool, Union[str, None]]:
        """
        Write records to destination.

        DataFrame columns:
        - bizon_id: str (unique record ID)
        - bizon_extracted_at: datetime
        - bizon_loaded_at: datetime
        - source_data: str (JSON string of record)
        - source_record_id: str
        """
        try:
            for record in df_destination_records.iter_rows(named=True):
                # record["source_data"] contains the JSON payload
                pass

            logger.info(f"Wrote {df_destination_records.height} records")
            return True, None
        except Exception as e:
            return False, str(e)

    def finalize(self) -> bool:
        # Optional: cleanup after all writes complete
        return True
```

## Step 3: Register in 3 Places

### 3.1 Add to DestinationTypes Enum

**File**: `bizon/destination/config.py`

```python
class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    BIGQUERY_STREAMING = "bigquery_streaming"
    BIGQUERY_STREAMING_V2 = "bigquery_streaming_v2"
    LOGGER = "logger"
    FILE = "file"
    MY_DEST = "mydest"  # <-- ADD THIS
```

### 3.2 Add to BizonConfig Union

**File**: `bizon/common/models.py`

```python
# Add import at top
from bizon.connectors.destinations.mydest.src.config import MyDestConfig

# Add to Union (around line 108)
destination: Union[
    BigQueryConfig,
    BigQueryStreamingConfig,
    BigQueryStreamingV2Config,
    LoggerConfig,
    FileDestinationConfig,
    MyDestConfig,  # <-- ADD THIS
] = Field(
    description="Destination configuration",
    discriminator="name",
    default=...,
)
```

### 3.3 Add to DestinationFactory

**File**: `bizon/destination/destination.py`

```python
# Add elif branch in get_destination() method (around line 290)
elif config.name == DestinationTypes.MY_DEST:
    from bizon.connectors.destinations.mydest.src.destination import (
        MyDestDestination,
    )

    return MyDestDestination(
        sync_metadata=sync_metadata,
        config=config.config,
        backend=backend,
        source_callback=source_callback,
        monitor=monitor,
    )
```

## Verification

```bash
# Format all modified files
uv run ruff format bizon/connectors/destinations/mydest
uv run ruff format bizon/destination/config.py
uv run ruff format bizon/common/models.py
uv run ruff format bizon/destination/destination.py

# Run tests
uv run pytest tests/connectors/destinations/mydest -v
```

## Registration Checklist

Before submitting:

- [ ] `src/__init__.py` exists (can be empty)
- [ ] Details config extends `AbstractDestinationDetailsConfig`
- [ ] Main config extends `AbstractDestinationConfig`
- [ ] `Literal[DestinationTypes.X]` matches enum exactly
- [ ] Destination extends `AbstractDestination`
- [ ] **Registered in `DestinationTypes` enum**
- [ ] **Registered in `BizonConfig.destination` Union**
- [ ] **Registered in `DestinationFactory.get_destination()`**
- [ ] All 3 registrations use the same name string
- [ ] Tests pass

## Incremental Sync Support

Destinations must implement `finalize()` to properly handle different sync modes.

### Sync Mode Handling in `finalize()`

```python
from bizon.source.config import SourceSyncModes

def finalize(self) -> bool:
    """
    Finalize sync - handle data based on sync mode.

    Sync modes:
    - FULL_REFRESH: Replace main table with temp table
    - INCREMENTAL: Append temp table to main table
    - STREAM: Direct writes, no temp table
    """
    sync_mode = self.sync_metadata.sync_mode

    if sync_mode == SourceSyncModes.FULL_REFRESH.value:
        # Replace main table with temp table contents
        logger.info(f"Replacing {self.table_id} with {self.temp_table_id}")
        self.client.query(f"CREATE OR REPLACE TABLE {self.table_id} AS SELECT * FROM {self.temp_table_id}")
        self.client.delete_table(self.temp_table_id, not_found_ok=True)
        return True

    elif sync_mode == SourceSyncModes.INCREMENTAL.value:
        # Append temp table to main table (append-only strategy)
        logger.info(f"Appending {self.temp_table_id} to {self.table_id}")
        self.client.query(f"INSERT INTO {self.table_id} SELECT * FROM {self.temp_table_id}")
        self.client.delete_table(self.temp_table_id, not_found_ok=True)
        return True

    elif sync_mode == SourceSyncModes.STREAM.value:
        # Stream mode writes directly, no temp table management
        logger.info("Stream sync completed")
        return True

    return True
```

### Temp Table Naming Convention

Use different temp table names per sync mode for clarity:

```python
@property
def temp_table_id(self) -> str:
    """Return temp table name based on sync mode."""
    if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
        return f"{self.table_id}_temp"
    elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
        return f"{self.table_id}_incremental"
    else:  # STREAM
        return self.table_id  # Direct writes
```

### write_records() Considerations

In `write_records()`, always write to `self.temp_table_id` (not `self.table_id`):

```python
def write_records(self, df: pl.DataFrame) -> Tuple[bool, str | None]:
    try:
        # Write to temp table - finalize() will move to main table
        self.client.write_to_table(self.temp_table_id, df)
        return True, None
    except Exception as e:
        return False, str(e)
```

### File-Based Destinations

For file destinations, append instead of using temp files:

```python
def finalize(self) -> bool:
    if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
        # Move temp file to main file (replace)
        os.replace(self.temp_file_path, self.file_path)
        return True

    elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
        # Append temp file contents to main file
        with open(self.file_path, "a") as main_file:
            with open(self.temp_file_path, "r") as temp_file:
                main_file.write(temp_file.read())
        os.remove(self.temp_file_path)
        return True

    return True
```

### Incremental Checklist for Destinations

- [ ] Import `SourceSyncModes` from `bizon.source.config`
- [ ] Implement `temp_table_id` property with mode-specific naming
- [ ] Write records to `temp_table_id` in `write_records()`
- [ ] Handle `INCREMENTAL` mode in `finalize()` (append strategy)
- [ ] Clean up temp table/file after finalize
- [ ] Test with incremental config

## Reference

See existing implementations:
- Simple: `bizon/connectors/destinations/logger/` (~40 lines)
- Complex: `bizon/connectors/destinations/bigquery/` (batch loading with schema management)
- Incremental: `bizon/connectors/destinations/bigquery_streaming_v2/` (streaming with incremental support)
