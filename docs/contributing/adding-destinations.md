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

## Reference

See existing implementations:
- Simple: `bizon/connectors/destinations/logger/` (~40 lines)
- Complex: `bizon/connectors/destinations/bigquery/` (batch loading with schema management)
