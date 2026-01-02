# AI Destination Generation Guide

This guide enables AI agents to create production-ready Bizon destination connectors.

## Overview

A Bizon destination receives Polars DataFrames and writes them to a target system. Each connector consists of:

```
bizon/connectors/destinations/{dest_name}/
└── src/
    ├── __init__.py       # Empty file
    ├── config.py         # Configuration models
    └── destination.py    # Main implementation
```

**Key difference from sources**: Destinations require **manual registration in 3 files**.

## Step 1: Understand the Target System

Before writing code, identify:

| Information | Purpose | Example |
|-------------|---------|---------|
| Connection params | Config fields | host, port, credentials |
| Write method | Implementation | batch insert, streaming, file upload |
| Error handling | Retry logic | transient vs permanent failures |
| Batch optimization | Buffer settings | optimal batch size |

## Step 2: Generate Config

### Template: config.py

```python
from typing import Literal, Optional
from pydantic import Field
from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class {{DEST_NAME}}DestinationConfig(AbstractDestinationDetailsConfig):
    """Configuration for {{dest_name}} destination.

    Inherits from AbstractDestinationDetailsConfig:
    - buffer_size: int (default 50 MB)
    - buffer_flush_timeout: int (default 600 seconds)
    - max_concurrent_threads: int (default 10)
    """

    # Connection settings
    {{CONNECTION_FIELD}}: str = Field(..., description="{{CONNECTION_DESCRIPTION}}")

    # Optional settings
    {{OPTIONAL_FIELD}}: Optional[str] = Field(None, description="{{OPTIONAL_DESCRIPTION}}")


class {{DEST_NAME}}Config(AbstractDestinationConfig):
    name: Literal[DestinationTypes.{{DEST_ENUM}}]
    alias: str = "{{dest_alias}}"
    config: {{DEST_NAME}}DestinationConfig
```

## Step 3: Generate Destination

### Template: destination.py

```python
from typing import Tuple, Union

import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback

from .config import {{DEST_NAME}}DestinationConfig


class {{DEST_NAME}}Destination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: {{DEST_NAME}}DestinationConfig,
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
        # Initialize client/connection
        self.client = self._create_client()

    def _create_client(self):
        """Initialize connection to destination."""
        # Implementation specific
        pass

    def check_connection(self) -> bool:
        """Test connectivity to destination."""
        try:
            # Test connection
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False

    def write_records(
        self, df_destination_records: pl.DataFrame
    ) -> Tuple[bool, Union[str, None]]:
        """
        Write records to destination.

        Args:
            df_destination_records: DataFrame with columns:
                - bizon_id: str (unique record ID)
                - bizon_extracted_at: datetime
                - bizon_loaded_at: datetime
                - source_data: str (JSON string of record)
                - source_record_id: str

        Returns:
            Tuple of (success: bool, error_message: str or None)
        """
        try:
            for record in df_destination_records.iter_rows(named=True):
                # record["source_data"] contains JSON payload
                # Parse with: orjson.loads(record["source_data"])
                pass

            logger.info(f"Wrote {df_destination_records.height} records")
            return True, None

        except Exception as e:
            return False, str(e)

    def finalize(self) -> bool:
        """Optional: Called after all records written."""
        return True
```

## Step 4: Register in 3 Places (CRITICAL)

### 4.1 DestinationTypes Enum

**File**: `bizon/destination/config.py`

```python
class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    BIGQUERY_STREAMING = "bigquery_streaming"
    BIGQUERY_STREAMING_V2 = "bigquery_streaming_v2"
    LOGGER = "logger"
    FILE = "file"
    {{DEST_ENUM}} = "{{dest_name}}"  # <-- ADD
```

### 4.2 BizonConfig Union

**File**: `bizon/common/models.py`

```python
# Add import at top of file
from bizon.connectors.destinations.{{dest_name}}.src.config import {{DEST_NAME}}Config

# Add to Union around line 108
destination: Union[
    BigQueryConfig,
    BigQueryStreamingConfig,
    BigQueryStreamingV2Config,
    LoggerConfig,
    FileDestinationConfig,
    {{DEST_NAME}}Config,  # <-- ADD
] = Field(
    description="Destination configuration",
    discriminator="name",
    default=...,
)
```

### 4.3 DestinationFactory

**File**: `bizon/destination/destination.py`

```python
# Add elif branch in get_destination() method around line 290
elif config.name == DestinationTypes.{{DEST_ENUM}}:
    from bizon.connectors.destinations.{{dest_name}}.src.destination import (
        {{DEST_NAME}}Destination,
    )

    return {{DEST_NAME}}Destination(
        sync_metadata=sync_metadata,
        config=config.config,
        backend=backend,
        source_callback=source_callback,
        monitor=monitor,
    )
```

## Placeholder Reference

| Placeholder | Format | Example |
|-------------|--------|---------|
| `{{DEST_NAME}}` | PascalCase | `Postgres`, `S3`, `Snowflake` |
| `{{dest_name}}` | snake_case | `postgres`, `s3`, `snowflake` |
| `{{DEST_ENUM}}` | UPPER_SNAKE | `POSTGRES`, `S3`, `SNOWFLAKE` |
| `{{dest_alias}}` | lowercase | `postgres`, `s3`, `snowflake` |

## Example YAML Config

```yaml
name: source_to_{{dest_name}}

source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: test

destination:
  name: {{dest_name}}
  config:
    {{connection_field}}: "{{connection_value}}"
```

## Validation Checklist

### Config Validation
- [ ] `__init__.py` exists in `src/`
- [ ] Details config extends `AbstractDestinationDetailsConfig`
- [ ] Main config extends `AbstractDestinationConfig`
- [ ] `Literal[DestinationTypes.X]` matches enum exactly
- [ ] All required fields have descriptions

### Implementation Validation
- [ ] Destination extends `AbstractDestination`
- [ ] `check_connection()` returns `bool`
- [ ] `write_records()` returns `Tuple[bool, str|None]`
- [ ] Errors are logged and returned, not raised

### Registration Validation
- [ ] Added to `DestinationTypes` enum in `bizon/destination/config.py`
- [ ] Added to `BizonConfig.destination` Union in `bizon/common/models.py`
- [ ] Added to `DestinationFactory.get_destination()` in `bizon/destination/destination.py`
- [ ] All 3 use identical name string

### Final Validation
```bash
# Format all files
uv run ruff format bizon/connectors/destinations/{{dest_name}}
uv run ruff format bizon/destination/config.py
uv run ruff format bizon/common/models.py
uv run ruff format bizon/destination/destination.py

# Run tests
uv run pytest tests/ -v
```

## Reference Implementations

| Complexity | Connector | Key Pattern |
|------------|-----------|-------------|
| Minimal | `logger/` | 40 lines, just logs records |
| Simple | `file/` | File writes with format handling |
| Complex | `bigquery/` | Schema management, batch loading |
| Streaming | `bigquery_streaming/` | Real-time inserts with buffering |
