# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bizon is a Python-based data extraction and loading (EL) framework for processing large data streams with native fault tolerance, checkpointing, and high throughput (billions of records).

## Common Commands

```bash
# Install dependencies
make install                    # Full install with dev/test dependencies
poetry install --with test      # Install with test dependencies only

# Run tests
poetry run pytest               # Run all tests
poetry run pytest tests/path/to/test_file.py -k "test_name"  # Single test

# Format code
make format                     # Run pre-commit (black + isort)
pre-commit run --all-files      # Equivalent

# CLI commands
bizon run config.yml            # Run a pipeline from YAML config
bizon source list               # List available sources
bizon stream list <source>      # List streams for a source
```

## Code Style

- Black formatter with line length 120
- isort with black profile for import sorting

## Architecture

### Core Components

The framework uses a **producer-consumer pattern** with pluggable components:

```
YAML Config → RunnerFactory → Producer → Queue → Consumer → Destination
                                ↑                    ↓
                              Source              Backend (checkpoints)
```

### Key Abstractions

| Abstraction | Base Class | Location |
|-------------|------------|----------|
| Source | `AbstractSource` | `bizon/source/source.py` |
| Destination | `AbstractDestination` | `bizon/destination/destination.py` |
| Queue | `AbstractQueue` | `bizon/engine/queue/queue.py` |
| Backend | `AbstractBackend` | `bizon/engine/backend/backend.py` |
| Runner | `AbstractRunner` | `bizon/engine/runner/runner.py` |

### Directory Structure

- `bizon/cli/` - CLI entry points (`bizon run`, `bizon source list`)
- `bizon/source/` - Source abstraction, auth, cursor, discovery
- `bizon/destination/` - Destination abstraction, buffering
- `bizon/engine/` - Queue, backend, runner implementations
- `bizon/engine/pipeline/` - Producer and consumer logic
- `bizon/connectors/sources/` - Built-in source connectors
- `bizon/connectors/destinations/` - Built-in destination connectors
- `bizon/common/models.py` - `BizonConfig` main YAML schema
- `bizon/transform/` - Data transformation system

### Adding New Sources

Sources are auto-discovered via AST parsing. Create:

```
bizon/connectors/sources/{source_name}/src/
├── __init__.py
├── config.py    # SourceConfig subclass
└── source.py    # AbstractSource implementation
```

Required methods:
- `streams() -> List[str]` - Available streams
- `get_config_class()` - Return config class
- `get_authenticator()` - Return auth handler
- `check_connection()` - Test connectivity
- `get(pagination)` - Fetch records (returns `SourceIteration`)
- `get_records_after()` - For incremental sync support (optional)

### Adding New Destinations

Create:

```
bizon/connectors/destinations/{dest_name}/src/
├── __init__.py
├── config.py      # DestinationConfig subclass with Literal name
└── destination.py # AbstractDestination implementation
```

Then register in:
1. `DestinationTypes` enum in `bizon/destination/config.py`
2. `BizonConfig.destination` Union in `bizon/common/models.py`
3. `DestinationFactory.get_destination()` in `bizon/destination/destination.py`

### Sync Modes

- `FULL_REFRESH` - Full dataset each run
- `INCREMENTAL` - Only new/updated records since last run
- `STREAM` - Continuous streaming mode

### Queue Types

- `python_queue` - In-memory (dev/test)
- `kafka` - Apache Kafka/Redpanda (production)
- `rabbitmq` - RabbitMQ (production)

### Backend Types (state storage)

- `sqlite` / `sqlite_in_memory` - File/memory (dev/test)
- `postgres` - PostgreSQL (production)
- `bigquery` - Google BigQuery (production)

### Runner Types

- `thread` - ThreadPoolExecutor (default)
- `process` - ProcessPoolExecutor (true parallelism)
- `stream` - Synchronous single-thread

### Key Patterns

- **Factory Pattern**: `RunnerFactory`, `QueueFactory`, `BackendFactory`, `DestinationFactory`
- **Cursor-based Checkpointing**: Producer and destination cursors saved to backend for recovery
- **Pydantic Discriminators**: Union types route to correct implementation based on `type`/`name` field
- **Polars DataFrames**: Used for memory-efficient columnar data processing
- **Buffering**: Destinations buffer records before batch writes (configurable size/timeout)
