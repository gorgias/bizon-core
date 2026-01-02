# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bizon is a Python-based data extraction and loading (EL) framework for processing large data streams with native fault tolerance, checkpointing, and high throughput (billions of records).

## Common Commands

```bash
# Install dependencies
make install                    # Full install with dev/test dependencies
uv sync --group test            # Install with test dependencies only
uv sync --all-extras            # Install with all extras (postgres, kafka, etc.)

# Run tests
uv run pytest                   # Run all tests
uv run pytest tests/path/to/test_file.py -k "test_name"  # Single test

# Format code
make format                     # Run Ruff formatter and linter
uv run ruff format .            # Format only
uv run ruff check --fix .       # Lint and auto-fix

# CLI commands
uv run bizon run config.yml     # Run a pipeline from YAML config
uv run bizon source list        # List available sources
uv run bizon stream list <source>  # List streams for a source
```

## Code Style

- Ruff formatter with line length 120
- Ruff linter with isort rules for import sorting
- Configuration in `pyproject.toml` under `[tool.ruff]`

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

### Claude Skills

Use these skills for common workflows:

| Skill | Description |
|-------|-------------|
| `/new-source` | Scaffold a new source connector |
| `/new-destination` | Scaffold a new destination connector |
| `/run-checks` | Run format, lint, and tests |

### AI-Assisted Connector Generation

**Source connectors** - Read these guides:
- `docs/ai-connector-guide.md` - Templates, decision trees, extraction checklists
- `docs/reference-connector.md` - Fully annotated production example

**Destination connectors** - Read:
- `docs/ai-destination-guide.md` - Templates with placeholders, registration steps

**Workflow for sources**:
```
API Docs URL → Extract info → Generate code → Validate
```
Sources are auto-discovered - no registration needed!

**Workflow for destinations**:
```
Generate code → Register in 3 places → Validate
```
Must register in: `DestinationTypes` enum, `BizonConfig.destination` Union, `DestinationFactory`

**Files to create for sources**:
```
bizon/connectors/sources/{source_name}/
├── config/
│   └── {source_name}.example.yml
└── src/
    ├── __init__.py
    ├── config.py
    └── source.py
```

**Files to create for destinations**:
```
bizon/connectors/destinations/{dest_name}/
└── src/
    ├── __init__.py
    ├── config.py
    └── destination.py
```

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
