## Claude Code Guide for bizon

This file teaches Claude Code how to work in this repository.

### Project structure

- Bizon runs data pipelines from `sources` to `destinations`.
- `sources` and `destinations` are connectors that are used to read and write data.
- The classes in `bizon/connectors/sources` and `bizon/connectors/destinations` are the abstract base classes for sources and destinations.
- The actual connectors are in `bizon/connectors/sources/` and `bizon/connectors/destinations/`.
- The tests for these connectors are in `tests/source/` and `tests/destination/`.



### Environment
- **Python**: 3.9–3.12 (see `pyproject.toml`)
- **Package manager**: Poetry
- **Entry points**:
  - CLI: `bizon` (installed via Poetry script)
  - Module entry: `python -m bizon`

### Quick setup
```bash
# From repo root
pip install poetry
make install    # Installs with dev + test groups and all extras, installs pre-commit
```

If Make isn’t available:
```bash
pip install poetry
poetry install --with dev --with test --all-extras
pre-commit install
```

### Common commands
- **Format / hooks**:
```bash
make format      # runs pre-commit on all files
```
- **Run tests**:
```bash
poetry run pytest -q
# Or target specific tests
poetry run pytest -q tests/source/test_discover.py
```
- **Run the CLI (inside the repo via Poetry)**:
```bash
poetry run bizon source list
poetry run bizon stream list dummy
poetry run bizon run config.yml --runner thread --log-level INFO
```

### Running the app
Minimal pipeline config example:
```yaml
name: demo-creatures-pipeline

source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: logger
  config:
    dummy: dummy
```
Run it:
```bash
poetry run bizon run config.yml
```

### Local services (Docker Compose)
Some connectors/tests require local services. Use the provided compose files.

- **Kafka / Redpanda**:
```bash
docker compose --file ./scripts/queues/kafka-compose.yml up
# or Redpanda
docker compose --file ./scripts/queues/redpanda-compose.yml up
```
- Configure in YAML:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:9092  # Redpanda: 19092
```

- **RabbitMQ**:
```bash
docker compose --file ./scripts/queues/rabbitmq-compose.yml up
```
- Configure in YAML:
```yaml
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost
        queue_name: bizon
```

- **Postgres backend (optional)**:
```bash
docker compose --file ./scripts/backend/postgres-compose.yml up -d
```

### Project layout highlights
- `bizon/cli/main.py`: Click-based CLI (`bizon` entrypoint)
- `bizon/engine/`: Runner, queue, backend abstraction
- `bizon/connectors/`: Sources and destinations
  - Sources under `bizon/connectors/sources/*`
  - Destinations under `bizon/connectors/destinations/*`
- `tests/`: unit and e2e tests

### Development guidance for Claude
- Prefer `poetry run ...` when executing Python or tools.
- After code edits that affect behavior, run `poetry run pytest -q` and fix failures before finishing.
- Keep changes scoped and readable. Match existing formatting and imports. Do not reformat unrelated files.
- Use explicit, descriptive names; avoid 1–2 letter identifiers.
- When adding features, include tests under `tests/`.
- For connectors needing optional deps, rely on Poetry extras (this repo installs all extras via `make install`).

### Useful references
- README with usage and configuration: `README.md`
- CLI help:
```bash
poetry run bizon --help
poetry run bizon run --help
poetry run bizon source --help
poetry run bizon stream --help
```
