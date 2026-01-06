# Contributing

## Local setup
- Install poetry: `pip install poetry`
- Activate the virtual environment: `poetry shell`
- Install dependencies: `poetry install --all-extras`

## Testing

### Message brokers

To test the pipeline with Kafka, you can use `docker compose` to setup Kafka, Redpanda or RabbitMQ locally.

**Kafka**
```bash
docker compose --file ./scripts/queues/kafka-compose.yml up # Kafka
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue: bootstrap_server: localhost:9092 # Kafka
```

**RedPanda**

```bash
docker compose --file ./scripts/queues/redpanda-compose.yml up # Redpanda
```

In your YAML configuration, set the `queue` configuration to Redpanda under `engine`:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue: bootstrap_server: localhost:19092
```


**RabbitMQ**
```bash
docker compose --file ./scripts/queues/rabbitmq-compose.yml up
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:

```yaml
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost

```

### Start the backends (Optional)
If you don't start the backend, the test fall back on the bizon default backend which is the destination warehouse.

Create a .env file living in the /tests folder:
```bash
BIGQUERY_PROJECT_ID=<YOUR_PROJECT_ID>
BIGQUERY_DATASET_ID=bizon_test
```

## Implementing Incremental Sync for Sources

To support incremental sync in a source, implement the `get_records_after()` method:

```python
from bizon.source.models import SourceIncrementalState, SourceIteration

def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
    """
    Fetch records updated after source_state.last_run.

    Args:
        source_state: Contains:
            - last_run (datetime): Timestamp of the previous successful sync
            - cursor_field (str, optional): The field name to filter by (from config)
            - state (dict): Custom state from previous sync (usually empty)
        pagination: Pagination state for multi-page results

    Returns:
        SourceIteration with records and next_pagination
    """
    # Get the cursor field from state (configured by user in YAML)
    cursor_field = source_state.cursor_field or "updated_at"

    # Filter records by the cursor field > last_run
    records = self.fetch_records_after_timestamp(
        timestamp=source_state.last_run,
        cursor_field=cursor_field,
        pagination=pagination,
    )

    return SourceIteration(
        records=records,
        next_pagination=next_page_token,  # None if no more pages
    )
```

### Key Points:
- `source_state.last_run` is the `created_at` timestamp of the last successful job
- `source_state.cursor_field` is the field name configured in the source YAML (e.g., `updated_at`)
- The method should filter records where `cursor_field > last_run`
- Return `next_pagination={}` or `next_pagination=None` when there are no more records
- On the first incremental run (no previous job), Bizon falls back to calling `get()` instead