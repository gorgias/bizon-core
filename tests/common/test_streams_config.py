import pytest
from yaml import safe_load

from bizon.common.models import (
    BizonConfig,
    StreamConfig,
    StreamDestinationConfig,
    StreamSourceConfig,
)


class TestStreamSourceConfig:
    def test_kafka_topic_field(self):
        config = StreamSourceConfig(topic="events.users.update")
        assert config.topic == "events.users.update"

    def test_api_endpoint_field(self):
        config = StreamSourceConfig(endpoint="/api/v1/users")
        assert config.endpoint == "/api/v1/users"

    def test_extra_fields_allowed(self):
        config = StreamSourceConfig(topic="test", custom_field="custom_value")
        assert config.topic == "test"
        assert config.custom_field == "custom_value"


class TestStreamDestinationConfig:
    def test_table_id_required(self):
        config = StreamDestinationConfig(table_id="project.dataset.table")
        assert config.table_id == "project.dataset.table"

    def test_with_record_schema(self):
        config = StreamDestinationConfig(
            table_id="project.dataset.table",
            record_schema=[
                {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            ],
            clustering_keys=["id"],
        )
        assert len(config.record_schema) == 2
        assert config.clustering_keys == ["id"]


class TestStreamConfig:
    def test_valid_stream_config(self):
        config = StreamConfig(
            name="user_events",
            source=StreamSourceConfig(topic="events.users.update"),
            destination=StreamDestinationConfig(table_id="project.dataset.users"),
        )
        assert config.name == "user_events"
        assert config.source.topic == "events.users.update"
        assert config.destination.table_id == "project.dataset.users"

    def test_table_id_format_validation_valid(self):
        config = StreamConfig(
            name="test",
            source=StreamSourceConfig(topic="test"),
            destination=StreamDestinationConfig(table_id="project.dataset.table"),
        )
        assert config.destination.table_id == "project.dataset.table"

    def test_table_id_format_validation_invalid(self):
        with pytest.raises(ValueError, match="table_id must be in format"):
            StreamConfig(
                name="test",
                source=StreamSourceConfig(topic="test"),
                destination=StreamDestinationConfig(table_id="invalid_table_id"),
            )

    def test_table_id_format_validation_two_parts(self):
        with pytest.raises(ValueError, match="Found 2 parts instead of 3"):
            StreamConfig(
                name="test",
                source=StreamSourceConfig(topic="test"),
                destination=StreamDestinationConfig(table_id="dataset.table"),
            )


class TestBizonConfigWithStreams:
    @pytest.fixture
    def base_config(self):
        return {
            "name": "test_pipeline",
            "source": {
                "name": "kafka",
                "stream": "topic",
                "sync_mode": "stream",
                "bootstrap_servers": "localhost:9092",
                "group_id": "test-group",
                "authentication": {
                    "type": "basic",
                    "params": {"username": "user", "password": "pass"},
                },
            },
            "destination": {
                "name": "logger",
                "config": {},
            },
            "engine": {"runner": {"type": "stream"}},
        }

    def test_streams_config_loads_successfully(self, base_config):
        base_config["streams"] = [
            {
                "name": "user_events",
                "source": {"topic": "events.users.update"},
                "destination": {"table_id": "project.dataset.users"},
            }
        ]
        config = BizonConfig(**base_config)
        assert len(config.streams) == 1
        assert config.streams[0].name == "user_events"

    def test_streams_requires_stream_sync_mode(self, base_config):
        base_config["source"]["sync_mode"] = "full_refresh"
        base_config["streams"] = [
            {
                "name": "test",
                "source": {"topic": "test"},
                "destination": {"table_id": "project.dataset.table"},
            }
        ]
        with pytest.raises(ValueError, match="requires source.sync_mode='stream'"):
            BizonConfig(**base_config)

    def test_duplicate_stream_names_rejected(self, base_config):
        base_config["streams"] = [
            {
                "name": "duplicate_name",
                "source": {"topic": "topic1"},
                "destination": {"table_id": "project.dataset.table1"},
            },
            {
                "name": "duplicate_name",
                "source": {"topic": "topic2"},
                "destination": {"table_id": "project.dataset.table2"},
            },
        ]
        with pytest.raises(ValueError, match="Duplicate stream names"):
            BizonConfig(**base_config)

    def test_duplicate_table_ids_rejected(self, base_config):
        base_config["streams"] = [
            {
                "name": "stream1",
                "source": {"topic": "topic1"},
                "destination": {"table_id": "project.dataset.same_table"},
            },
            {
                "name": "stream2",
                "source": {"topic": "topic2"},
                "destination": {"table_id": "project.dataset.same_table"},
            },
        ]
        with pytest.raises(ValueError, match="Duplicate table_ids"):
            BizonConfig(**base_config)

    def test_multiple_streams_valid(self, base_config):
        base_config["streams"] = [
            {
                "name": "stream1",
                "source": {"topic": "topic1"},
                "destination": {"table_id": "project.dataset.table1"},
            },
            {
                "name": "stream2",
                "source": {"topic": "topic2"},
                "destination": {"table_id": "project.dataset.table2"},
            },
        ]
        config = BizonConfig(**base_config)
        assert len(config.streams) == 2


class TestKafkaTopicsInjectionFromStreams:
    """Test that topics are injected into Kafka source config from streams.

    Note: BizonConfig.source is typed as SourceConfig (base class), so Kafka-specific
    fields like 'topics' aren't preserved after model instantiation. However, the
    injection still works at the dict level, and the Kafka connector factory
    properly instantiates KafkaSourceConfig with the injected topics.

    These tests verify the injection works by directly instantiating KafkaSourceConfig
    from the pre-processed data.
    """

    @pytest.fixture
    def kafka_config_without_topics(self):
        return {
            "name": "test_kafka_pipeline",
            "source": {
                "name": "kafka",
                "stream": "topic",
                "sync_mode": "stream",
                "bootstrap_servers": "localhost:9092",
                "group_id": "test-group",
                "authentication": {
                    "type": "basic",
                    "params": {"username": "user", "password": "pass"},
                },
            },
            "destination": {
                "name": "logger",
                "config": {},
            },
            "engine": {"runner": {"type": "stream"}},
            "streams": [
                {
                    "name": "user_events",
                    "source": {"topic": "events.users.update"},
                    "destination": {"table_id": "project.dataset.users"},
                },
                {
                    "name": "order_events",
                    "source": {"topic": "events.orders.create"},
                    "destination": {"table_id": "project.dataset.orders"},
                },
            ],
        }

    def test_topics_injected_from_streams(self, kafka_config_without_topics):
        from bizon.connectors.sources.kafka.src.config import KafkaSourceConfig

        # Run the model_validator by instantiating BizonConfig
        # This modifies the input dict in place
        BizonConfig(**kafka_config_without_topics)

        # Verify topics were injected into source dict
        source_dict = kafka_config_without_topics["source"]
        topics = source_dict.get("topics")
        assert topics is not None
        assert len(topics) == 2

        topic_names = [t["name"] for t in topics]
        assert "events.users.update" in topic_names
        assert "events.orders.create" in topic_names

        # Verify KafkaSourceConfig can be instantiated with injected topics
        kafka_config = KafkaSourceConfig(**source_dict)
        assert len(kafka_config.topics) == 2

    def test_topics_not_overwritten_if_already_set(self):
        from bizon.connectors.sources.kafka.src.config import KafkaSourceConfig

        config_dict = {
            "name": "test_kafka_pipeline",
            "source": {
                "name": "kafka",
                "stream": "topic",
                "sync_mode": "stream",
                "bootstrap_servers": "localhost:9092",
                "group_id": "test-group",
                "topics": [{"name": "existing.topic", "destination_id": "existing.dest"}],
                "authentication": {
                    "type": "basic",
                    "params": {"username": "user", "password": "pass"},
                },
            },
            "destination": {
                "name": "logger",
                "config": {},
            },
            "engine": {"runner": {"type": "stream"}},
            "streams": [
                {
                    "name": "new_stream",
                    "source": {"topic": "new.topic"},
                    "destination": {"table_id": "project.dataset.new"},
                },
            ],
        }

        # Run the model_validator
        BizonConfig(**config_dict)

        # Verify existing topics were not overwritten
        source_dict = config_dict["source"]
        topics = source_dict.get("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "existing.topic"

        # Verify KafkaSourceConfig preserves the existing topic
        kafka_config = KafkaSourceConfig(**source_dict)
        assert len(kafka_config.topics) == 1
        assert kafka_config.topics[0].name == "existing.topic"


class TestRecordSchemasInjectionFromStreams:
    @pytest.fixture
    def bigquery_config_with_streams(self):
        return {
            "name": "test_bq_pipeline",
            "source": {
                "name": "kafka",
                "stream": "topic",
                "sync_mode": "stream",
                "bootstrap_servers": "localhost:9092",
                "group_id": "test-group",
                "authentication": {
                    "type": "basic",
                    "params": {"username": "user", "password": "pass"},
                },
            },
            "destination": {
                "name": "bigquery_streaming_v2",
                "config": {
                    "project_id": "test-project",
                    "dataset_id": "test_dataset",
                    "unnest": True,
                },
            },
            "engine": {"runner": {"type": "stream"}},
            "streams": [
                {
                    "name": "user_events",
                    "source": {"topic": "events.users.update"},
                    "destination": {
                        "table_id": "project.dataset.users",
                        "record_schema": [
                            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
                        ],
                        "clustering_keys": ["id"],
                    },
                },
            ],
        }

    def test_record_schemas_injected_from_streams(self, bigquery_config_with_streams):
        config = BizonConfig(**bigquery_config_with_streams)

        # Check that record_schemas were injected
        record_schemas = config.destination.config.record_schemas
        assert record_schemas is not None
        assert len(record_schemas) == 1
        assert record_schemas[0].destination_id == "project.dataset.users"
        assert len(record_schemas[0].record_schema) == 2
        assert record_schemas[0].clustering_keys == ["id"]

    def test_unnest_true_works_with_streams_record_schemas(self, bigquery_config_with_streams):
        # This should not raise validation error because record_schemas are injected
        config = BizonConfig(**bigquery_config_with_streams)
        assert config.destination.config.unnest is True
        assert config.destination.config.record_schemas is not None


class TestStreamsConfigFromYaml:
    def test_full_yaml_config(self):
        from bizon.connectors.sources.kafka.src.config import KafkaSourceConfig

        yaml_config = """
        name: kafka_to_bigquery_with_streams

        source:
          name: kafka
          stream: topic
          sync_mode: stream
          bootstrap_servers: localhost:9092
          group_id: test-group
          authentication:
            type: basic
            params:
              username: user
              password: pass

        destination:
          name: bigquery_streaming_v2
          config:
            project_id: my-project
            dataset_id: my_dataset
            unnest: true

        streams:
          - name: user_events
            source:
              topic: events.users.update
            destination:
              table_id: my-project.my_dataset.users
              record_schema:
                - name: id
                  type: STRING
                  mode: REQUIRED
                - name: email
                  type: STRING
                  mode: NULLABLE
              clustering_keys:
                - id

          - name: order_events
            source:
              topic: events.orders.create
            destination:
              table_id: my-project.my_dataset.orders
              record_schema:
                - name: order_id
                  type: INTEGER
                  mode: REQUIRED
                - name: total
                  type: FLOAT
                  mode: NULLABLE
              clustering_keys:
                - order_id

        engine:
          runner:
            type: stream
        """
        config_dict = safe_load(yaml_config)
        config = BizonConfig(**config_dict)

        # Verify streams loaded correctly
        assert len(config.streams) == 2
        assert config.streams[0].name == "user_events"
        assert config.streams[0].source.topic == "events.users.update"
        assert config.streams[1].name == "order_events"

        # Verify topics were injected into source dict
        # (BizonConfig.source is SourceConfig base class, so we check the raw dict)
        source_dict = config_dict["source"]
        topics = source_dict.get("topics")
        assert len(topics) == 2

        # Verify KafkaSourceConfig can parse the injected topics
        kafka_config = KafkaSourceConfig(**source_dict)
        assert len(kafka_config.topics) == 2
        topic_names = [t.name for t in kafka_config.topics]
        assert "events.users.update" in topic_names
        assert "events.orders.create" in topic_names

        # Verify record_schemas were injected
        assert len(config.destination.config.record_schemas) == 2
