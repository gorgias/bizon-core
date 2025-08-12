from bizon.connectors.sources.kafka.src.config import KafkaAuthConfig, TopicConfig
from bizon.connectors.sources.kafka.src.source import KafkaSourceConfig


def test_kafka_source_config_timestamp_to_parse():
    conf = KafkaSourceConfig(
        name="kafka",
        stream="topic",
        topics=[
            TopicConfig(name="cookie", destination_id="cookie"),
        ],
        bootstrap_servers="fdjvfv",
        batch_size=87,
        consumer_timeout=56,
        authentication=KafkaAuthConfig(
            type="basic",
            params={"username": "user", "password": "password"},
        ),
    )
    assert conf
