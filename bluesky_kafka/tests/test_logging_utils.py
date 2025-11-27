from bluesky_kafka.logging_utils import redact_config


def test_redact_config():
    example_config = {
        "bootstrap.servers": "some-kafka-server:9092",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "brokerUser",
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/opt/kafka/config/certs/kafka-tls-ca",
        "sasl.password": "SECRET PASSWORD",
        "acks": "all",  # Producer key
        "enable.idempotence": "false",
        "group.id": "some-group",  # Consumer key
    }

    reacted_example_config = redact_config(example_config)

    assert reacted_example_config == {
        "bootstrap.servers": "some-kafka-server:9092",
        "sasl.mechanisms": "****",
        "sasl.username": "****",
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "****",
        "sasl.password": "****",
        "acks": "all",
        "enable.idempotence": "false",
        "group.id": "some-group",
    }
