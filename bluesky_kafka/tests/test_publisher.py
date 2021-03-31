from bluesky_kafka import Publisher


def test_producer_config():
    """
    This test targets combining bootstrap servers specified
    with the `bootstrap_servers` parameter and in the `producer_config`.
    """
    topic = "test.producer.config"
    publisher = Publisher(
        topic=topic,
        bootstrap_servers="1.2.3.4:9092",
        key="test.producer.config",
        # work with a single broker
        producer_config={
            "bootstrap.servers": "5.6.7.8:9092",
            "acks": 1,
            "enable.idempotence": False,
            "request.timeout.ms": 5000,
        },
    )

    assert (
        publisher._producer_config["bootstrap.servers"] == "1.2.3.4:9092,5.6.7.8:9092"
    )
