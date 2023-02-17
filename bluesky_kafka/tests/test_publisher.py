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
        producer_config={
            "bootstrap.servers": "5.6.7.8:9092",
        },
    )

    assert (
        publisher._producer_config["bootstrap.servers"] == "1.2.3.4:9092,5.6.7.8:9092"
    )


def test_redact_password_from_str_output():
    topic = "test.redact.password"
    publisher = Publisher(
        topic=topic,
        bootstrap_servers="1.2.3.4:9092",
        key="test-redact-password-key",
        producer_config={
            "sasl.password": "PASSWORD",
        },
    )

    publisher_str_output = str(publisher)
    assert "PASSWORD" not in publisher_str_output
    assert "sasl.password" in publisher_str_output
    assert "****" in publisher_str_output
