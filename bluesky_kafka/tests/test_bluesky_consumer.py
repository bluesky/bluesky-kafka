import pytest

from bluesky_kafka import BlueskyConsumer


def test_consumer_config():
    """
    This test targets combining bootstrap servers specified
    with the `bootstrap_servers` parameter and in the `consumer_config`.
    """
    test_topic = "test.consumer.config"
    bluesky_consumer = BlueskyConsumer(
        topics=[test_topic],
        bootstrap_servers="1.2.3.4:9092",
        group_id="abc",
        consumer_config={
            "bootstrap.servers": "5.6.7.8:9092",
            "auto.offset.reset": "latest",
        },
    )

    assert (
        bluesky_consumer._consumer_config["bootstrap.servers"]
        == "1.2.3.4:9092,5.6.7.8:9092"
    )


def test_bad_consumer_config():
    test_topic = "test.bad.consumer.config"
    with pytest.raises(ValueError) as excinfo:
        BlueskyConsumer(
            topics=[test_topic],
            bootstrap_servers="1.2.3.4:9092",
            group_id="abc",
            consumer_config={
                "bootstrap.servers": "5.6.7.8:9092",
                "auto.offset.reset": "latest",
                "group.id": "raise an exception!",
            },
        )
        assert (
            "do not specify 'group.id' in consumer_config, use only the 'group_id' argument"
            in excinfo.value
        )
