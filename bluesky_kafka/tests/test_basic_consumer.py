import pytest

from bluesky_kafka.consume import BasicConsumer


@pytest.mark.parametrize(
    "bootstrap_servers, consumer_config, combined_bootstrap_servers",
    [
        ([], {}, ""),
        (["1.2.3.4:9092"], {}, "1.2.3.4:9092"),
        (["1.2.3.4:9092", "localhost:9092"], {}, "1.2.3.4:9092,localhost:9092"),
        ([], {"bootstrap.servers": "10.11.12.13:9092"}, "10.11.12.13:9092"),
        (
            [],
            {"bootstrap.servers": "10.11.12.13:9092,10.11.12.14:9092"},
            "10.11.12.13:9092,10.11.12.14:9092",
        ),
        (
            ["1.2.3.4:9092"],
            {"bootstrap.servers": "10.11.12.13:9092"},
            "1.2.3.4:9092,10.11.12.13:9092",
        ),
        (
            ["1.2.3.4:9092", "localhost:9092"],
            {"bootstrap.servers": "10.11.12.13:9092,10.11.12.14:9092"},
            "1.2.3.4:9092,localhost:9092,10.11.12.13:9092,10.11.12.14:9092",
        ),
    ],
)
def test_bootstrap_servers(
    bootstrap_servers, consumer_config, combined_bootstrap_servers
):
    """
    This test targets combining bootstrap servers specified
    with the `bootstrap_servers` parameter and in the `consumer_config`.
    """
    test_topic = "test.consumer.config"
    bluesky_consumer = BasicConsumer(
        topics=[test_topic],
        bootstrap_servers=bootstrap_servers,
        group_id="abc",
        consumer_config=consumer_config,
    )

    assert (
        bluesky_consumer._consumer_config["bootstrap.servers"]
        == combined_bootstrap_servers
    )


def test_bad_bootstrap_servers():
    test_topic = "test.bad.bootstrap.servers"
    with pytest.raises(TypeError) as excinfo:
        BasicConsumer(
            topics=[test_topic],
            bootstrap_servers="1.2.3.4:9092",
            group_id="abc",
            consumer_config={
                "bootstrap.servers": "5.6.7.8:9092",
                "auto.offset.reset": "latest",
            },
        )
        assert (
            "parameter `bootstrap_servers` must be a sequence of str, not str"
            in excinfo.value
        )


def test_bad_consumer_config():
    test_topic = "test.bad.consumer.config"
    with pytest.raises(ValueError) as excinfo:
        BasicConsumer(
            topics=[test_topic],
            bootstrap_servers=["1.2.3.4:9092"],
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


def test_redact_password_from_str():
    test_topic = "test.redact_password_from_str"
    basic_consumer = BasicConsumer(
        topics=[test_topic],
        bootstrap_servers=["1.2.3.4:9092"],
        group_id="abc",
        consumer_config={
            "sasl.password": "PASSWORD",
        },
    )
    assert (
        "PASSWORD" not in str(basic_consumer)
    )
    assert (
        "****" in str(basic_consumer)
    )
