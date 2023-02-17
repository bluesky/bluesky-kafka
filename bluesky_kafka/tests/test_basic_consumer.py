import pytest

from bluesky_kafka.consume import BasicConsumer


@pytest.mark.parametrize(
    "bootstrap_servers, consumer_config_bootstrap_servers",
    [
        ([], ""),
        (["1.2.3.4:9092"], "1.2.3.4:9092"),
        (["1.2.3.4:9092", "localhost:9092"], "1.2.3.4:9092,localhost:9092"),
    ],
)
def test_bootstrap_servers(bootstrap_servers, consumer_config_bootstrap_servers):
    """
    This test targets combining bootstrap servers specified
    with the `bootstrap_servers` parameter and in the `consumer_config`.
    """
    bluesky_consumer = BasicConsumer(
        topics=["abc"],
        bootstrap_servers=bootstrap_servers,
        group_id="abc",
        consumer_config={},
    )

    assert (
        bluesky_consumer._consumer_config["bootstrap.servers"]
        == consumer_config_bootstrap_servers
    )


def test_bootstrap_servers_in_consumer_config():
    """
    This test verifies that ValueError is raised when the `consumer_config`
    dictionary includes the `bootstrap.servers` key.
    """
    with pytest.raises(ValueError) as excinfo:
        BasicConsumer(
            topics=["abc"],
            bootstrap_servers=["localhost:9092"],
            group_id="abc",
            consumer_config={"bootstrap.servers": ""},
        )

        assert (
            "do not specify 'bootstrap.servers' in consumer_config dictionary, "
            "use only the 'bootstrap_servers' parameter" in excinfo.value
        )


def test_bootstrap_servers_not_list():
    with pytest.raises(TypeError) as excinfo:
        BasicConsumer(
            topics=["abc"],
            bootstrap_servers="1.2.3.4:9092",
            group_id="abc",
            consumer_config={},
        )
        assert (
            "parameter `bootstrap_servers` must be a sequence of str, not str"
            in excinfo.value
        )


def test_bad_consumer_config():
    with pytest.raises(ValueError) as excinfo:
        BasicConsumer(
            topics=["abc"],
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
    basic_consumer = BasicConsumer(
        topics=["abc"],
        bootstrap_servers=["localhost:9091", "localhost:9092"],
        group_id="abc",
        consumer_config={
            "sasl.password": "PASSWORD",
        },
    )

    assert str(basic_consumer) == (
        "<class 'bluesky_kafka.consume.BasicConsumer'>("
        "topics=['abc'], "
        "consumer_config={"
            "'sasl.password': '****', "
            "'group.id': 'abc', "
            "'bootstrap.servers': 'localhost:9091,localhost:9092'}"
        ")"
    )