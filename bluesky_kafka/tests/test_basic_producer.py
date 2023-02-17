import pytest

from bluesky_kafka.produce import BasicProducer


@pytest.mark.parametrize(
    "bootstrap_servers, producer_config, combined_bootstrap_servers",
    [
        ([], {}, ""),
        (["1.2.3.4:9092"], {}, "1.2.3.4:9092"),
        (["1.2.3.4:9092", "localhost:9092"], {}, "1.2.3.4:9092,localhost:9092"),
    ],
)
def test_bootstrap_servers(
    bootstrap_servers, producer_config, combined_bootstrap_servers
):
    """
    This test targets combining bootstrap servers specified
    with the `bootstrap_servers` parameter and in the `producer_config`.
    """
    topic = "test.producer.config"
    basic_producer = BasicProducer(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        key=None,
        producer_config=producer_config,
    )

    assert (
        basic_producer._producer_config["bootstrap.servers"]
        == combined_bootstrap_servers
    )


def test_bootstrap_servers_in_producer_config():
    """
    This test verifies that ValueError is raised when the `producer_config`
    dictionary includes the `bootstrap.servers` key.
    """
    with pytest.raises(ValueError) as excinfo:
        BasicProducer(
            topic="abc",
            bootstrap_servers=["localhost:9092"],
            key=None,
            producer_config={"bootstrap.servers": ""},
        )

        assert (
            "do not specify 'bootstrap.servers' in producer_config dictionary, "
            "use only the 'bootstrap_servers' parameter" in excinfo.value
        )


def test_bad_bootstrap_servers():
    test_topic = "test.bad.bootstrap.servers"
    with pytest.raises(TypeError) as excinfo:
        BasicProducer(
            topic=test_topic,
            bootstrap_servers="localhost:9091",
            key=None,
            producer_config={
                "bootstrap.servers": "localhost:9092",
            },
        )
        assert (
            "parameter `bootstrap_servers` must be a sequence of str, not str"
            in excinfo.value
        )


def test_redact_password_from_str_output():
    topic = "test.redact.password"
    basic_producer = BasicProducer(
        topic=topic,
        bootstrap_servers=["localhost:9091", "localhost:9092"],
        key="test-redact-password",
        producer_config={
            "sasl.password": "PASSWORD",
        },
    )

    basic_producer_str_output = str(basic_producer)

    assert basic_producer_str_output == (
        "<class 'bluesky_kafka.produce.BasicProducer'>("
        "topic='test.redact.password', "
        "key='test-redact-password', "
        "bootstrap_servers=['localhost:9091', 'localhost:9092'], "
        "producer_config={'sasl.password': '****', 'bootstrap.servers': 'localhost:9091,localhost:9092'}"
        ")"
    )