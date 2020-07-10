import ophyd.sim
import pytest
import msgpack
import msgpack_numpy as mpn

from functools import partial
from bluesky.tests.conftest import RE


TEST_TOPIC = "bluesky-kafka-test"

# get bootstrap server IP from command line
def pytest_addoption(parser):
    parser.addoption(
        "--kafka-bootstrap-servers",
        action="store",
        default="127.0.0.1:9092",
        help="comma-separated list of address:port for Kafka bootstrap servers",
    )


@pytest.fixture
def bootstrap_servers(request):
    print(request)
    return request.config.getoption("--kafka-bootstrap-servers")


@pytest.fixture(scope="function")
def hw(request):
    return ophyd.sim.hw()


@pytest.fixture(scope="module")
def msgpack_serializer(request):


@pytest.fixture(scope="module")
def msgpack_deserializer(request):


@pytest.fixture(scope="module")
def publisher(request, bootstrap_servers, msgpack_serializer):
    return Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers=bootstrap_servers,
        key="kafka-unit-test-key",
        # work with a single broker
        producer_config={
            "acks": 1,
            "enable.idempotence": False,
            "request.timeout.ms": 5000,
        },
        serializer=msgpack_serializer,
    )


@pytest.fixture(scope="module")
def mongo_client(request):
    mongobox = pytest.importorskip('mongobox')
    box = mongobox.MongoBox()
    box.start()
    return box.client()


@pytest.fixture(scope="module")
def mongo_uri(request, mongo_client):
    return f"mongodb://{client.address[0]}:{client.address[1]}/{TEST_TOPIC}"


@pytest.fixture(scope="module")
def mongo_consumer(request, bootstrap_servers, msgpack_deserializer, mongo_client):
    return MongoBlueskyConsumer(
            topics=[TEST_TOPIC],
            bootstrap_servers=bootstrap_servers,
            group_id="kafka-unit-test-group-id",
            mongo_uri=mongo_uri,
            # "latest" should always work but
            # has been failing on Linux, passing on OSX
            consumer_config={"auto.offset.reset": auto_offset_reset},
            polling_duration=1.0,
            deserializer=msgpack_deserializer,
        )
