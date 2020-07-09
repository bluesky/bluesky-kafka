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
