from contextlib import contextmanager
import multiprocessing
import os
import queue
import tempfile
import time

import intake
import numpy as np
import pytest
import yaml

from bluesky_kafka import Publisher, RemoteDispatcher
from bluesky.tests.conftest import RE  # noqa
from ophyd.tests.conftest import hw  # noqa

TEST_TOPIC = "bluesky-kafka-test"
TEST_TOPIC2 = "bluesky2-kafka-test"


def pytest_addoption(parser):
    """
    Add `--kafka-bootstrap-servers` to the pytest command line parser.
    """
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
def publisher(request, bootstrap_servers):
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
        flush_on_stop_doc=True,
    )


@pytest.fixture(scope="function")
def publisher2(request, bootstrap_servers):
    return Publisher(
        topic=TEST_TOPIC2,
        bootstrap_servers=bootstrap_servers,
        key="kafka-unit-test-key",
        # work with a single broker
        producer_config={
            "acks": 1,
            "enable.idempotence": False,
            "request.timeout.ms": 5000,
        },
        flush_on_stop_doc=True,
    )


@pytest.fixture(scope="function")
def mongo_client(request):
    mongobox = pytest.importorskip("mongobox")
    box = mongobox.MongoBox()
    box.start()
    return box.client()


@pytest.fixture(scope="function")
def mongo_uri(request, mongo_client):
    return f"mongodb://{mongo_client.address[0]}:{mongo_client.address[1]}"


@pytest.fixture(scope="function")
def numpy_md(request):
    return {
        "numpy_data": {"nested": np.array([1, 2, 3])},
        "numpy_scalar": np.float64(3),
        "numpy_array": np.ones((3, 3)),
    }


@pytest.fixture(scope="function")
def data_broker(request, mongo_uri):
    TMP_DIR = tempfile.mkdtemp()
    YAML_FILENAME = "intake_test_catalog.yml"

    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    # Write a catalog file.
    with open(fullname, "w") as f:
        f.write(
            f"""
sources:
  xyz:
    description: Some imaginary beamline
    driver: "bluesky-mongo-normalized-catalog"
    container: catalog
    args:
      metadatastore_db: {mongo_uri}/{TEST_TOPIC}
      asset_registry_db: {mongo_uri}/{TEST_TOPIC}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
  xyz2:
    description: Some imaginary beamline
    driver: "bluesky-mongo-normalized-catalog"
    container: catalog
    args:
      metadatastore_db: {mongo_uri}/{TEST_TOPIC2}
      asset_registry_db: {mongo_uri}/{TEST_TOPIC2}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
                """
        )

    def load_config(filename):
        package_directory = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(package_directory, filename)
        with open(filename) as f:
            return yaml.load(f, Loader=getattr(yaml, "FullLoader", yaml.Loader))

    # Create a databroker with the catalog config file.
    return intake.open_catalog(fullname)
