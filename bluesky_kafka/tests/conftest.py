import msgpack
import msgpack_numpy as mpn
import ophyd.sim
import pytest
import tempfile

from functools import partial
from bluesky.tests.conftest import RE
from bluesky_kafka import Publisher, MongoBlueskyConsumer
from databroker.v1 import from_config


TEST_TOPIC = "bluesky-kafka-test"
TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_PATH = [TMP_DIR]
YAML_FILENAME = 'intake_test_catalog.yml'


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
def publisher(request, bootstrap_servers, msgpack_serializer, test):
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
    return f"mongodb://{mongo_client.address[0]}:{mongo_client.address[1]}/{TEST_TOPIC}"


@pytest.fixture(scope="module")
def mongo_consumer(request, bootstrap_servers, msgpack_deserializer, mongo_client):
    return MongoBlueskyConsumer(
            topics=["^*-test"],
            bootstrap_servers=bootstrap_servers,
            group_id="kafka-unit-test-group-id",
            mongo_uri=mongo_uri,
            # "latest" should always work but
            # has been failing on Linux, passing on OSX
            consumer_config={"auto.offset.reset": auto_offset_reset},
            polling_duration=1.0,
            deserializer=msgpack_deserializer,
        )


@pytest.fixture(scope="module")
def md(request):
    return {"numpy_data": {"nested": np.array([1, 2, 3])},
            "numpy_scalar": np.float64(3),
            "numpy_array": np.ones((3, 3))}


@pytest.fixture(scope="module")
def broker(request, mongo_uri):
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    # Write a catalog file.
    with open(fullname, 'w') as f:
        f.write(f'''
sources:
  xyz:
    description: Some imaginary beamline
    driver: "bluesky-mongo-normalized-catalog"
    container: catalog
    args:
      metadatastore_db: {mongo_uri}
      asset_registry_db: {mongo_uri}
      handler_registry:
        NPY_SEQ: ophyd.sim.NumpySeqHandler
    metadata:
      beamline: "00-ID"
                ''')

    def load_config(filename):
        package_directory = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(package_directory, filename)
        with open(filename) as f:
            return yaml.load(f, Loader=getattr(yaml, 'FullLoader', yaml.Loader))

    # Create a databroker with the catalog config file.
    return from_config(load_config(fullname)).v2
