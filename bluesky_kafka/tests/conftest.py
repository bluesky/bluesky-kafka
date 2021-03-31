from contextlib import contextmanager
import os
import tempfile

import intake
import numpy as np
import pytest
import yaml

from bluesky.tests.conftest import RE  # noqa
from ophyd.tests.conftest import hw  # noqa

from bluesky_kafka import Publisher
from bluesky_kafka.utils import create_topics, delete_topics


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


@pytest.fixture(scope="function")
def kafka_bootstrap_servers(request):
    """
    Return a comma-delimited string of Kafka bootstrap server host:port specified
    on the pytest command line with option --kafka-bootstrap-servers.

    Parameters
    ----------
    request : pytest request fixture

    Returns
    -------
    comma-delimited string of Kafka bootstrap server host:port
    """
    return request.config.getoption("--kafka-bootstrap-servers")


@pytest.fixture(scope="function")
def temporary_topics(kafka_bootstrap_servers):
    """
    Use this "factory as a fixture and context manager" to cleanly
    create new topics and delete them after a test.

    If `bootstrap_servers` is not specified to the factory function
    then the `kafka_bootstrap_servers` fixture will be used.

    Parameters
    ----------
    kafka_bootstrap_servers : pytest fixture
        comma-delimited str of Kafka bootstrap server host:port specified on the pytest command line

    """

    @contextmanager
    def _temporary_topics(topics, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = kafka_bootstrap_servers

        try:
            # delete existing requested topics
            # this will delete any un-consumed messages
            # the intention is to make tests repeatable by ensuring
            # they always start with a topics having no "old" messages
            delete_topics(
                bootstrap_servers=bootstrap_servers, topics_to_delete=topics
            )
            create_topics(
                bootstrap_servers=bootstrap_servers, topics_to_create=topics
            )
            yield topics
        finally:
            delete_topics(
                bootstrap_servers=bootstrap_servers, topics_to_delete=topics
            )

    return _temporary_topics


@pytest.fixture(scope="function")
def publisher_factory(kafka_bootstrap_servers):
    """
    Use this "factory as a fixture" to create one or more Publishers in a test function.
    If `bootstrap_servers` is not specified to the factory function then the `kafka_bootstrap_servers`
    fixture will be used. The `serializer` parameter can be passed through **kwargs of the factory function.

    For example:

        def test_something(publisher_factory):
            publisher_abc = publisher_factory(topic="abc")
            publisher_xyz = publisher_factory(topic="xyz", serializer=pickle.dumps)
            ...

    Parameters
    ----------
    kafka_bootstrap_servers : pytest fixture
        comma-delimited str of Kafka bootstrap server host:port specified on the pytest command line

    Returns
    -------
    _publisher_factory : function(topic, key, producer_config, flush_on_stop_doc, **kwargs)
        a factory function returning bluesky_kafka.Publisher instances constructed with the
        specified arguments
    """

    def _publisher_factory(
        topic,
        bootstrap_servers=None,
        key=None,
        producer_config=None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        topic : str
            Topic to which all messages will be published.
        bootstrap_servers: str
            Comma-delimited list of Kafka server addresses as a string such as ``'127.0.0.1:9092'``;
            default is the value of the pytest command line parameter --kafka-bootstrap-servers
        key : str
            Kafka "key" string. Specify a key to maintain message order. If None is specified
            no ordering will be imposed on messages.
        producer_config : dict, optional
            Dictionary configuration information used to construct the underlying Kafka Producer.
        **kwargs
            **kwargs will be passed to bluesky_kafka.Publisher() and may include on_delivery,
            flush_on_stop_doc, and serializer

        Returns
        -------
        publisher : bluesky_kafka.Publisher
            a Publisher instance constructed with the specified arguments
        """
        if bootstrap_servers is None:
            bootstrap_servers = kafka_bootstrap_servers

        if producer_config is None:
            # this default configuration is not guaranteed
            # to be generally appropriate
            producer_config = {
                "acks": 1,
                "enable.idempotence": False,
                "request.timeout.ms": 1000,
            }

        return Publisher(
            topic=topic,
            key=key,
            bootstrap_servers=bootstrap_servers,
            producer_config=producer_config,
            **kwargs,
        )

    return _publisher_factory


@pytest.fixture(scope="function")
def publisher(request, kafka_bootstrap_servers):
    return Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers=kafka_bootstrap_servers,
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
def publisher2(request, kafka_bootstrap_servers):
    return Publisher(
        topic=TEST_TOPIC2,
        bootstrap_servers=kafka_bootstrap_servers,
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
