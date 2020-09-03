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
        topic, bootstrap_servers=None, key="pytest", producer_config=None, **kwargs,
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


@pytest.fixture
def dispatcher_factory(kafka_bootstrap_servers):
    """
    Use this "factory as a fixture" to create bluesky_kafka.RemoteDispatchers
    in a test function or fixture.

    Parameters
    ----------
    kafka_bootstrap_servers: pytest fixture (str)
        comma-separated list of host:port

    Returns
    -------
    _dispatcher_factory : function(topics, group_id, bootstrap_servers, consumer_config, **kwargs)
        factory function returning bluesky_kafka.RemoteDispatcher instances constructed with
        the specified arguments
    """

    def _dispatcher_factory(
        topics,
        group_id="pytest",
        bootstrap_servers=None,
        consumer_config=None,
        **kwargs,
    ):
        """
        Construct and return a bluesky_kafka.RemoteDispatcher with the specified arguments.

        Parameters
        ----------
        topics: list of str, required
            the underlying Kafka consumer will subscribe to the specified topics
        group_id: str, optional
            the underlying Kafka consumer will have the specified group_id, "pytest" by default
        bootstrap_servers : str, optional
            comma-delimited str of Kafka bootstrap server host:port specified on the pytest command line;
            default is the value of the pytest command line parameter --kafka-bootstrap-servers
        consumer_config: dict, optional
            the underlying Kafka consumer will be created with the specified configuration parameters;
            it is recommended that consumer configuration include "auto.commit.interval.ms": 100
        kwargs
            kwargs will be passed to bluesky_kafka.RemoteDispatcher()

        Returns
        -------
        remote_dispatcher : bluesky_kafka.RemoteDispatcher
            instance of bluesky_kafka.RemoteDispatcher constructed with the specified arguments
        """
        if bootstrap_servers is None:
            bootstrap_servers = kafka_bootstrap_servers

        if consumer_config is None:
            consumer_config = {
                # it is important to set a short time interval
                # for automatic commits or the Kafka broker may
                # not be notified by the consumer that messages
                # were received before the test ends; the result
                # is that the Kafka broker will try to re-deliver
                # those messages to the next consumer that subscribes
                # to the same topic(s)
                "auto.commit.interval.ms": 100,
            }

        remote_dispatcher = RemoteDispatcher(
            topics=topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            consumer_config=consumer_config,
            **kwargs,
        )
        return remote_dispatcher

    return _dispatcher_factory


@pytest.fixture(scope="function")
def consumer_process_factory(kafka_bootstrap_servers):
    """
    Use this "factory as a fixture" to create multiprocessing.Process running
    a Kafka consumer polling loop in a test function.

    Parameters
    ----------
    kafka_bootstrap_servers: pytest fixture (str)
        comma-separated list of host:port

    Returns
    -------
    _consumer_process_factory :
            function(consumer_factory, topics, group_id, consumer_config, document_queue, **kwargs)
        factory function returning a multiprocessing.Process that will run a Kafka
        consumer polling loop

    """

    def _consumer_process_factory(
        consumer_factory, topics, group_id, consumer_config, document_queue, **kwargs
    ):
        """
        Parameters
        ----------
        consumer_factory : function(topics, group_id, consumer_config, **kwargs)
            a factory function (or callable) returning a BlueskyConsumer-like object
        topics : list of str, required
            the underlying Kafka consumer will subscribe to the specified topics
        group_id : str, optional
            the underlying Kafka consumer will have the specified group_id, "pytest" by default
        consumer_config : dict, optional
            the underlying Kafka consumer will be created with the specified configuration parameters
        document_queue : multiprocessing.Queue
            the underlying Kafka consumer will place documents it receives in this queue
        kwargs
            kwargs will be passed to the consumer_factory to be used in constructing the
            underlying Kafka consumer

        Returns
        -------
        document_queue: multiprocessing.Queue
            this queue will contain bluesky (name, document) pairs that were delivered
            to the underlying Kafka consumer
        """
        if consumer_config is None:
            consumer_config = {
                # it is important to set a short time interval
                # for automatic commits or the Kafka broker may
                # not be notified by the consumer that messages
                # were received before the test ends; the result
                # is that the Kafka broker will try to re-deliver
                # those messages to the next consumer that subscribes
                # to the same topic(s)
                "auto.commit.interval.ms": 100,
            }

        # this function will run in the external process created below
        def start_consumer_with_queue(document_queue_):
            logger = multiprocessing.get_logger()
            logger.warning("constructing consumer process with inter-process queue")

            def put_document_in_queue(consumer, topic, name, doc):
                logger.warning("BlueskyConsumer putting %s in queue", name)
                document_queue_.put((name, doc))

            # it is important the BlueskyConsumer be
            # constructed in the external process
            bluesky_consumer_ = consumer_factory(
                topics=topics,
                bootstrap_servers=kafka_bootstrap_servers,
                group_id=group_id,
                consumer_config=consumer_config,
                process_document=put_document_in_queue,
                **kwargs,
            )
            # consume messages published by a Kafka broker
            bluesky_consumer_.start()

        # create an external process for the bluesky_kafka.BlueskyConsumer polling loop
        # but do not start it, the client of this function will start the process
        consumer_process = multiprocessing.Process(
            target=start_consumer_with_queue, args=(document_queue,), daemon=True,
        )

        return consumer_process

    return _consumer_process_factory


@pytest.fixture(scope="function")
def remote_dispatcher_process_factory(dispatcher_factory):
    def _remote_dispatcher_process_factory(
        topics, group_id, consumer_config, document_queue, **kwargs
    ):
        # this function will run in the external process created below
        def start_remote_dispatcher_with_queue(document_queue_):
            logger = multiprocessing.get_logger()

            def put_document_in_queue(name, doc):
                logger.warning("putting %s in queue", name)
                document_queue_.put((name, doc))

            # it is important the RemoteDispatcher be
            # constructed inside the external process
            remote_dispatcher_ = dispatcher_factory(
                topics=topics,
                group_id=group_id,
                consumer_config=consumer_config,
                **kwargs,
            )
            # consume messages published by a Kafka broker
            remote_dispatcher_.subscribe(put_document_in_queue)
            remote_dispatcher_.start()

        # create an external process for the bluesky_kafka.RemoteDispatcher polling loop
        # do not start it, the client of this function will start the process
        remote_dispatcher_process = multiprocessing.Process(
            target=start_remote_dispatcher_with_queue,
            args=(document_queue,),
            daemon=True,
        )

        return remote_dispatcher_process

    return _remote_dispatcher_process_factory


@pytest.fixture(scope="function")
def external_process_document_queue():
    """
    This is a pytest "factory as a fixture" intended to be used for
    testing that Kafka messages containing bluesky documents are
    produced and consumed.

    It returns
        a factory function that returns
            a context manager that sets up and tears down
                a daemon multiprocessing.Process running
                    a Kafka consumer polling loop that inserts documents into
                        a multiprocessing.Queue

    Usage:
        def test_kafka_messages(external_process_document_queue):

            with external_process_document_queue(
                topics=["topic_1", "topic_2", ...]
            ) as document_queue:
                ... set up a Kafka Producer ...

                ... produce documents with Kafka ...

                published_documents = get_all_documents_from_queue(document_queue)

                ... assert expected results ...

    Returns
    -------
    _external_process_document_queue: contextmanager
        a context manager that sets up and tears down a multiprocessing.Process
        intended to run a Kafka consumer of some kind appending documents published
        by a Kafka broker to a multiprocessing.Queue
    """

    @contextmanager
    def _external_process_document_queue(
        topics,
        group_id="pytest",
        consumer_config=None,
        process_factory=consumer_process_factory,
        **kwargs,
    ):
        """
        This function is a context manager that sets up and tears down a
        multiprocessing.Process running some kind of Kafka consumer which
        places documents published by a Kafka broker in a multiprocessing.Queue.

        Parameters
        ----------
        topics : list of str, required
            the underlying Kafka consumer will subscribe to the specified topics
        group_id : str, optional
            the underlying Kafka consumer will have the specified group_id, "pytest" by default
        consumer_config : dict, optional
            the underlying Kafka consumer will be created with the specified configuration parameters
        process_factory : function(topics, group_id, consumer_config, document_queue, **kwargs), optional
            a factory function returning a multiprocessing.Queue that runs a Kafka consumer
        kwargs
            kwargs will be passed to bluesky_kafka.RemoteDispatcher.__init__()

        Yields
        -------
        document_queue: multiprocessing.Queue
            this queue will contain bluesky (name, document) pairs that were
            delivered as messages with the specified topics to a Kafka consumer
        """
        document_queue = multiprocessing.Queue()

        process = process_factory(
            topics=topics,
            group_id=group_id,
            consumer_config=consumer_config,
            document_queue=document_queue,
            **kwargs,
        )
        process.start()
        # it is important to give the process time to start
        # less than 10 seconds has not been enough
        time.sleep(10)

        try:
            yield document_queue
        finally:
            process.terminate()
            process.join()

    return _external_process_document_queue


def get_all_documents_from_queue(document_queue):
    """
    Get all bluesky (name, document) pairs placed in a multiprocessing.Queue.

    The queue will be read until it is empty. The intention is that no Kafka
    (name, document) pairs will be added to the queue while this function runs.

    Parameters
    ----------
    document_queue : multiprocessing.Queue
        a multiprocessing.Queue yielded by external_process_document_queue

    Returns
    -------
    documents : list
        a list of all (name, document) pairs read from the specified queue
    """
    documents = list()
    while True:
        try:
            name, doc = document_queue.get(timeout=1)
            documents.append((name, doc))
        except queue.Empty:
            break

    return documents


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
