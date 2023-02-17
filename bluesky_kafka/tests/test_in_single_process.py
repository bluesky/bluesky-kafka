"""
    Most bluesky-kafka tests require a Kafka broker.

    Start Kafka and Zookeeper like this:
      $ cd scripts
      $ bash start_kafka.sh
    Stop Kafka and Zookeeper with Ctrl-C.
    Remove Kafka and Zookeeper containers like this:
      $ sudo docker ps -a -q
      78485383ca6f
      8a80fb4a385f
      $ sudo docker stop 78485383ca6f 8a80fb4a385f
      78485383ca6f
      8a80fb4a385f
      $ sudo docker rm 78485383ca6f 8a80fb4a385f
      78485383ca6f
      8a80fb4a385f
    Or remove ALL containers like this:
      $ sudo docker stop $(sudo docker ps -a -q)
      $ sudo docker rm $(sudo docker ps -a -q)
    Use this in difficult cases to remove *all traces* of docker containers:
      $ sudo docker system prune -a
"""
import pickle

import msgpack
import numpy as np
import pytest

from bluesky import RunEngine
from bluesky.plans import count
from event_model import sanitize_doc

from bluesky_kafka import RemoteDispatcher


@pytest.mark.parametrize(
    "serializer, deserializer",
    [(pickle.dumps, pickle.loads), (msgpack.packb, msgpack.unpackb)],
)
def test_publisher_and_consumer(
    kafka_bootstrap_servers,
    temporary_topics,
    publisher_factory,
    consume_documents_from_kafka_until_first_stop_document,
    hw,
    serializer,
    deserializer,
):
    """Test publishing and consuming bluesky documents in Kafka messages.

    Messages will be "consumed" by a `bluesky_kafka.BlueskyConsumer`.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "localhost:9092"
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    publisher_factory: pytest fixture
        fixture-as-a-factory for creating Publishers
    hw: pytest fixture
        ophyd simulated hardware objects
    serializer: function (pytest test parameter)
        function used to serialize bluesky documents in Kafka messages
    deserializer: function (pytest test parameter)
        function used to deserialize bluesky documents from Kafka messages
    """

    with temporary_topics(
        topics=[f"test.publisher.and.consumer.{serializer.__module__}"]
    ) as (topic,):
        failed_deliveries = []
        successful_deliveries = []

        def on_delivery(err, msg):
            if err is None:
                successful_deliveries.append(msg)
            else:
                failed_deliveries.append((err, msg))

        bluesky_publisher = publisher_factory(
            topic=topic,
            key=f"{topic}.key",
            flush_on_stop_doc=True,
            on_delivery=on_delivery,
            serializer=serializer,
        )

        published_bluesky_documents = []

        # this function will store all documents
        # published by the RunEngine in a list
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE = RunEngine()
        RE.subscribe(bluesky_publisher)
        RE.subscribe(store_published_document)

        # include some metadata in the count plan
        # to test numpy serialization
        RE.md = {
            "numpy_data": {"nested": np.array([1, 2, 3])},
            "numpy_scalar": np.float64(3),
            "numpy_array": np.ones((3, 3)),
        }

        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        # expect 4 successful deliveries and 0 failed deliveries
        assert len(successful_deliveries) == 4
        assert len(failed_deliveries) == 0

        # retrieve the documents published as Kafka messages
        consumed_bluesky_documents = (
            consume_documents_from_kafka_until_first_stop_document(
                kafka_topic=topic, deserializer=deserializer
            )
        )

        assert len(published_bluesky_documents) == len(consumed_bluesky_documents)

        # sanitize_doc normalizes some document data, such as numpy arrays, that are
        # problematic for direct comparison of documents by 'assert'
        sanitized_published_bluesky_documents = [
            sanitize_doc(doc) for doc in published_bluesky_documents
        ]
        sanitized_consumed_bluesky_documents = [
            sanitize_doc(doc) for doc in consumed_bluesky_documents
        ]

        assert len(sanitized_consumed_bluesky_documents) == len(
            sanitized_published_bluesky_documents
        )
        assert (
            sanitized_consumed_bluesky_documents
            == sanitized_published_bluesky_documents
        )


@pytest.mark.parametrize(
    "serializer, deserializer",
    [(pickle.dumps, pickle.loads), (msgpack.packb, msgpack.unpackb)],
)
def test_publisher_and_remote_dispatcher(
    kafka_bootstrap_servers,
    temporary_topics,
    publisher_factory,
    hw,
    serializer,
    deserializer,
):
    """Test publishing and dispatching bluesky documents in Kafka messages.

    Messages will be "dispatched" by a `bluesky_kafka.RemoteDispatcher`.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "localhost:9092"
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    publisher_factory: pytest fixture
        fixture-as-a-factory for creating Publishers
    hw: pytest fixture
        ophyd simulated hardware objects
    serializer: function (pytest test parameter)
        function used to serialize bluesky documents in Kafka messages
    deserializer: function (pytest test parameter)
        function used to deserialize bluesky documents from Kafka messages
    """

    with temporary_topics(
        topics=[f"test.publisher.and.remote.dispatcher.{serializer.__module__}"]
    ) as (topic,):
        bluesky_publisher = publisher_factory(
            topic=topic,
            key=f"{topic}.key",
            flush_on_stop_doc=True,
            serializer=serializer,
        )

        published_bluesky_documents = []

        # this function will store all documents
        # published by the RunEngine in a list
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE = RunEngine()
        RE.subscribe(bluesky_publisher)
        RE.subscribe(store_published_document)

        # include some metadata in the count plan
        # to test numpy serialization
        RE.md = {
            "numpy_data": {"nested": np.array([1, 2, 3])},
            "numpy_scalar": np.float64(3),
            "numpy_array": np.ones((3, 3)),
        }

        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        consumer_config = {
            # this consumer is intended to read messages that
            # have already been published, so it is necessary
            # to specify "earliest" here
            "auto.offset.reset": "earliest",
        }

        remote_dispatcher = RemoteDispatcher(
            topics=[topic],
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=f"{topic}.remote.dispatcher.group",
            consumer_config=consumer_config,
            polling_duration=1.0,
            deserializer=deserializer,
        )

        dispatched_bluesky_documents = []

        # this function stores all documents remote_dispatcher
        # gets from the Kafka broker in a list
        def store_dispatched_document(name, document):
            dispatched_bluesky_documents.append((name, document))

        remote_dispatcher.subscribe(store_dispatched_document)

        # this function returns False to end the remote_dispatcher polling loop
        def until_first_stop_document():
            assert len(dispatched_bluesky_documents) <= len(published_bluesky_documents)
            if "stop" in [name for name, _ in dispatched_bluesky_documents]:
                return False
            else:
                return True

        # start() will return when 'until_first_stop_document' returns False
        remote_dispatcher.start(
            continue_polling=until_first_stop_document,
        )

        assert len(published_bluesky_documents) == len(dispatched_bluesky_documents)

        # sanitize_doc normalizes some document data, such as numpy arrays, that are
        # problematic for direct comparison of documents by 'assert'
        sanitized_published_bluesky_documents = [
            sanitize_doc(doc) for doc in published_bluesky_documents
        ]
        sanitized_dispatched_bluesky_documents = [
            sanitize_doc(doc) for doc in dispatched_bluesky_documents
        ]

        assert len(sanitized_dispatched_bluesky_documents) == len(
            sanitized_published_bluesky_documents
        )
        assert (
            sanitized_dispatched_bluesky_documents
            == sanitized_published_bluesky_documents
        )
