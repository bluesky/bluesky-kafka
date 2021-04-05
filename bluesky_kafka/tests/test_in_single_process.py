import logging
import pickle

import msgpack
import numpy as np
import pytest

from bluesky import RunEngine
from bluesky.plans import count
from event_model import sanitize_doc

from bluesky_kafka import Publisher, BlueskyConsumer, RemoteDispatcher


test_log = logging.getLogger("bluesky.kafka.test")


@pytest.mark.parametrize(
    "serializer, deserializer",
    [(pickle.dumps, pickle.loads), (msgpack.packb, msgpack.unpackb)],
)
def test_publisher_and_consumer(
    kafka_bootstrap_servers, temporary_topics, publisher_factory, hw, serializer, deserializer
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

        bluesky_publisher = publisher_factory(
            topic=topic,
            key=f"{topic}.key",
            flush_on_stop_doc=True,
            serializer=serializer
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
        md = {
            "numpy_data": {"nested": np.array([1, 2, 3])},
            "numpy_scalar": np.float64(3),
            "numpy_array": np.ones((3, 3)),
        }

        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        consumed_bluesky_documents = []

        # this function stores all documents bluesky_consumer
        # gets from the Kafka broker in a list
        def store_consumed_document(consumer, topic, name, document):
            consumed_bluesky_documents.append((name, document))

        bluesky_consumer = BlueskyConsumer(
            topics=[topic],
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=f"{topic}.consumer.group",
            consumer_config={
                # it is important to set a short time interval
                # for automatic commits or the Kafka broker may
                # not be notified by the consumer that messages
                # were received before the test ends; the result
                # is that the Kafka broker will try to re-deliver
                # those messages to the next consumer that subscribes
                # to the same topic(s)
                "auto.commit.interval.ms": 100,
                # this consumer is intended to read messages that
                # have already been published, so it is necessary
                # to specify "earliest" here
                "auto.offset.reset": "earliest",
            },
            process_document=store_consumed_document,
            polling_duration=1.0,
            deserializer=deserializer,
        )

        # this function returns False to end the bluesky_consumer polling loop
        def until_first_stop_document():
            assert len(consumed_bluesky_documents) <= len(published_bluesky_documents)
            if "stop" in [name for name, _ in consumed_bluesky_documents]:
                return False
            else:
                return True

        # start() will return when 'until_first_stop_document' returns False
        bluesky_consumer.start(
            continue_polling=until_first_stop_document,
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
    kafka_bootstrap_servers, temporary_topics, publisher_factory, hw, serializer, deserializer
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
            serializer=serializer
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
        md = {
            "numpy_data": {"nested": np.array([1, 2, 3])},
            "numpy_scalar": np.float64(3),
            "numpy_array": np.ones((3, 3)),
        }

        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        remote_dispatcher = RemoteDispatcher(
            topics=[topic],
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=f"{topic}.consumer.group",
            consumer_config={
                # it is important to set a short time interval
                # for automatic commits or the Kafka broker may
                # not be notified by the consumer that messages
                # were received before the test ends; the result
                # is that the Kafka broker will try to re-deliver
                # those messages to the next consumer that subscribes
                # to the same topic(s)
                "auto.commit.interval.ms": 100,
                # this consumer is intended to read messages that
                # have already been published, so it is necessary
                # to specify "earliest" here
                "auto.offset.reset": "earliest",
            },
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
