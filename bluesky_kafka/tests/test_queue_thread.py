import logging
import re
import queue
import threading
import time as ttime
import uuid

from unittest.mock import Mock

from bluesky.plans import count
from event_model import sanitize_doc

from bluesky_kafka import BlueskyKafkaException
from bluesky_kafka.tools.queue_thread import (
    build_and_start_kafka_publisher_thread,
    start_kafka_publisher_thread,
)


def test_build_and_start_kafka_publisher_thread(
    kafka_bootstrap_servers,
    temporary_topics,
    consume_documents_from_kafka_until_first_stop_document,
    RE,
    hw,
):
    """Test publishing Kafka messages from a thread.

    This test follows the pattern in bluesky_kafka/tests/test_in_single_process.py,
    which is to publish Kafka messages _before_ subscribing a Kafka consumer to
    those messages. After the messages have been published a consumer is subscribed
    to the topic and should receive all messages since they will have been cached by
    the Kafka broker(s). This keeps the test code relatively simple.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "kafka1:9092,kafka2:9092"
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    consume_documents_from_kafka_until_first_stop_document: pytest fixture
        a pytest fixture that runs a Kafka consumer until the first Kafka message
        containing a stop document is encountered
    RE: pytest fixture
        bluesky RunEngine
    hw: pytest fixture
        ophyd simulated hardware objects
    """

    # use a random string as the beamline name so topics will not be duplicated across tests
    beamline_name = str(uuid.uuid4())[:8]
    with temporary_topics(topics=[f"{beamline_name}.bluesky.runengine.documents"]) as (
        beamline_topic,
    ):
        (
            kafka_publisher_queue,
            kafka_publisher_thread_exit_event,
        ) = build_and_start_kafka_publisher_thread(
            topic=beamline_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config={},
            #     "acks": "all",
            #     "enable.idempotence": False,
            #     "request.timeout.ms": 1000,
            # },
        )

        assert isinstance(kafka_publisher_queue, queue.Queue)
        assert isinstance(kafka_publisher_thread_exit_event, threading.Event)

        published_bluesky_documents = []

        # store all documents published directly by the RunEngine
        # for comparison with documents published by Kafka
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE.subscribe(store_published_document)

        # put each document published by the RunEngine
        # on the kafka_publisher_queue
        def put_document_on_publisher_queue(name, doc):
            kafka_publisher_queue.put((name, doc))

        RE.subscribe(put_document_on_publisher_queue)

        # all documents created by this plan are expected
        # to be published as Kafka messages
        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        # retrieve the documents published as Kafka messages
        consumed_bluesky_documents = consume_documents_from_kafka_until_first_stop_document(
            kafka_topic=beamline_topic
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


def test_no_beamline_topic(caplog, kafka_bootstrap_servers, RE):
    """ Test the case of a topic that does not exist in the Kafka broker.

    If the beamline Kafka topic does not exist then an exception
    should be raised and handled by writing an exception message
    to the bluesky_kafka logger.

    Parameters
    ----------
    caplog: built-in pytest fixture
        logging output will be captured by this fixture
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "kafka1:9092,kafka2:9092"
    RE: RunEngine (pytest fixture)
        bluesky RunEngine
    """

    with caplog.at_level(level=logging.ERROR, logger="bluesky_kafka"):
        # use a random string as the beamline name so topics will not be duplicated across tests
        beamline_name = str(uuid.uuid4())[:8]
        beamline_topic = f"{beamline_name}.bluesky.runengine.documents"
        build_and_start_kafka_publisher_thread(
            topic=beamline_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config={},
            #     "acks": "all",
            #     "enable.idempotence": False,
            #     "request.timeout.ms": 1000,
            # },
        )

        assert (
            f"topic `{beamline_topic}` does not exist on Kafka broker(s)" in caplog.text
        )


def test_subscribe_kafka_publisher(caplog, temporary_topics, RE):
    """Test exception handling when a bluesky_kafka.Publisher raises an exception.

    Parameters
    ----------
    caplog: built-in pytest fixture
        logging output will be captured by this fixture
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    RE: pytest fixture
        bluesky RunEngine

    """

    # use a random string as the beamline name so topics will not be duplicated across tests
    beamline_name = str(uuid.uuid4())[:8]
    with temporary_topics(topics=[f"{beamline_name}.bluesky.runengine.documents"]) as (
        beamline_topic,
    ), caplog.at_level(logging.ERROR, logger="bluesky_kafka"):

        publisher_queue = queue.Queue()
        mock_kafka_publisher = Mock(side_effect=BlueskyKafkaException())
        (
            kafka_publisher_queue,
            kafka_publisher_thread,
            kafka_publisher_thread_stop_event,
        ) = start_kafka_publisher_thread(
            publisher_queue=publisher_queue,
            publisher=mock_kafka_publisher,
            publisher_queue_timeout=1,
        )

        # provoke two exceptions
        publisher_queue.put(("start", {}))
        publisher_queue.put(("stop", {}))

        # wait until both documents have been removed from the queue
        while not publisher_queue.empty():
            print("waiting for queue to empty")
            ttime.sleep(1)

        # stop the polling loop
        kafka_publisher_thread_stop_event.set()
        kafka_publisher_thread.join()

        # the logging output should contain 2 ERROR log records
        # with thread name starting with "kafka-publisher-thread-",
        # one for the start document, one for the stop document
        kafka_publisher_thread_log_records = [
            log_record
            for log_record
            in caplog.records
            if log_record.threadName.startswith("kafka-publisher-thread-")
        ]
        assert len(kafka_publisher_thread_log_records) == 2

        expected_log_message_pattern = re.compile(
            r"an error occurred after 0 successful Kafka messages when "
            r"'<Mock id='\d+'>' attempted to publish on topic <Mock name='mock.topic' id='\d+'>"
        )

        # test some aspects of each log record
        for error_log_record, expected_document_name in zip(
            kafka_publisher_thread_log_records, ("start", "stop")
        ):
            error_log_message = error_log_record.getMessage()
            assert expected_log_message_pattern.search(error_log_message)
            assert f"name: '{expected_document_name}'" in error_log_message


def test_publisher_with_no_broker(RE, hw):
    """Test the case of no Kafka broker.

    In this case build_and_start_kafka_publisher_thread
    should not raise an exception, but the returned
    publisher queue should be None.
    """

    beamline_name = str(uuid.uuid4())[:8]
    (
        kafka_publisher_queue,
        kafka_publisher_thread_exit_event,
    ) = build_and_start_kafka_publisher_thread(
        topic=beamline_name,
        # specify a bootstrap server that does not exist
        bootstrap_servers="100.100.100.100:9092",
        producer_config={},
        #     "acks": "all",
        #     "enable.idempotence": False,
        #     "request.timeout.ms": 1000,
        # },
        publisher_queue_timeout=1,
    )

    # in the case of no Kafka broker the publisher queue is None
    assert kafka_publisher_queue is None

    published_bluesky_documents = []

    # store all documents published directly by the RunEngine
    # to verify the expected documents were generated
    def store_published_document(name, document):
        published_bluesky_documents.append((name, document))

    RE.subscribe(store_published_document)

    RE(count([hw.det1]))

    # the RunEngine should have published 4 documents
    assert len(published_bluesky_documents) == 4
