import logging
import re
import queue
import threading
import time as ttime
import uuid

from typing import Callable
from unittest.mock import Mock

import pytest

from confluent_kafka.cimpl import KafkaException

from bluesky.plans import count
from event_model import sanitize_doc

from bluesky_kafka import BlueskyKafkaException
from bluesky_kafka.tools.queue_thread import (
    build_kafka_publisher_queue_and_thread,
    _start_kafka_publisher_thread,
)


def test_build_kafka_publisher_queue_and_thread(
    kafka_bootstrap_servers,
    broker_authorization_config,
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
    broker_authorization_config: dict
        Kafka broker authentication parameters for the test broker
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
        publisher_queue_thread_details = build_kafka_publisher_queue_and_thread(
            topic=beamline_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config=broker_authorization_config,
        )

        assert isinstance(publisher_queue_thread_details.publisher_queue, queue.Queue)
        assert isinstance(
            publisher_queue_thread_details.publisher_thread, threading.Thread
        )
        assert isinstance(
            publisher_queue_thread_details.publisher_thread_stop_event,
            threading.Event,
        )
        assert isinstance(
            publisher_queue_thread_details.put_on_publisher_queue, Callable
        )

        published_bluesky_documents = []

        # store all documents published directly by the RunEngine
        # for comparison with documents published by Kafka
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE.subscribe(store_published_document)

        RE.subscribe(publisher_queue_thread_details.put_on_publisher_queue)

        # all documents created by this plan are expected
        # to be published as Kafka messages
        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        # retrieve the documents published as Kafka messages
        consumed_bluesky_documents = (
            consume_documents_from_kafka_until_first_stop_document(
                kafka_topic=beamline_topic
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


def test_no_topic(caplog, kafka_bootstrap_servers, broker_authorization_config, RE):
    """Test the case of a topic that does not exist in the Kafka broker.

    If the topic does not exist a BlueskyKafkaException
    should be raised. In addition an ERROR should be logged.

    Parameters
    ----------
    caplog: built-in pytest fixture
        logging output will be captured by this fixture
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "kafka1:9092,kafka2:9092"
    broker_authorization_config: dict
        Kafka broker authentication parameters for the test broker
    RE: RunEngine (pytest fixture)
        bluesky RunEngine
    """

    with pytest.raises(BlueskyKafkaException), caplog.at_level(
        level=logging.ERROR, logger="bluesky_kafka"
    ):
        # use a random string so topics will not be duplicated across tests
        topic = str(uuid.uuid4())[:8]
        build_kafka_publisher_queue_and_thread(
            topic=topic,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config=broker_authorization_config,
        )

    assert f"topic `{topic}` does not exist on Kafka broker(s)" in caplog.text


def test__subscribe_kafka_publisher(caplog, temporary_topics, RE):
    """Test exception handling when a bluesky_kafka.Publisher raises an exception.

    The publisher thread should not terminate if bluesky_kafka.Publisher raises.

    Parameters
    ----------
    caplog: built-in pytest fixture
        logging output will be captured by this fixture
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    RE: pytest fixture
        bluesky RunEngine
    """

    # use a random string so topics will not be duplicated across tests
    with temporary_topics(topics=[str(uuid.uuid4())[:8]]) as (topic,), caplog.at_level(
        logging.ERROR, logger="bluesky_kafka"
    ):

        publisher_queue = queue.Queue()
        mock_kafka_publisher = Mock(side_effect=BlueskyKafkaException())
        publisher_queue_thread_details = _start_kafka_publisher_thread(
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
        publisher_queue_thread_details.publisher_thread_stop_event.set()
        publisher_queue_thread_details.publisher_thread.join()

        # the logging output should contain 2 ERROR log records
        # with thread name starting with "kafka-publisher-thread-",
        # one for the start document, one for the stop document
        kafka_publisher_thread_log_records = [
            log_record
            for log_record in caplog.records
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

    In this case a confluent_kafka.cimpl.KafkaException is raised.
    """

    with pytest.raises(KafkaException):
        beamline_name = str(uuid.uuid4())[:8]
        kafka_publisher_queue_thread_details = build_kafka_publisher_queue_and_thread(
            topic=beamline_name,
            # specify a bootstrap server that does not exist
            bootstrap_servers="100.100.100.100:9092",
            producer_config={},
            publisher_queue_timeout=1,
        )
