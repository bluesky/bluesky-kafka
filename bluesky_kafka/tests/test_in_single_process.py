import logging

from confluent_kafka.admin import AdminClient, NewTopic

from bluesky import RunEngine
from bluesky.plans import count
from event_model import sanitize_doc

from ophyd.tests.conftest import hw  # noqa

from bluesky_kafka import Publisher, BlueskyConsumer


test_log = logging.getLogger("bluesky.kafka.test")
#log_to_console("bluesky.kafka", level=logging.DEBUG)


def test_publisher_and_consumer(kafka_bootstrap_servers):
    """

    """

    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap_servers})
    topic = "test_publisher_and_consumer"
    test_log.info("creating topic '%s'", topic)
    new_topic_futures = admin_client.create_topics([
        NewTopic(
            topic="",
            num_partitions=1,
            replication_factor=1
        )
    ])
    for new_topic, new_topic_future in new_topic_futures.items():
        # the return valued is None
        # just looking for a no-exception call
        new_topic_future.result()
        test_log.info("topic '%s' was created successfully", new_topic)

    bluesky_publisher = Publisher(
        topic=topic,
        bootstrap_servers=kafka_bootstrap_servers,
        producer_config={
            "acks": 1,
            "request.timeout.ms": 5000,
        },
        flush_on_stop_doc=True
    )

    published_bluesky_documents = []

    def store_published_document(name, document):
        published_bluesky_documents.append((name, document))

    RE = RunEngine()
    RE.subscribe(bluesky_publisher)
    RE.subscribe(store_published_document)
    RE(count([hw.det]))

    assert len(published_bluesky_documents) == 4

    consumed_bluesky_documents = []

    def store_consumed_document(name, document):
        consumed_bluesky_documents.append((name, document))

    bluesky_consumer = BlueskyConsumer(
        topics=(topic, ),
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
        polling_duration=1.0
    )

    def until_first_stop_document():
        assert len(consumed_bluesky_documents) <= 4
        if "stop" in [name for name, _ in consumed_bluesky_documents]:
            return False
        else:
            return True

    bluesky_consumer.start(
        continue_polling=until_first_stop_document,
    )

    assert len(published_bluesky_documents) == len(consumed_bluesky_documents)

    # sanitize_doc normalizes some document data, such as numpy arrays, that are
    # problematic for direct comparison of documents by "assert"
    sanitized_published_bluesky_documents = [sanitize_doc(doc) for doc in published_bluesky_documents]
    sanitized_consumed_bluesky_documents = [sanitize_doc(doc) for doc in consumed_bluesky_documents]

    assert len(sanitized_consumed_bluesky_documents) == len(sanitized_published_bluesky_documents)
    assert sanitized_consumed_bluesky_documents == sanitized_published_bluesky_documents
