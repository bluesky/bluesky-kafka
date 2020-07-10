from functools import partial
import logging
import multiprocessing
import pprint
import time

import numpy as np
import pickle

import msgpack
import msgpack_numpy as mpn
import pytest

# this is recommended by msgpack-numpy as a way
# to patch msgpack but it caused a utf-8 decode error
# mpn.patch()

logging.getLogger("bluesky.kafka").setLevel("DEBUG")

from bluesky_kafka import Publisher, RemoteDispatcher
from bluesky.plans import count
from event_model import sanitize_doc


def test_mongo_consumer(RE, hw, publisher, mongo_consumer, mongo_client):
    """
    The structure of this test:

    The RunEngine (RE) generates documents.

    The publisher is subscribed to the RunEngine, when it gets a document
    it inserts it into the kafka topic TEST_TOPIC.

    The mongo_consumer is subscribed to TEST_TOPIC, it gets the documents
    from kafka and inserts them into a mongo database with a matching name.

    The mongo client reads the documents from mongo.

    The test then checks that the documents produced by the RunEngine
    match the documents that are in mongo.
    """

    # COMPONENT 0
    # A mongo database
    mongobox = pytest.importorskip('mongobox')
    box = mongobox.MongoBox()
    box.start()
    client = box.client()
    mongo_uri = f"mongodb://{client.address[0]}:{client.address[1]}/{TEST_TOPIC}"

    # COMPONENT 1
    # a Kafka broker must be running
    # in addition the broker must have topic "bluesky-kafka-test"
    # or be configured to create topics on demand

    # COMPONENT 2
    # Run a Publisher and a RunEngine in this process
    kafka_publisher = Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers=bootstrap_servers,
        key="kafka-unit-test-key",
        # work with a single broker
        producer_config={
            "acks": 1,
            "enable.idempotence": False,
            "request.timeout.ms": 5000,
        },
        serializer=serializer,
    )
    RE.subscribe(kafka_publisher)

    # COMPONENT 3
    # Run a RemoteDispatcher on a separate process. Pass the documents
    # it receives over a Queue to this process so we can count them for our
    # test.
    def make_and_start_dispatcher(queue):
        mongo_consumer = MongoBlueskyConsumer(
            topics=[TEST_TOPIC],
            bootstrap_servers=bootstrap_servers,
            group_id="kafka-unit-test-group-id",
            mongo_uri=mongo_uri,
            # "latest" should always work but
            # has been failing on Linux, passing on OSX
            consumer_config={"auto.offset.reset": auto_offset_reset},
            polling_duration=1.0,
            deserializer=deserializer,
        )
        mongo_consumer.start()

    queue_ = multiprocessing.Queue()
    consumer_proc = multiprocessing.Process(
        target=make_and_start_dispatcher, daemon=True, args=(queue_,)
    )
    consumer_proc.start()
    time.sleep(10)

    local_published_documents = []

    def local_cb(name, doc):
        print("local_cb: {}".format(name))
        local_published_documents.append((name, doc))

    # test that numpy data is transmitted correctly
    md = {
        "numpy_data": {"nested": np.array([1, 2, 3])},
        "numpy_scalar": np.float64(3),
        "numpy_array": np.ones((3, 3)),
    }

    RE.subscribe(local_cb)
    RE(count([hw.det]), md=md)
    time.sleep(10)

    # Get the documents from the queue (or timeout --- test will fail)
    remote_published_documents = []
    for i in range(len(local_published_documents)):
        remote_published_documents.append(queue_.get(timeout=2))

    consumer_proc.terminate()
    consumer_proc.join()

    # sanitize_doc normalizes some document data, such as numpy arrays, that are
    # problematic for direct comparison of documents by "assert"
    sanitized_local_published_documents = [
        sanitize_doc(doc) for doc in local_published_documents
    ]
    sanitized_remote_published_documents = [
        sanitize_doc(doc) for doc in remote_published_documents
    ]

    print("local_published_documents:")
    pprint.pprint(local_published_documents)
    print("remote_published_documents:")
    pprint.pprint(remote_published_documents)

    assert sanitized_remote_published_documents == sanitized_local_published_documents
