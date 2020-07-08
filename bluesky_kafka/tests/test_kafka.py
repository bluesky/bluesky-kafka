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


# the Kafka test broker should be configured with
# KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
TEST_TOPIC = "bluesky-kafka-test"


def test_producer_config():
    kafka_publisher = Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers="1.2.3.4:9092",
        key="kafka-unit-test-key",
        # work with a single broker
        producer_config={
            "bootstrap.servers": "5.6.7.8:9092",
            "acks": 1,
            "enable.idempotence": False,
            "request.timeout.ms": 5000,
        },
    )

    assert (
        kafka_publisher._producer_config["bootstrap.servers"]
        == "1.2.3.4:9092,5.6.7.8:9092"
    )


def test_consumer_config():
    kafka_dispatcher = RemoteDispatcher(
        topics=[TEST_TOPIC],
        bootstrap_servers="1.2.3.4:9092",
        group_id="abc",
        consumer_config={
            "bootstrap.servers": "5.6.7.8:9092",
            "auto.offset.reset": "latest",
        },
    )

    assert (
        kafka_dispatcher._consumer_config["bootstrap.servers"]
        == "1.2.3.4:9092,5.6.7.8:9092"
    )


def test_bad_consumer_config():
    with pytest.raises(ValueError) as excinfo:
        kafka_dispatcher = RemoteDispatcher(
            topics=[TEST_TOPIC],
            bootstrap_servers="1.2.3.4:9092",
            group_id="abc",
            consumer_config={
                "bootstrap.servers": "5.6.7.8:9092",
                "auto.offset.reset": "latest",
                "group.id": "raise an exception!",
            },
        )
        assert (
            "do not specify 'group.id' in consumer_config, use only the 'group_id' argument"
            in excinfo.value
        )


@pytest.mark.parametrize(
    "serializer, deserializer, auto_offset_reset",
    [
        (pickle.dumps, pickle.loads, "earliest"),
        (pickle.dumps, pickle.loads, "latest"),
        (
            partial(msgpack.dumps, default=mpn.encode),
            partial(msgpack.loads, object_hook=mpn.decode),
            "earliest",
        ),
        (
            partial(msgpack.dumps, default=mpn.encode),
            partial(msgpack.loads, object_hook=mpn.decode),
            "latest",
        ),
    ],
)
def test_kafka(RE, hw, bootstrap_servers, serializer, deserializer, auto_offset_reset):
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
        def put_in_queue(name, doc):
            logger = logging.getLogger("bluesky.kafka")
            logger.debug("putting %s in queue", name)
            queue.put((name, doc))

        kafka_dispatcher = RemoteDispatcher(
            topics=[TEST_TOPIC],
            bootstrap_servers=bootstrap_servers,
            group_id="kafka-unit-test-group-id",
            # "latest" should always work but
            # has been failing on Linux, passing on OSX
            consumer_config={"auto.offset.reset": auto_offset_reset},
            polling_duration=1.0,
            deserializer=deserializer,
        )
        kafka_dispatcher.subscribe(put_in_queue)
        kafka_dispatcher.start()

    queue_ = multiprocessing.Queue()
    dispatcher_proc = multiprocessing.Process(
        target=make_and_start_dispatcher, daemon=True, args=(queue_,)
    )
    dispatcher_proc.start()
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

    dispatcher_proc.terminate()
    dispatcher_proc.join()

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
