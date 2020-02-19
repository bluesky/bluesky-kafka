import logging
import multiprocessing
import pprint
import time

import numpy as np
import pickle

import msgpack
import pytest

logging.getLogger("bluesky.kafka").setLevel("DEBUG")

from bluesky_kafka import Publisher, RemoteDispatcher
from bluesky.plans import count


TEST_TOPIC = "bluesky-kafka-test"


@pytest.mark.parametrize(
    "serializer,deserializer",
    [(pickle.dumps, pickle.loads), (msgpack.dumps, msgpack.loads)],
)
def test_kafka(RE, hw, bootstrap_servers, serializer, deserializer):
    # COMPONENT 1
    # a Kafka broker must be running
    # in addition the broker must have topic "bluesky-kafka-test"

    # COMPONENT 2
    # Run a Publisher and a RunEngine in this process
    kafka_publisher = Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers=bootstrap_servers,
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
            group_id="kafka-unit-test",
            auto_offset_reset="earliest",
            deserializer=deserializer,
        )
        kafka_dispatcher.subscribe(put_in_queue)
        kafka_dispatcher.start()

    queue_ = multiprocessing.Queue()
    dispatcher_proc = multiprocessing.Process(
        target=make_and_start_dispatcher, daemon=True, args=(queue_,)
    )
    dispatcher_proc.start()
    time.sleep(10)  # As above, give this plenty of time to start.

    local_accumulator = []

    def local_cb(name, doc):
        print("local_cb: {}".format(name))
        local_accumulator.append((name, doc))

    # Check that numpy data is sanitized by putting some in the start doc.
    # sending numpy arrays with md causes this:
    #    assert remote_accumulator == local_accumulator
    #    ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()
    # md = {
    #     'numpy_data': {
    #         'nested': np.array([1, 2, 3])
    #     },
    #    'numpy_scalar': np.float64(3),
    #    'numpy_array': np.ones((3, 3))
    # }
    md = {}

    RE.subscribe(local_cb)
    RE(count([hw.det]), md=md)
    kafka_publisher.flush()
    time.sleep(10)

    # Get the documents from the queue (or timeout --- test will fail)
    remote_accumulator = []
    for i in range(len(local_accumulator)):
        remote_accumulator.append(queue_.get(timeout=2))

    dispatcher_proc.terminate()
    dispatcher_proc.join()

    print("local_accumulator:")
    pprint.pprint(local_accumulator)
    print("remote_accumulator:")
    pprint.pprint(remote_accumulator)

    # numpy arrays cause trouble sometimes
    # msgpack
    #remote_accumulator[0][1].pop("hints")
    #local_accumulator[0][1].pop("hints")
    assert remote_accumulator == local_accumulator
