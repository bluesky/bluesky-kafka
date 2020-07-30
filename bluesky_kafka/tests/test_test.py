from functools import partial
import logging
import multiprocessing
import pprint
import time

import msgpack
# this is recommended by msgpack-numpy as a way
# to patch msgpack but it caused a utf-8 decode error
# mpn.patch()
import msgpack_numpy as mpn
import numpy as np
import pickle
import pytest

from bluesky_kafka import Publisher, RemoteDispatcher, BlueskyConsumer
from bluesky.plans import count
from event_model import sanitize_doc


logging.getLogger("bluesky.kafka").setLevel("DEBUG")


# the Kafka test broker should be configured with
# KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
TEST_TOPIC = "garrett.bluesky.documents"

@pytest.mark.parametrize(
    "serializer, deserializer, auto_offset_reset",
    [
        (
            partial(msgpack.dumps, default=mpn.encode),
            partial(msgpack.loads, object_hook=mpn.decode),
            "latest",
        ),
    ],
)
def test_kafka(RE, hw, serializer, deserializer, auto_offset_reset):
    # COMPONENT 1
    # a Kafka broker must be running
    # in addition the broker must have topic "bluesky-kafka-test"
    # or be configured to create topics on demand

    # COMPONENT 2
    # Run a Publisher and a RunEngine in this process
    kafka_publisher = Publisher(
        topic=TEST_TOPIC,
        bootstrap_servers="10.0.10.9:9092, 10.0.10.10:9092, 10.0.10.11:9092",
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

    md = {
        "numpy_data": {"nested": np.array([1, 2, 3])},
        "numpy_scalar": np.float64(3),
        "numpy_array": np.ones((3, 3)),
    }

    RE(count([hw.det]), md=md)
