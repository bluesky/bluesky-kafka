from functools import partial
import os

import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import BlueskyStream
from databroker.core import discover_handlers

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
if bootstrap_servers is None:
    raise AttributeError("Environment variable KAFKA_BOOTSTRAP_SERVERS"
                         "must be set.")


# The BlueskyStream can only subscibe to one topic.
input_topic = "csx.bluesky.documents"
output_topic = "csx.bluesky.documents.filled"
handlers = discover_handlers()


def no_op(topic, name, doc):
    return name, doc


bluesky_stream = BlueskyStream(
    input_topic,
    output_topic,
    group_id="csx-filler-stream",
    bootstrap_servers=bootstrap_servers,
    consumer_config={"auto.offset.reset": auto_offset_reset},
    producer_config={"acks": 1, "enable.idempotence": False},
    polling_duration=1.0,
    handler_registry=handlers,
    process_produce=no_op
)


BlueskyStream.start()
