from functools import partial
import os

import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import BlueskyStream


bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
if bootstrap_servers is None:
    raise AttributeError("Environment variable KAFKA_BOOTSTRAP_SERVERS"
                         "must be set.")

auto_offset_reset = "latest"
# The BlueskyStream can only subscibe to one topic.
topics = ["csx.bluesky.documents"]


bluesky_stream = BlueskyStream(
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id="csx-filler-stream",
    consumer_config={"auto.offset.reset": auto_offset_reset},
    producer_config={"acks": 1, "enable.idempotence": False},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)


BlueskyStream.start()
