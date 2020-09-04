from functools import partial
import os

import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import RucioConsumer


bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
if bootstrap_servers is None:
    raise AttributeError("Environment variable KAFKA_BOOTSTRAP_SERVERS"
                         "must be set.")


kafka_deserializer = partial(msgpack.loads, object_hook=mpn.decode)
auto_offset_reset = "latest"
topics = ["^.*bluesky.documents"]
group_id = "rucio_archiver"

# These are NSLS2 specific.
rse='NSLS2'
scope='nsls2'
dataset='bluesky-sdcc',
pfn='globus://'

# Create a RucioConsumer that will automatically listen to new beamline topics.
# The parameter metadata.max.age.ms determines how often the consumer will check for
# new topics. The default value is 5000ms.
rucio_consumer = RucioConsumer(
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    consumer_config={"auto.offset.reset": auto_offset_reset},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)

rucio_consumer.start()
