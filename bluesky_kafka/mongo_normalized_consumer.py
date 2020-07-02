import msgpack
import os

from bluesky_kafka import RemoteDispatcher
from functools import partial
from suitcase.mongo_normalized import Serializer

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
mongo_uri = os.environ.get("BLUESKY_MONGO_URI")
kafka_deserializer = partial(msgpack.loads, object_hook=mpn.decode)

# "latest" should always work but has been failing on Linux, passing on OSX
auto_offset_reset = "latest"
topics = ["^*.bluesky.documents"]

# Pass mongo_uri twice so that all documents go to the same database.
# Hmm... I'm not sure how to make the documents go from a topic go to the right database.
mongo_serializer = Serializer(mongo_uri, mongo_uri)

kafka_dispatcher = RemoteDispatcher(
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id="kafka-unit-test-group-id",
    consumer_config={"auto.offset.reset": auto_offset_reset},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)

kafka_dispatcher.subscribe(mongo_serializer)
kafka_dispatcher.start()
