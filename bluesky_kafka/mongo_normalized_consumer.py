import msgpack
import os
from bluesky_kafka import DynamicConsumer, MongoSerializerFactory
from functools import partial


bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
mongo_uri = os.environ.get("BLUESKY_MONGO_URI")
kafka_deserializer = partial(msgpack.loads, object_hook=mpn.decode)
auto_offset_reset = "latest"  # "latest" should always work but has been failing on Linux, passing on OSX
topics = ["^*.bluesky.documents"]


factory = MongoSerializerFactory(mongo_uri)

mongo_consumer = DynamicConsumer(
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id="kafka-unit-test-group-id",
    factory=factory,
    consumer_config={"auto.offset.reset": auto_offset_reset},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)


mongo_consumer.start()

