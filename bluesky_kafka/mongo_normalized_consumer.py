import cachetools
import msgpack
import os

from bluesky_kafka import RemoteDispatcher
from functools import partial
from suitcase.mongo_normalized import Serializer

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
mongo_uri = os.environ.get("BLUESKY_MONGO_URI")
kafka_deserializer = partial(msgpack.loads, object_hook=mpn.decode)
auto_offset_reset = "latest"  # "latest" should always work but has been failing on Linux, passing on OSX
topics = ["^*.bluesky.documents"]


class LazyMongoRouter():
    """
    Route messages to the correct mongo database, determined by the topic name.
    TODO add caching mechanism to close old serializers.
    """
    def __init__(self, mongo_uri):
        self._mongo_uri = mongo_uri
        self._serializers = {}

    @property
    def serializers(self):
        return self._serializers

    def _get_serializer(self, topic):
        database_name = topic.replace('.', '-')
        serializer = Serializer(self._mongo_uri + database_name,
                                self._mongo_uri + datamase_name)
        self._serializers[topic] = serializer
        return serializer

    def __call__(self, topic, message):
        try:
            serializer = self._serialzers[topic]
        except KeyError:
            serializer = self._get_serializer(topic)
        return serializer(*message)


mongo_router = LazyMongoRouter(mongo_uri)


kafka_dispatcher = RemoteDispatcher(
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id="kafka-unit-test-group-id",
    consumer_config={"auto.offset.reset": auto_offset_reset},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)


kafka_dispatcher.subscribe(mongo_router)
kafka_dispatcher.start()

