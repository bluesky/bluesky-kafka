from functools import partial
import os

import msgpack
import msgpack_numpy as mpn

from bluesky_kafka import MongoConsumer


bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
if bootstrap_servers is None:
    raise AttributeError("Environment variable KAFKA_BOOTSTRAP_SERVERS"
                         "must be set.")

mongo_uri = os.environ.get("BLUESKY_MONGO_URI")
if mongo_uri is None:
    raise AttributeError("Environment variable BLUESKY_MONGO_URI "
                         "must be set.")

beamline_password = os.environ.get("KAFKA_BEAMLINE_PASSWORD")
if beamline_password is None:
    raise AttributeError("Environment variable KAFKA_BEAMLINE_PASSWORD"
                         "must be set.")

kafka_deserializer = partial(msgpack.loads, object_hook=mpn.decode)
auto_offset_reset = "latest"
topics = ["^.*bluesky.runengine.documents"]

topic_database_map = {'amx.bluesky.runengine.documents': 'amx-bluesky-documents',
                      'bmm.bluesky.runengine.documents': 'bmm-bluesky-documents',
                      'chx.bluesky.runengine.documents': 'chx-bluesky-documents',
                      'cms.bluesky.runengine.documents': 'cms-bluesky-documents',
                      'csx.bluesky.runengine.documents': 'csx-bluesky-documents',
                      'esm.bluesky.runengine.documents': 'esm-bluesky-documents',
                      'fmx.bluesky.runengine.documents': 'fmx-bluesky-documents',
                      'fxi.bluesky.runengine.documents': 'fxi-bluesky-documents',
                      'ios.bluesky.runengine.documents': 'ios-bluesky-documents',
                      'isr.bluesky.runengine.documents': 'isr-bluesky-documents',
                      'iss.bluesky.runengine.documents': 'iss-bluesky-documents',
                      'ixs.bluesky.runengine.documents': 'ixs-bluesky-documents',
                      'opls.bluesky.runengine.documents': 'opls-bluesky-documents',
                      'lix.bluesky.runengine.documents': 'lix-bluesky-documents',
                      'nyx.bluesky.runengine.documents': 'nyx-bluesky-documents',
                      'pdf.bluesky.runengine.documents': 'pdf-bluesky-documents',
                      'qas.bluesky.runengine.documents': 'qas-bluesky-documents',
                      'rsoxs.bluesky.runengine.documents': 'rsoxs-bluesky-documents',
                      'six.bluesky.runengine.documents': 'six-bluesky-documents',
                      'smi.bluesky.runengine.documents': 'smi-bluesky-documents',
                      'srx.bluesky.runengine.documents': 'srx-bluesky-documents',
                      'tes.bluesky.runengine.documents': 'tes-bluesky-documents',
                      'xfm.bluesky.runengine.documents': 'xfm-bluesky-documents',
                      'xfp.bluesky.runengine.documents': 'xfp-bluesky-documents',
                      'xpd.bluesky.runengine.documents': 'xpd-bluesky-documents',
                      'xpdd.bluesky.runengine.documents': 'xpdd-bluesky-documents'}

# Create a MongoConsumer that will automatically listen to new beamline topics.
# The parameter metadata.max.age.ms determines how often the consumer will check for
# new topics. The default value is 5000ms.
mongo_consumer = MongoConsumer(
    mongo_uri,
    topic_database_map
    tls=True,
    topics=topics,
    bootstrap_servers=bootstrap_servers,
    group_id="mongodb",
    consumer_config={"auto.offset.reset": auto_offset_reset},
    polling_duration=1.0,
    deserializer=kafka_deserializer,
)


mongo_consumer.start()
