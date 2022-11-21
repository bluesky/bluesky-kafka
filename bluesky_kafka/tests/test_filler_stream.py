from functools import partial
import json
import logging
import multiprocessing
import time
import sys

from dictdiffer import diff
import pytest

from bluesky_kafka import MongoConsumer
from bluesky.plans import count
import event_model


logging.getLogger("bluesky.kafka").setLevel("DEBUG")
INPUT_TOPIC = "bluesky-kafka-test"
OUTPUT_TOPIC = "bluesky-kafka-test-filled"

path_root = pathlib.Path("/placeholder/path")
run_bundle = event_model.compose_run()
desc_bundle = run_bundle.compose_descriptor(
    data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
               'image': {'shape': [512, 512], 'dtype': 'number',
                         'source': '...', 'external': 'FILESTORE:'}},
    name='primary')
desc_bundle_baseline = run_bundle.compose_descriptor(
    data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'}},
    name='baseline')
res_bundle = run_bundle.compose_resource(
    spec='DUMMY', root=str(path_root), resource_path='stack.tiff',
    resource_kwargs={'a': 1, 'b': 2})
datum_doc = res_bundle.compose_datum(datum_kwargs={'c': 3, 'd': 4})
raw_event = desc_bundle.compose_event(
    data={'motor': 0, 'image': datum_doc['datum_id']},
    timestamps={'motor': 0, 'image': 0}, filled={'image': False},
    seq_num=1)
stop_doc = run_bundle.compose_stop()


@pytest.mark.skip(reason="this test is not ready")
def test_filler_stream(
    RE,
    hw,
    numpy_md,
    publisher,
    data_broker,
    mongo_uri,
    consumer_process_factory,
    external_process_document_queue,
):
    """
    Subscribe a MongoConsumer to a kafka topic, and check that
    documents published to this topic are inserted correctly in a mongo database.

    If there is a problem with the Consumer running on the separate process. You may receive
    a very unhelpful error message: "KeyError 421e977f-eec1-48f6-9288-fb03fc5342b9" To debug
    try running pytest with the '-s' option. This should tell you what went wrong with the
    Consumer.
    """

    original_documents = []

    def record(name, doc):
        original_documents.append((name, doc))

    # Subscribe the publisher to the run engine. This puts the RE documents into Kafka.
    RE.subscribe(publisher)

    # Also keep a copy of the produced documents to compare with later.
    RE.subscribe(record)

    # Create the consumer, that takes documents from Kafka, and puts them in mongo.
    # For some reason this does not work as a fixture.
    with external_process_document_queue(
        topics=["^.*-kafka-test*"],
        group_id="kafka-unit-test-group-id",
        consumer_config={"auto.offset.reset": "latest"},
        process_factory=consumer_process_factory,
        consumer_factory=MongoConsumer,
        # the next two arguments will be passed to MongoConsumer() as **kwargs
        polling_duration=1.0,
        mongo_uri=mongo_uri,
    ) as document_queue:  # noqa

        # Run a plan to generate documents.
        (uid,) = RE(count([hw.det]), md=numpy_md)

        # The documents should now be flowing from the RE to the mongo database, via Kafka.
        time.sleep(10)

        # Get the documents from the mongo database.
        mongo_documents = list(data_broker["xyz"][uid].canonical(fill="no"))

        # Check that the original documents are the same as the documents in the mongo database.
        original_docs = [
            json.loads(json.dumps(event_model.sanitize_doc(item)))
            for item in original_documents
        ]
        compare(original_docs, mongo_documents, "mongo_consumer_test")


@pytest.mark.xfail(reason="does not work correctly on Travis CI")
def test_mongo_consumer_multi_topic(
    RE,
    hw,
    numpy_md,
    publisher,
    publisher2,
    data_broker,
    mongo_uri,
    consumer_process_factory,
    external_process_document_queue,
):
    """
    Subscribe a MongoConsumer to multiple kafka topics, and check that
    documents published to these topics are inserted to the correct mongo database.

    If there is a problem with the Consumer running on the separate process. You may receive
    a very unhelpful error message: "KeyError 421e977f-eec1-48f6-9288-fb03fc5342b9" To debug
    try running pytest with the '-s' option. This should tell you what went wrong with the
    Consumer.
    """

    original_documents = []

    def record(name, doc):
        original_documents.append((name, doc))

    # Subscribe the two publishers to the run engine.
    # This publishes the documents to two different Kafka topics.
    RE.subscribe(publisher)
    RE.subscribe(publisher2)

    # Also keep a copy of the produced documents to compare with later.
    RE.subscribe(record)

    # Create the consumer, that takes documents from Kafka, and puts them in mongo.
    # For some reason this does not work as a fixture.
    # This consumer should read from both of the producers topics.
    with external_process_document_queue(
        topics=["^.*-kafka-test*"],
        group_id="kafka-unit-test-group-id",
        consumer_config={"auto.offset.reset": "latest"},
        process_factory=partial(
            consumer_process_factory, consumer_factory=MongoConsumer
        ),
        # the next two arguments will be passed to MongoConsumer() as **kwargs
        polling_duration=1.0,
        mongo_uri=mongo_uri,
    ) as document_queue:  # noqa

        # Run a plan to generate documents.
        (uid,) = RE(count([hw.det]), md=numpy_md)

        # The documents should now be flowing from the RE to the mongo database, via Kafka.
        time.sleep(10)

        # Get the documents from the mongo database.
        mongo_documents1 = list(data_broker["xyz"][uid].canonical(fill="no"))
        mongo_documents2 = list(data_broker["xyz2"][uid].canonical(fill="no"))

        # Check that the original documents are the same as the documents in the mongo database.
        original_docs = [
            json.loads(json.dumps(event_model.sanitize_doc(item)))
            for item in original_documents
        ]
        compare(original_docs, mongo_documents1, "mongo_consumer_test1")
        compare(original_docs, mongo_documents2, "mongo_consumer_test2")
