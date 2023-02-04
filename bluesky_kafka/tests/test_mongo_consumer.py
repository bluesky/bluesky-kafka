from functools import partial
import json
import logging
import time

from dictdiffer import diff
import pytest

from bluesky_kafka import MongoConsumer
from bluesky.plans import count
import event_model


logging.getLogger("bluesky.kafka").setLevel("DEBUG")
TEST_TOPIC = "bluesky-kafka-test"


# Vendored from https://github.com/NSLS-II/databroker-nsls2-tests/
# We plan to move these methods to event_model.
# Once we do that we should import them from there.
def normalize(gen):
    """
    Converted any pages to singles.
    """
    for name, doc in gen:
        if name == "event_page":
            for event in event_model.unpack_event_page(doc):
                yield "event", event
        elif name == "datum_page":
            for datum in event_model.unpack_datum_page(doc):
                yield "datum", datum
        else:
            yield name, doc


# Vendored from https://github.com/NSLS-II/databroker-nsls2-tests/
# We plan to move these methods to event_model.
# Once we do that we should import them from there.
def index(run):
    """
    Turn a Bluesky run into a dictionary of documents, indexed by
    (name, doc['uid'])
    """
    indexed = {}
    for name, doc in run:
        if name == "start":
            indexed["uid"] = doc["uid"]
        if name == "resource":
            # Check for an extraneous duplicate key in old documents.
            if "id" in doc:
                assert doc["id"] == doc["uid"]
                doc = doc.copy()
                doc.pop("id")
            indexed[(name, doc["uid"])] = doc.copy()
        elif name == "datum":
            indexed[("datum", doc["datum_id"])] = doc.copy()
        # v0 yields {'_name": 'RunStop'} if the stop doc is missing; v2 yields
        # None.
        elif name == "stop" and doc is None or "uid" not in doc:
            indexed[(name, None)] = None
        else:
            indexed[(name, doc["uid"])] = doc.copy()
    return indexed


# Vendored from https://github.com/NSLS-II/databroker-nsls2-tests/
# We plan to move these methods to event_model.
# Once we do that we should import them from there.
def xfail(difference, uid):
    """
    Make pytest skip this test if all differences are acceptable.
    """
    acceptable = []
    reasons = []
    known_problems = {}

    for change_type, change, _ in difference:
        # For now it is ok if you get a resource that doesn't have a matching
        # run_start. This will be fixed by a database migration.
        if (
            change_type == "change"
            and change[0][0] == "resource"
            and change[1] == "run_start"
        ):
            acceptable.append(True)
            reasons.append("DUPLICATE RESOURCE UID")
        elif uid in known_problems:
            acceptable.append(True)
            reasons.append(known_problems[uid])
        else:
            acceptable.append(False)

    if difference and all(acceptable):
        pytest.xfail(str(reasons + difference))


# Vendored from https://github.com/NSLS-II/databroker-nsls2-tests/
# We plan to move these methods to event_model.
# Once we do that we should import them from there.
def compare(a, b, label, remove_ok=False):
    """
    Check that there is no differences between Bluesky runs a and b.
    Setting remove_ok to True means that it is ok for a to have more
    information than run b.
    """
    run_a = index(normalize(a))
    run_b = index(normalize(b))

    if remove_ok:
        difference = [item for item in diff(run_a, run_b) if item[0] != "remove"]
    else:
        difference = list(diff(run_a, run_b))

    xfail(difference, run_a["uid"])

    if difference:
        print(label + " " + run_a["uid"] + " " + run_b["uid"] + " " + str(difference))

    assert not difference


@pytest.mark.skip(reason="this test is not ready")
def test_mongo_consumer(
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
