import event_model
import logging
import multiprocessing
import os
import pytest
import pprint
import time

from bluesky.plans import count
from dictdiffer import diff
from event_model import sanitize_doc
from functools import partial

logging.getLogger("bluesky.kafka").setLevel("DEBUG")


# Vendored from https://github.com/NSLS-II/databroker-nsls2-tests/
# We plan to move these methods to event_model.
# Once we do that we should import them from there.
def normalize(gen):
    """
    Converted any pages to singles.
    """
    for name, doc in gen:
        if name == 'event_page':
            for event in event_model.unpack_event_page(doc):
                yield 'event', event
        elif name == 'datum_page':
            for datum in event_model.unpack_datum_page(doc):
                yield 'datum', datum
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
        if name == 'start':
            indexed['uid'] = doc['uid']
        if name == 'resource':
            # Check for an extraneous duplicate key in old documents.
            if 'id' in doc:
                assert doc['id'] == doc['uid']
                doc = doc.copy()
                doc.pop('id')
            indexed[(name, doc['uid'])] = doc.copy()
        elif name == 'datum':
            indexed[('datum', doc['datum_id'])] = doc.copy()
        # v0 yields {'_name": 'RunStop'} if the stop doc is missing; v2 yields
        # None.
        elif name == 'stop' and doc is None or 'uid' not in doc:
            indexed[(name, None)] = None
        else:
            indexed[(name, doc['uid'])] = doc.copy()
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
    for change_type, change, _ in difference:
        # For now it is ok if you get a resource that doesn't have a matching
        # run_start. This will be fixed by a database migration.
        if (change_type == 'change' and change[0][0] == 'resource' and change[1] =='run_start'):
            acceptable.append(True)
            reasons.append('DUPLICATE RESOURCE UID')
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
        difference = [item for item in diff(run_a, run_b) if item[0]!='remove']
    else:
        difference = list(diff(run_a, run_b))

    xfail(difference, run_a['uid'])

    if difference:
        logger.info(label + " " + run_a['uid'] + " " +
                    run_b['uid'] + " " + str(difference))

    assert not difference


def test_mongo_consumer(RE, hw, md, publisher, mongo_consumer, broker):
    """
    Subscribe a MongoBlueskyConsumer to a kafka topic, and check that
    documents published to this topic are inserted correctly in a mongo database.
    """

    original_documents = []

    def record(name, doc):
        original_documents.append((name, doc))

    def start_consumer():
        mongo_consumer.start()

    # Subscribe the publisher to the run engine. This puts the RE documents into Kafka.
    RE.subscribe(publisher)

    # Also keep a copy of the produced documents to compare with later.
    RE.subscribe(record)

    # Create the consumer, that takes documents from Kafka, and puts them in mongo.
    consumer_proc = multiprocessing.Process(target=start_consumer, daemon=True)
    consumer_proc.start()
    time.sleep(10)

    # Run a plan to generate documents.
    uid = RE(count([hw.det]), md=md)

    # The documents should now be flowing from the RE to the mongo database, via Kafka.
    time.sleep(10)

    # Get the documents from the mongo database.
    mongo_documents = list(broker[uid].cannonical(fill='no'))

    # Check that the original documents are the same as the documents in the mongo database.
    compare(original_documents, mongo_documents, "mongo_consumer_test")