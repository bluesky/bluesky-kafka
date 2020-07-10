import collections
import event_model
import logging
import multiprocessing
import numpy as np
import os
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
def compare(a, b, beamline, remove_ok=False):
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
        logger.info(beamline + " " + run_a['uid'] + " " +
                    run_b['uid'] + " " + str(difference))

    assert not difference


def test_mongo_consumer(RE, hw, md, publisher, mongo_consumer, mongo_client):
    """
    The structure of this test:

    The RunEngine (RE) generates documents.

    The publisher is subscribed to the RunEngine, when it gets a document
    it inserts it into the kafka topic TEST_TOPIC.

    The mongo_consumer is subscribed to TEST_TOPIC, it gets the documents
    from kafka and inserts them into a mongo database with a matching name.

    The mongo client reads the documents from mongo.

    The test then checks that the documents produced by the RunEngine
    match the documents that are in mongo.

    This test needs a Kafka Broker with TEST_TOPIC and a Mongo database.
    """
    doc_types = ['start', 'stop', 'descriptor', 'resource', 'event',
                 'event_page', 'datum', 'datum_page']
    original_uids = {key: set() for key in doc_types}
    final_uids = {key:set() for key in doc_types}

    def local_cb(name, doc):
        print("local_cb: {}".format(name))
        local_published_documents.append((name, doc))

    def start_consumer():
        mongo_consumer.start()

    consumer_proc = multiprocessing.Process(target=start_consumer, daemon=True)
    consumer_proc.start()
    time.sleep(10)

    RE.subscribe(publisher)
    RE.subscribe(local_cb)
    RE(count([hw.det]), md=md)
    time.sleep(10)


    # May need a consumer join here.

    # Get the documents from the mongo.


    # sanitize_doc normalizes some document data, such as numpy arrays, that are
    # problematic for direct comparison of documents by "assert"
    sanitized_local_published_documents = [
        sanitize_doc(doc) for doc in local_published_documents
    ]
    sanitized_remote_published_documents = [
        sanitize_doc(doc) for doc in remote_published_documents
    ]

    print("local_published_documents:")
    pprint.pprint(local_published_documents)
    print("remote_published_documents:")
    pprint.pprint(remote_published_documents)

    assert sanitized_remote_published_documents == sanitized_local_published_documents
