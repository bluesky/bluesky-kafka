import re

import pytest

from bluesky_kafka import BlueskyKafkaException
from bluesky_kafka.utils import create_topics, delete_topics, list_topics


def test_create_topics(kafka_bootstrap_servers):
    """
    Test creation and verification of new topics.

    This test begins by deleting, if they exist, the topics
    to be created.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of hostname:port, for example "localhost:9092"
    """

    new_topics = {"topic.a", "topic.b", "topic.c"}
    delete_topics(
        bootstrap_servers=kafka_bootstrap_servers, topics_to_delete=new_topics
    )

    try:
        create_topics(
            bootstrap_servers=kafka_bootstrap_servers, topics_to_create=new_topics
        )
        all_topics = set(list_topics(bootstrap_servers=kafka_bootstrap_servers).keys())
    finally:
        # clean up the topics used for this test before asserting anything
        delete_topics(
            bootstrap_servers=kafka_bootstrap_servers, topics_to_delete=new_topics
        )

    assert new_topics & all_topics == new_topics


def test_create_topics_name_failure(kafka_bootstrap_servers):
    """
    Force a failure with an illegal topic name. Topics with
    valid names will be created.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of hostname:port, for example "localhost:9092"
    """

    new_topics = {"topic.a!", "topic.b", "topic.c"}
    delete_topics(
        bootstrap_servers=kafka_bootstrap_servers, topics_to_delete=new_topics
    )

    try:
        with pytest.raises(
            BlueskyKafkaException,
            match=re.escape("failed to create topic(s) ['topic.a!']"),
        ):
            create_topics(
                bootstrap_servers=kafka_bootstrap_servers,
                topics_to_create=new_topics,
            )

        all_topics = set(list_topics(bootstrap_servers=kafka_bootstrap_servers).keys())
    finally:
        delete_topics(
            bootstrap_servers=kafka_bootstrap_servers, topics_to_delete=new_topics
        )

    # the topics with legal names will be created
    assert new_topics & all_topics == {"topic.b", "topic.c"}


def test_delete_nonexisting_topic(kafka_bootstrap_servers):
    """
    Trying to delete a topic that does not exist causes no error.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of hostname:port, for example "localhost:9092"
    """
    delete_topics(
        bootstrap_servers=kafka_bootstrap_servers,
        topics_to_delete=["not.a.valid.topic!", "not.a.real.topic"],
    )