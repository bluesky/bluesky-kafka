import concurrent.futures
import logging
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from bluesky_kafka import BlueskyKafkaException


log = logging.getLogger("bluesky.kafka")


def get_cluster_metadata(bootstrap_servers, timeout=10, producer_config=None):
    """
    Return cluster metadata for the cluster specified by bootstrap_servers.

    Parameters
    ----------
    bootstrap_servers: str
        comma-delimited string of Kafka broker host:port, for example "localhost:9092"
    timeout: float
        maximum time to wait for a connection to a Kafka broker, 10s by default
    producer_config: dict
        optional dictionary of configuration parameters for the Producer used to query Kafka

    Returns
    -------
        confluent_kafka.admin.ClusterMetadata
    """
    if producer_config is None:
        producer_config = {}

    producer_config.update({"bootstrap.servers": bootstrap_servers})
    kafka_producer = Producer(producer_config)
    cluster_metadata = kafka_producer.list_topics(timeout=timeout)
    return cluster_metadata


def list_topics(bootstrap_servers, timeout=10, producer_config=None):
    """
    Return the topics dictionary from cluster metadata.

    Parameters
    ----------
    bootstrap_servers: str
        comma-delimited string of Kafka broker host:port, for example "localhost:9092"
    timeout: float
        maximum time to wait for a connection to a Kafka broker, 10s by default
    producer_config: dict
        optional dictionary of configuration parameters for the Producer used to query Kafka

    Returns
    -------
        dictionary of topic name -> TopicMetadata
    """
    if producer_config is None:
        producer_config = {}

    cluster_metadata = get_cluster_metadata(
        bootstrap_servers, timeout=timeout, producer_config=producer_config
    )
    return cluster_metadata.topics


def create_topics(
    bootstrap_servers,
    topics_to_create,
    num_partitions=1,
    replication_factor=1,
    max_checks=3,
    seconds_between_checks=1.0,
    admin_client_config=None,
):
    if admin_client_config is None:
        admin_client_config = {}

    admin_client_config.update({"bootstrap.servers": bootstrap_servers})
    admin_client = AdminClient(admin_client_config)
    log.debug(
        "creating topics '%s' with num_partitions=%d replication_factor=%d max_checks=%d seconds_between_checks=%.1f "
        "admin_client_config=%s",
        topics_to_create,
        num_partitions,
        replication_factor,
        max_checks,
        seconds_between_checks,
        admin_client_config,
    )

    topics_to_create_set = set(topics_to_create)
    existing_topics_set = set(list_topics(bootstrap_servers=bootstrap_servers).keys())
    log.debug("existing topics: '%s'", existing_topics_set)

    new_topics_to_futures = admin_client.create_topics(
        [
            NewTopic(
                topic=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            for topic in topics_to_create_set
        ]
    )
    futures_to_new_topics = {
        future: new_topic for new_topic, future in new_topics_to_futures.items()
    }
    for new_topic_future in concurrent.futures.as_completed(futures_to_new_topics):
        new_topic = futures_to_new_topics[new_topic_future]
        try:
            new_topic_future.result()
            log.debug(
                "topic '%s' has been created but may not be visible right away",
                new_topic,
            )
        except KafkaException as ke:
            # the verification check below will raise BlueskyKafkaException
            # if one or more topic is not created
            log.exception(ke)

    check_count = 0
    newly_created_topics_set = topics_to_create_set & existing_topics_set
    while (
        len(newly_created_topics_set) < len(topics_to_create_set)
        and check_count < max_checks
    ):
        check_count += 1
        log.debug(
            "create_topics sleeping for %.1fs before check %d of %d",
            seconds_between_checks,
            check_count,
            max_checks,
        )
        time.sleep(seconds_between_checks)
        existing_topics_set = set(
            list_topics(bootstrap_servers=bootstrap_servers).keys()
        )
        newly_created_topics_set = topics_to_create_set & existing_topics_set
        log.debug("newly created visible topics: '%s'", newly_created_topics_set)

    if not check_count < max_checks:
        failed_topics = sorted(list(topics_to_create_set - existing_topics_set))
        log.error("failed to create topic(s) '%s'", failed_topics)
        raise BlueskyKafkaException(f"failed to create topic(s) {failed_topics}")


def delete_topics(
    bootstrap_servers,
    topics_to_delete,
    max_checks=3,
    seconds_between_checks=1.0,
    admin_client_config=None,
):
    if admin_client_config is None:
        admin_client_config = {}

    admin_client_config.update({"bootstrap.servers": bootstrap_servers})

    log.info(
        "deleting topics '%s' from '%s' with max_checks=%d seconds_between_checks=%.1fs admin_client_config=%s",
        topics_to_delete,
        bootstrap_servers,
        max_checks,
        seconds_between_checks,
        admin_client_config,
    )

    topics_to_delete_set = set(topics_to_delete)
    existing_topics_set = set(list_topics(bootstrap_servers=bootstrap_servers).keys())

    existing_topics_to_delete_set = topics_to_delete_set & existing_topics_set

    if len(existing_topics_to_delete_set) == 0:
        log.debug(
            "topics to be deleted '%s' do not exist",
            topics_to_delete,
        )
    else:
        log.debug("deleting topics '%s'", existing_topics_to_delete_set)
        admin_client = AdminClient(admin_client_config)
        deleted_topics_to_futures = admin_client.delete_topics(
            topics=list(existing_topics_to_delete_set)
        )
        futures_to_delete_topics = {
            future: delete_topic
            for delete_topic, future in deleted_topics_to_futures.items()
        }
        for deleted_topic_future in concurrent.futures.as_completed(
            futures_to_delete_topics
        ):
            delete_topic = futures_to_delete_topics[deleted_topic_future]
            try:
                deleted_topic_future.result()
                log.info(
                    "topic '%s' has been deleted but may be visible for a short time",
                    delete_topic,
                )
            except KafkaException as ke:
                # the verification check below will raise BlueskyKafkaException
                # if one or more topic is not deleted
                log.exception(ke)

        check_count = 0
        undeleted_topics_set = existing_topics_to_delete_set & existing_topics_set
        while len(undeleted_topics_set) > 0 and check_count < max_checks:
            check_count += 1
            log.debug(
                "delete_topics sleeping for %.1fs before check %d of %d",
                seconds_between_checks,
                check_count,
                max_checks,
            )
            time.sleep(seconds_between_checks)
            existing_topics_set = set(
                list_topics(bootstrap_servers=bootstrap_servers).keys()
            )
            undeleted_topics_set = existing_topics_to_delete_set & existing_topics_set

        if not check_count < max_checks:
            log.error("failed to delete topics '%s'", undeleted_topics_set)
            raise BlueskyKafkaException(
                f"failed to delete topics '{undeleted_topics_set}'"
            )
