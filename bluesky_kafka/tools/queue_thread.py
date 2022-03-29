import logging
import threading
import queue
import uuid

from collections import namedtuple

from bluesky_kafka import BlueskyKafkaException, Publisher
from bluesky_kafka.utils import list_topics


"""
A namedtuple for holding details of a publisher queue and thread
constructed by build_publisher_queue_and_thread(...).

This namedtuple is not intended to be created by client code.
"""
_PublisherQueueThreadDetails = namedtuple(
    "PublisherQueueThreadDetails",
    [
        "publisher_queue",
        "publisher_thread",
        "publisher_thread_stop_event",
        "put_on_publisher_queue",
    ],
)


def _start_kafka_publisher_thread(
    publisher, publisher_queue=None, publisher_queue_timeout=1
):
    """Start a thread to take (name, document) tuples off a Queue and publish them as Kafka messages.

    This function is intended for internal use and testing, not for use in client code.

    Parameters
    ----------
    publisher: bluesky_kafka.Publisher
        publishes (name, document) tuples as Kafka messages
    publisher_queue: queue.Queue-like object (optional)
        (name, document) tuples placed on this queue will be published as Kafka messages
        by the specified publisher; by default a queue.Queue will be used
    publisher_queue_timeout: float
        time in seconds to wait for a document to become available on the publisher_queue
        before checking if the publisher thread should terminate; default is 1s

    Returns
    -------
    publisher_queue
        (name, document) tuples placed on this queue.Queue-like object will be published
        as Kafka messages by the specified publisher
    publisher_thread
        threading.Thread responsible for running function publish_documents_from_publisher_queue
    publisher_thread_stop_event
        call set() on this threading.Event to terminate publisher_thread

    """

    def publish_documents_from_publisher_queue(
        publisher_,
        publisher_queue_,
        publisher_thread_stop_event_,
        publisher_queue_timeout_=1,
    ):
        """
        This function is intended to execute in a dedicated thread. It defines a polling
        loop that takes (name, document) tuples from publisher_queue_ as they become
        available and uses publisher_ to publish those tuples as Kafka messages.

        The intention is to separate a RunEngine or other source of documents
        from a Publisher in order to insulate plans from Publisher failures that
        might otherwise interrupt data collection.

        Parameters
        ---------
        publisher_: bluesky_kafka.Publisher
            publishes (name, document) tuples as Kafka messages on a beamline-specific topic
        publisher_queue_: queue.Queue-like object
            (name, document) tuples placed on this queue will be published as
            Kafka messages by the publisher_
        publisher_thread_stop_event_: threading.Event
            the polling loop will terminate cleanly if publisher_thread_stop_event_ is set
        publisher_queue_timeout_: float
            time in seconds to wait for a document to become available on publisher_queue_
            before checking if publisher_thread_stop_event_ has been set
        """
        name_ = None
        document_ = None
        published_document_count = 0
        logger_ = logging.getLogger("bluesky_kafka")
        logger_.info("starting Kafka message publishing loop")
        while not publisher_thread_stop_event_.is_set():
            try:
                name_, document_ = publisher_queue_.get(
                    timeout=publisher_queue_timeout_
                )
                publisher_(name_, document_)
                published_document_count += 1
            except queue.Empty:
                # publisher_queue_.get() timed out waiting for a new document
                # the while condition will now be checked to see if someone
                # has requested that this thread terminate
                # if not then try again to get a new document from publisher_queue_
                pass
            except BaseException:
                # something bad happened while trying to publish a Kafka message
                # log the exception and continue taking documents from publisher_queue_
                logger_.exception(
                    "an error occurred after %d successful Kafka messages when '%s' "
                    "attempted to publish on topic %s\nname: '%s'\ndoc '%s'",
                    published_document_count,
                    publisher_,
                    publisher_.topic,
                    name_,
                    document_,
                )

    if publisher_queue is None:
        publisher_queue = queue.Queue()
    publisher_thread_stop_event = threading.Event()
    publisher_thread = threading.Thread(
        # include a random string in the thread name in case
        # more than one Kafka publisher thread is started
        name=f"kafka-publisher-thread-{str(uuid.uuid4())[:8]}",
        target=publish_documents_from_publisher_queue,
        kwargs={
            "publisher_": publisher,
            "publisher_queue_": publisher_queue,
            "publisher_thread_stop_event_": publisher_thread_stop_event,
            "publisher_queue_timeout_": publisher_queue_timeout,
        },
        daemon=True,
    )
    publisher_thread.start()
    logger = logging.getLogger("bluesky_kafka")
    logger.info("Kafka publisher thread has started")

    publisher_queue_thread_details = _PublisherQueueThreadDetails(
        publisher_queue=publisher_queue,
        publisher_thread=publisher_thread,
        publisher_thread_stop_event=publisher_thread_stop_event,
        put_on_publisher_queue=lambda name, document: publisher_queue.put(
            (name, document)
        ),
    )

    return publisher_queue_thread_details


def build_kafka_publisher_queue_and_thread(
    topic,
    bootstrap_servers,
    producer_config,
    publisher_queue=None,
    publisher_queue_timeout=1,
):
    """Create and start a separate thread to publish bluesky documents as Kafka messages.

    This function performs four tasks:
      1) verify a Kafka broker with the specified topic is available
      2) instantiate a bluesky_kafka.Publisher with the specified topic and configuration
      3) if publisher_queue=None is specified instantiate a queue.Queue
      4) call _start_kafka_publisher_thread to start a polling loop to publish (name, document)
         tuples placed on the publisher queue
      5) return the publisher queue so client code can put (name, document) tuples on it

    Parameters
    ----------
    topic: str
        topic for Kafka messages
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses or hostnames and ports as a string
        such as ``'kafka1:9092,kafka2:9092``
    producer_config: dict
        dictionary of Kafka Producer configuration settings
    publisher_queue: queue.Queue-like object (optional)
        (name, document) tuples placed on this queue will be published as Kafka messages
        by kafka_publisher; by default a queue.Queue will be used
    publisher_queue_timeout: float (optional)
        time in seconds to wait for a document to become available on the publisher_queue
        before checking if the publisher thread should terminate; default is 1s

    Returns
    -------
    PublisherQueueThreadDetails: namedtuple with these attributes:
        publisher_queue: queue.Queue-like object
            (name, document) tuples placed on this queue will be published as Kafka messages
            by kafka_publisher. If no Kafka broker can be found the returned publisher_queue
            will be None.
        publisher_thread: threading.Thread
            the thread that publishes (name, document) tuples placed on the publisher_queue
            as Kafka messages
        publisher_thread_stop_event: threading.Event
            call set() on this threading.Event to terminate the message publication loop;
            this may have unexpected memory consequences if (name, document) tuples continue
            to be placed on publisher_queue
        put_on_thread_queue: function(name, document) -> None
            a convenience function to put name, document pairs on the publisher_queue, suitable for
            RunEngine subscription: RE.subscribe(put_on_thread_queue)
    """

    logger = logging.getLogger("bluesky_kafka")

    logger.info("connecting to Kafka broker(s): '%s'", bootstrap_servers)
    # verify the specified topic exists on the Kafka broker(s) before subscribing
    topic_to_topic_metadata = list_topics(
        bootstrap_servers=bootstrap_servers, producer_config=producer_config
    )
    if topic in topic_to_topic_metadata:
        # since the topic exists, build a Publisher for the topic
        kafka_publisher = Publisher(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            # specify a key to guarantee messages will be delivered in order
            key=str(uuid.uuid4()),
            producer_config=producer_config,
            flush_on_stop_doc=True,
        )
        publisher_queue_thread_details = _start_kafka_publisher_thread(
            publisher_queue=publisher_queue,
            publisher=kafka_publisher,
            publisher_queue_timeout=publisher_queue_timeout,
        )
        logger.info(
            "bluesky documents placed on queue `%s` will be published on Kafka topic '%s'",
            publisher_queue_thread_details.publisher_queue,
            topic,
        )
    else:
        logger.error(
            f"topic `%s` does not exist on Kafka broker(s) `%s`",
            topic,
            bootstrap_servers,
        )
        raise BlueskyKafkaException(
            f"topic `{topic}` does not exist on Kafka broker(s) `{bootstrap_servers}`",
        )

    return publisher_queue_thread_details
