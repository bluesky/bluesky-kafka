import logging

from confluent_kafka import Consumer, Producer

import msgpack
import msgpack_numpy as mpn

from bluesky.run_engine import Dispatcher, DocumentNames
from event_model import RunRouter
from suitcase import mongo_normalized

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

# this is the recommended way to modify the python msgpack
# package to handle numpy arrays with msgpack_numpy
mpn.patch()

logger = logging.getLogger(name="bluesky.kafka")


def default_delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().

    Parameters
    ----------
    err : str
    msg : Kafka message without headers
    """

    if err is not None:
        logger.error("message delivery failed: %s", err)
    else:
        logger.debug(
            "message delivered to topic %s [partition %s]",
            msg.topic(),
            msg.partition(),
        )


class Publisher:
    """
    A class for publishing bluesky documents to a Kafka broker.

    The intention is that Publisher objects be subscribed to a RunEngine.

    Reference: https://github.com/confluentinc/confluent-kafka-python/issues/137

    There is no default configuration. A reasonable production configuration for use
    with bluesky is Kafka's "idempotent" configuration specified by
        producer_config = {
            "enable.idempotence": True
        }
    This is short for
        producer_config = {
            "acks": "all",                              # acknowledge only after all brokers receive a message
            "retries": sys.maxsize,                     # retry indefinitely
            "max.in.flight.requests.per.connection": 5  # maintain message order *when retrying*
        }

    This means three things:
        1) delivery acknowledgement is not sent until all replicate brokers have received a message
        2) message delivery will be retried indefinitely (messages will not be dropped by the Producer)
        3) message order will be maintained during retries

    A reasonable testing configuration is
        producer_config={
            "acks": 1,
            "request.timeout.ms": 5000,
        }

    Parameters
    ----------
    topic : str
        Topic to which all messages will be published.
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses as a string such as ``'127.0.0.1:9092'``.
    key : str
        Kafka "key" string. Specify a key to maintain message order. If None is specified
        no ordering will be imposed on messages.
    producer_config : dict, optional
        Dictionary configuration information used to construct the underlying Kafka Producer.
    on_delivery : function(err, msg), optional
        A function to be called after a message has been delivered or after delivery has
        permanently failed.
    flush_on_stop_doc : bool, optional
        False by default, set to True to flush() the underlying Kafka Producer when a stop
        document is received.
    serializer : function, optional
        Function to serialize data. Default is pickle.dumps.

    Example
    -------
    Publish documents from a RunEngine to a Kafka broker on localhost port 9092.

    >>> publisher = Publisher(
    >>>     topic="bluesky.documents",
    >>>     bootstrap_servers='localhost:9092',
    >>>     key="abcdef"
    >>> )
    >>> RE = RunEngine({})
    >>> RE.subscribe(publisher)
    """

    def __init__(
        self,
        topic,
        bootstrap_servers,
        key,
        producer_config=None,
        on_delivery=None,
        flush_on_stop_doc=False,
        serializer=msgpack.dumps,
    ):
        self.topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._key = key
        # in the case that "bootstrap.servers" is included in producer_config
        # combine it with the bootstrap_servers argument
        self._producer_config = dict()
        if producer_config is not None:
            self._producer_config.update(producer_config)
        if "bootstrap.servers" in self._producer_config:
            self._producer_config["bootstrap.servers"] = ",".join(
                [bootstrap_servers, self._producer_config["bootstrap.servers"]]
            )
        else:
            self._producer_config["bootstrap.servers"] = bootstrap_servers

        logger.debug("producer configuration: %s", self._producer_config)

        if on_delivery is None:
            self.on_delivery = default_delivery_report
        else:
            self.on_delivery = on_delivery

        self._flush_on_stop_doc = flush_on_stop_doc
        self._producer = Producer(self._producer_config)
        self._serializer = serializer

    def __str__(self):
        return (
            "bluesky_kafka.Publisher("
            f"topic='{self.topic}',"
            f"key='{self._key}',"
            f"bootstrap_servers='{self._bootstrap_servers}'"
            f"producer_config='{self._producer_config}'"
            ")"
        )

    def get_cluster_metadata(self, timeout=5.0):
        """
        Return information about the Kafka cluster and this Publisher's topic.

        Parameters
        ----------
        timeout: float, optional
            maximum time to wait before timing out, -1 for inifinte timeout, default is 5.0

        Returns
        -------
        cluster_metadata: confluent_kafka.admin.ClusterMetadata
        """
        cluster_metadata = self._producer.list_topics(topic=self.topic, timeout=timeout)
        return cluster_metadata

    def __call__(self, name, doc):
        """
        Publish the specified name and document as a Kafka message.

        Flush the Producer on every stop document. This guarantees
        that _at the latest_ all documents for a run will be delivered
        to the broker(s) at the end of the run. Without this flush
        the documents for a short run may wait for some time to be
        delivered. The flush call is blocking so it is a bad idea to
        flush after every document but reasonable to flush after a
        stop document since this is the end of the run.

        Parameters
        ----------
        name: str
            Document name, one of "start", "descriptor", "event", "resource", "datum", "stop".
        doc: dict
            event-model document dictionary

        """
        logger.debug(
            "publishing document to Kafka broker(s):"
            "topic: '%s'\n"
            "key:   '%s'\n"
            "name:  '%s'\n"
            "doc:   %s",
            self.topic,
            self._key,
            name,
            doc,
        )
        self._producer.produce(
            topic=self.topic,
            key=self._key,
            value=self._serializer((name, doc)),
            on_delivery=self.on_delivery,
        )
        if name == "stop" and self._flush_on_stop_doc:
            self.flush()

    def flush(self):
        """
        Flush all buffered messages to the broker(s).
        """
        logger.debug(
            "flushing Kafka Producer for topic '%s' and key '%s'",
            self.topic,
            self._key,
        )
        self._producer.flush()


# Most of this code was vendored from nslsii. We plan to update the
# nslsii code to import this class.
class PublisherRouter():
    """
    Subscribe a RunRouter to the specified RE to create Kafka Publishers.
    Each Publisher will publish documents from a single run to the
    Kafka topic "<beamline_name>.bluesky.documents".
    Parameters
    ----------
    RE: RunEngine
        the RunEngine to which the RunRouter will be subscribed
    beamline_name: str
        beamline name, for example "csx", to be used in building the
        Kafka topic to which messages will be published
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses as a string such as ``'10.0.137.8:9092'``
    producer_config: dict
        dictionary of Kafka Producer configuration settings
    Returns
    -------
    topic: str
        the Kafka topic on which bluesky documents will be published
    runrouter_token: int
        subscription token corresponding to the RunRouter subscribed to the RunEngine
        by this function
    """
    def __init__(
        self,
        topic,
        bootstrap_servers,
        producer_config=None,
    }:
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._producer_config = producer_config
        self._run_router =  RunRouter(factories=[self.kafka_publisher_factory])

    def __call__(self, name, doc):
        self._run_router(name, doc)

    def kafka_publisher_factory(self, name, start_doc):
        # create a Kafka Publisher for a single run
        kafka_publisher = Publisher(
            topic=self._topic,
            bootstrap_servers=self._bootstrap_servers,
            key=start_doc["uid"],
            producer_config=self._producer_config,
            flush_on_stop_doc=True,
            serializer=partial(msgpack.dumps, default=mpn.encode),
        )

        def handle_publisher_exceptions(name_, doc_):
            """
            Do not let exceptions from the Kafka producer kill the RunEngine.
            This is for testing and is not sufficient for Kafka in production.
            TODO: improve exception handling for production
            """
            try:
                kafka_publisher(name_, doc_)
            except Exception as ex:
                logger = logging.getLogger("nslsii")
                logger.exception(
                    "an error occurred when %s published %s\nname: %s\ndoc %s",
                    kafka_publisher,
                    name_,
                    doc_,
                )

        try:
            # call Producer.list_topics to test if we can connect to a Kafka broker
            # TODO: add list_topics method to KafkaPublisher
            cluster_metadata = kafka_publisher._producer.list_topics(
                topic=topic, timeout=5.0
            )
            logging.getLogger("nslsii").info(
                "connected to Kafka broker(s): %s", cluster_metadata
            )
            return [handle_publisher_exceptions], []
        # TODO: raise BlueskyException or similar from KafkaPublisher.list_topics
        except Exception:
            # For now failure to connect to a Kafka broker will not be considered a
            # significant problem because we are not relying on Kafka. When and if
            # we do rely on Kafka for storing documents we will need a more robust
            # response here.
            # TODO: improve exception handling for production
            nslsii_logger = logging.getLogger("nslsii")
            nslsii_logger.exception("%s failed to connect to Kafka", kafka_publisher)

            # documents will not be published to Kafka brokers
            return [], []


class BlueskyConsumer:
    """
    Process Bluesky documents received over the network from a Kafka server.

    There is no default configuration. A reasonable configuration for production is
        consumer_config={
            "auto.offset.reset": "latest"
        }

    Parameters
    ----------
    topics : list of str
        List of topics as strings such as ["topic-1", "topic-2"]
    bootstrap_servers : str
        Comma-delimited list of Kafka server addresses as a string
        such as ``'broker1:9092,broker2:9092,127.0.0.1:9092'``
    group_id : str
        Required string identifier for the consumer's Kafka Consumer group.
    consumer_config : dict
        Override default configuration or specify additional configuration
        options to confluent_kafka.Consumer.
    polling_duration : float
        Time in seconds to wait for a message before running function work_during_wait
        in the _poll method. Default is 0.05.
    deserializer : function, optional
        Function to deserialize data. Default is msgpack.loads.
    process_document : function(consumer, topic, name, doc), optional
        A function that procceses received documents, this allows you to have custom document
        processing without the need to make a subclass. The function signature must match
        BlueskyConsumer.process_document(consumer, topic, name, document).

    Example
    -------

    Print all documents generated by remote RunEngines.

    >>> consumer = BlueskyConsumer(
    >>>         topics=["abc.bluesky.documents", "xyz.bluesky.documents"],
    >>>         bootstrap_servers='localhost:9092',
    >>>         group_id="print-document-group",
    >>>         consumer_config={
    >>>             "auto.offset.reset": "latest"
    >>>         }
    >>>         process_document=lambda consumer, topic, name, doc: print(doc)
    >>>    )
    >>> consumer.start()  # runs until interrupted
    """

    def __init__(
        self,
        topics,
        bootstrap_servers,
        group_id,
        consumer_config=None,
        polling_duration=0.05,
        deserializer=msgpack.loads,
        process_document=None,
    ):
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._deserializer = deserializer
        self._process_document = process_document
        self.polling_duration = polling_duration

        self._consumer_config = dict()
        if consumer_config is not None:
            self._consumer_config.update(consumer_config)

        if "group.id" in self._consumer_config:
            raise ValueError(
                "do not specify 'group.id' in consumer_config, use only the 'group_id' argument"
            )
        else:
            self._consumer_config["group.id"] = group_id

        if "bootstrap.servers" in self._consumer_config:
            self._consumer_config["bootstrap.servers"] = ",".join(
                [bootstrap_servers, self._consumer_config["bootstrap.servers"]]
            )
        else:
            self._consumer_config["bootstrap.servers"] = bootstrap_servers

        logger.debug(
            "BlueskyConsumer configuration:\n%s", self._consumer_config,
        )
        logger.debug("subscribing to Kafka topic(s): %s", topics)

        self.consumer = Consumer(self._consumer_config)
        self.consumer.subscribe(topics=topics)
        self.closed = False

    def _poll(self, work_during_wait=None):
        """
        This method defines the polling loop in which messages are pulled from
        one or more Kafka brokers and processed with self.process().

        The polling loop will be interrupted if self.process() returns False.

        Parameters
        ----------
        work_during_wait : function(), optional
            a parameter-less function to be called between calls to Consumer.poll

        """

        def no_work_during_wait():
            # do nothing between message deliveries
            pass

        if work_during_wait is None:
            work_during_wait = no_work_during_wait

        while True:
            msg = self.consumer.poll(self.polling_duration)
            if msg is None:
                # no message was delivered
                # do some work before polling again
                work_during_wait()
            elif msg.error():
                logger.error("Kafka Consumer error: %s", msg.error())
            else:
                try:
                    if self.process(msg) is False:
                        logger.debug(
                            "breaking out of polling loop after process(msg) returned False"
                        )
                        break
                except Exception as exc:
                    logger.exception(exc)

    def process(self, msg):
        """
        Deserialize the Kafka message and extract the bluesky document.

        Document processing is delegated to self.process_document(name, document).

        This method can be overridden to customize message handling.

        Parameters
        ----------
        msg : Kafka message

        Returns
        -------
        continue_polling : bool
            return True to continue polling, False to break out of the polling loop
        """
        name, doc = self._deserializer(msg.value())
        logger.debug(
            "BlueskyConsumer deserialized document with "
            "topic %s for Kafka Consumer name: %s doc: %s",
            msg.topic(),
            name,
            doc,
        )
        continue_polling = self.process_document(msg.topic(), name, doc)
        return continue_polling

    def process_document(self, topic, name, doc):
        """
        Subclasses may override this method to process documents.
        Alternatively a document-processing function can be specified at init time
        and it will be called here. The function must have the same signature as
        this method (except for the `self` parameter).

        If this method returns False the BlueskyConsumer will break out of the
        polling loop.

        Parameters
        ----------
        topic : str
            the Kafka topic of the message containing name and doc
        name : str
            bluesky document name: `start`, `descriptor`, `event`, etc.
        doc : dict
            bluesky document

        Returns
        -------
        continue_polling : bool
            return False to break out of the polling loop, return True to continue polling
        """
        if self._process_document is None:
            raise NotImplementedError(
                "This class must either be subclassed to override the "
                "process_document method, or have a process function passed "
                "in at init time via the process_document parameter."
            )
        else:
            continue_polling = self._process_document(self.consumer, topic, name, doc)
            return continue_polling

    def start(self, work_during_wait=None):
        """
        Start the polling loop.

        Parameters
        ----------
        work_during_wait : function(), optional
            a parameter-less function to be called between calls to Consumer.poll

        """
        if self.closed:
            raise RuntimeError(
                "This BlueskyConsumer has already been "
                "started and interrupted. Create a fresh "
                f"instance with {repr(self)}"
            )
        try:
            self._poll(work_during_wait=work_during_wait)
        except Exception:
            self.stop()
            raise
        finally:
            self.stop()

    def stop(self):
        """
        Close the underlying consumer.
        """
        self.consumer.close()
        self.closed = True


class RemoteDispatcher(Dispatcher):
    """
    Dispatch documents from Kafka to bluesky callbacks.

    There is no default configuration. A reasonable configuration for production is
        consumer_config={
            "auto.offset.reset": "latest"
        }

    Parameters
    ----------
    topics: list
        List of topics as strings such as ["topic-1", "topic-2"]
    bootstrap_servers : str
        Comma-delimited list of Kafka server addresses as a string such as ``'127.0.0.1:9092'``
    group_id: str
        Required string identifier for Kafka Consumer group
    consumer_config: dict
        Override default configuration or specify additional configuration
        options to confluent_kafka.Consumer.
    polling_duration: float
        Time in seconds to wait for a message before running function work_while_waiting.
        Default is 0.05.
    deserializer: function, optional
        optional function to deserialize data. Default is msgpack.loads.

    Example
    -------
    Print all documents generated by remote RunEngines.

    >>> d = RemoteDispatcher(
    >>>         topics=["abc.bluesky.documents", "ghi.bluesky.documents"],
    >>>         bootstrap_servers='localhost:9092',
    >>>         group_id="document-printers",
    >>>         consumer_config={
    >>>             "auto.offset.reset": "latest"
    >>>         }
    >>>    )
    >>> d.subscribe(print)
    >>> d.start()  # runs until interrupted
    """

    def __init__(
        self,
        topics,
        bootstrap_servers,
        group_id,
        consumer_config=None,
        polling_duration=0.05,
        deserializer=msgpack.loads,
    ):
        super().__init__()

        self._bluesky_consumer = BlueskyConsumer(
            topics=topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            consumer_config=consumer_config,
            process_document=self.process_document,
            polling_duration=polling_duration,
            deserializer=deserializer,
        )

        self.closed = False

    def process_document(self, consumer, topic, name, document):
        """
        Send bluesky document to RemoteDispatcher.process(name, doc).

        Parameters
        ----------
        consumer : confluent_kafka.consumer
            the underlying Kafka Consumer, useful for committing messages
        topic : str
            the Kafka topic of the message containing name and doc
        name : str
            bluesky document name: `start`, `descriptor`, `event`, etc.
        doc : dict
            bluesky document

        Return
        ------
        continue_polling : bool

        """
        self.process(DocumentNames[name], document)
        return True

    def start(self, work_during_wait=None):
        """
        Start the BlueskyConsumer polling loop.

        Parameters
        ----------
        work_during_wait : function()
            optional function to be called inside the polling loop between polls
        """
        if self.closed:
            raise RuntimeError(
                "This RemoteDispatcher has already been "
                "started and interrupted. Create a fresh "
                f"instance with {repr(self)}"
            )
        try:
            self._bluesky_consumer.start(work_during_wait=work_during_wait)
        finally:
            self.stop()

    def stop(self):
        """
        Mark this RemoteDispatcher as closed.
        """
        self.closed = True


class MongoConsumer(BlueskyConsumer):
    """
    Subclass of BlueskyConsumer that is specialized for inserting into a mongo
    database determined by the topic name
    """

    class SerializerFactory(dict):
        """
        Like a defaultdict, but it makes a Serializer based on the
        key, which in this case is the topic name.
        """

        def __init__(self, mongo_uri, auth_source):
            self._mongo_uri = mongo_uri
            self._auth_source = auth_source

        def get_database(self, topic):
            return topic.replace(".", "-")

        def __missing__(self, topic):
            result = self[topic] = mongo_normalized.Serializer(
                self._mongo_uri
                + "/"
                + self.get_database(topic)
                + "?authSource="
                + self._auth_source,
                self._mongo_uri
                + "/"
                + self.get_database(topic)
                + "?authSource="
                + self._auth_source,
            )
            return result

    def __init__(self, mongo_uri, auth_source="admin", *args, **kwargs):
        self._serializers = self.SerializerFactory(mongo_uri, auth_source)
        super().__init__(*args, **kwargs)

    def process_document(self, topic, name, doc):
        result_name, result_doc = self._serializers[topic](name, doc)
        if name == "stop":
            self.consumer.commit(asynchronous=False)

        return True


class BlueskyStream(BlueskyConsumer):
    """
    This class is intended to subscribe to a single topic only.
    """
    def __init__(self, input_topic, output_topic, group_id,
                 bootstrap_servers, consumer_config=None,
                 producer_config=None, polling_duration=0.05,
                 process_document):
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._consumer_config = consumer_config
        self._producer_config = producer_config
        self._polling_duration = polling_duration
        self._process_document = process_document
        self._producer_router = PublisherRouter(self._output_topic,
                                                bootstrap_servers,
                                                producer_config=self._producer_config)
        super().__init__(
            [self._input_topic],
            self._bootstrap_servers,
            self._group_id,
            consumer_config=self._consumer_config,
            polling_duration=self._polling_duration,
            deserializer=msgpack.loads,
            process_document=self._process_document
        )
