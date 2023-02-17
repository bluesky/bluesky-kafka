import logging

import msgpack
import msgpack_numpy as mpn

from bluesky_kafka.consume import BasicConsumer
from bluesky_kafka.produce import BasicProducer

from bluesky.run_engine import Dispatcher, DocumentNames
from suitcase import mongo_normalized

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

# this is the recommended way to modify the python msgpack
# package to handle numpy arrays with msgpack_numpy
mpn.patch()

logger = logging.getLogger(name="bluesky_kafka")


class BlueskyKafkaException(Exception):
    pass


class Publisher(BasicProducer):
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
        Dictionary of configuration information used to construct the underlying
        confluent_kafka.Producer.
    on_delivery : function(err, msg), optional
        A function to be called after a message has been delivered or after delivery has
        permanently failed.
    flush_on_stop_doc : bool, optional
        False by default, set to True to flush() the underlying confluent_kafka.Producer when
        a stop document is published.
    serializer : function, optional
        Function to serialize data. Default is pickle.dumps.

    Example
    -------
    Publish documents from a RunEngine to a Kafka broker.

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
        sanitized_producer_config = {}
        if producer_config is not None:
            sanitized_producer_config.update(producer_config)

        bootstrap_servers_list = bootstrap_servers.split(",")
        if "bootstrap.servers" in sanitized_producer_config:
            bootstrap_servers_list.extend(
                sanitized_producer_config.pop("bootstrap.servers").split(",")
            )

        super().__init__(
            topic=topic,
            bootstrap_servers=bootstrap_servers_list,
            key=key,
            producer_config=sanitized_producer_config,
            on_delivery=on_delivery,
            serializer=serializer,
        )

        self._flush_on_stop_doc = flush_on_stop_doc

    def __call__(self, name, doc):
        """
        Publish the specified name and document as a Kafka message.

        Flushing the Producer on every stop document guarantees
        that _at the latest_ all documents for a run will be delivered
        to the broker(s) at the end of the run. Without this flush
        the documents for a run may wait some time to be delivered.
        The flush call is blocking so it is a bad idea to flush after
        every document but reasonable to flush after a stop document
        since it is generated the end of the run.

        Parameters
        ----------
        name: str
            Document name, one of "start", "descriptor", "event", "resource", "datum", "stop".
        doc: dict
            event-model document dictionary

        """
        self.produce(message=(name, doc))
        if self._flush_on_stop_doc and name == "stop":
            self.flush()


class BlueskyConsumer(BasicConsumer):
    """
    Processes Bluesky documents received from a Kafka broker.

    There is no default configuration. A reasonable configuration for production is
        consumer_config={
            "auto.offset.reset": "latest"
        }

    Parameters
    ----------
    topics : list of str
        List of existing_topics as strings such as ["topic-1", "topic-2"]
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

    Print all documents.

    >>> consumer = BlueskyConsumer(
    >>>         topics=["abc.bluesky.documents", "xyz.bluesky.documents"],
    >>>         bootstrap_servers='localhost:9092',
    >>>         group_id="print.document.group",
    >>>         consumer_config={
    >>>             "auto.offset.reset": "latest"  # consume messages published after this consumer starts
    >>>         }
    >>>         process_document=lambda consumer, topic, name, doc: print(doc)
    >>>    )
    >>> bluesky_consumer.start()
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
        sanitized_consumer_config = {}
        if consumer_config is not None:
            sanitized_consumer_config.update(consumer_config)

        bootstrap_servers_list = bootstrap_servers.split(",")
        if "bootstrap.servers" in sanitized_consumer_config:
            bootstrap_servers_list.extend(
                sanitized_consumer_config.pop("bootstrap.servers").split(",")
            )

        super().__init__(
            topics=topics,
            bootstrap_servers=bootstrap_servers_list,
            group_id=group_id,
            consumer_config=sanitized_consumer_config,
            polling_duration=polling_duration,
            deserializer=deserializer,
        )

        self._process_document = process_document

    def process_message(self, topic, message):
        name, document = message
        self.process_document(topic, name, document)

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
            continue_polling = self._process_document(self._consumer, topic, name, doc)
            return continue_polling

    def start(self, continue_polling=None, work_during_wait=None):
        """
        Start the polling loop.

        Parameters
        ----------
        continue_polling: function(), optional
            a parameter-less function called before every call to Consumer.poll,
            the intention is to allow outside logic to stop the Consumer

        work_during_wait : function(), optional
            a parameter-less function to be called between calls to Consumer.poll

        """
        self.start_polling(
            continue_polling=continue_polling, work_during_wait=work_during_wait
        )

    def stop(self):
        """
        Close the underlying consumer.
        """
        self.stop_polling()


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

    def start(self, continue_polling=None, work_during_wait=None):
        """
        Start the BlueskyConsumer polling loop.

        Parameters
        ----------
        continue_polling: function(), optional
            a parameter-less function called before every call to Consumer.poll,
            the intention is to allow outside logic to stop the Consumer

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
            self._bluesky_consumer.start(
                continue_polling=continue_polling, work_during_wait=work_during_wait
            )
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

        def __init__(self, mongo_uri, topic_database_map, auth_source, tls):
            self._mongo_uri = mongo_uri
            self._topic_database_map = topic_database_map
            self._auth_source = auth_source
            self._tls = "&tls=true" if tls else ""

        def __missing__(self, topic):
            result = self[topic] = mongo_normalized.Serializer(
                self._mongo_uri
                + "/"
                + self._topic_database_map[topic]
                + "?authSource="
                + self._auth_source
                + self._tls,
                self._mongo_uri
                + "/"
                + self._topic_database_map[topic]
                + "?authSource="
                + self._auth_source
                + self._tls,
            )
            return result

    def __init__(
        self,
        mongo_uri,
        topic_database_map,
        auth_source="admin",
        tls=False,
        *args,
        **kwargs,
    ):
        self._serializers = self.SerializerFactory(
            mongo_uri, topic_database_map, auth_source, tls
        )
        super().__init__(*args, **kwargs)

    def process_document(self, topic, name, doc):
        result_name, result_doc = self._serializers[topic](name, doc)
        if name == "stop":
            self._consumer.commit(asynchronous=False)
        return True
