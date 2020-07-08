import logging
import pickle

from confluent_kafka import Consumer, Producer

from bluesky.run_engine import Dispatcher, DocumentNames

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

logger = logging.getLogger(name="bluesky.kafka")


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().

    Parameters
    ----------
    err
    msg

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
    A callback that publishes documents to a Kafka server.

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
    topic: str
        Topic to which all messages will be published.
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses as a string such as ``'127.0.0.1:9092'``.
    key: str
        Kafka "key" string. Specify a key to maintain message order. If None is specified
        no ordering will be imposed on messages.
    producer_config: dict, optional
        Dictionary configuration information used to construct the underlying Kafka Producer.
    flush_on_stop_doc: bool, optional
        False by default, set to True to flush() the underlying Kafka Producer when a stop
        document is received.
    serializer: function, optional
        Function to serialize data. Default is pickle.dumps.

    Example
    -------

    Publish documents from a RunEngine to a Kafka broker on localhost on port 9092.

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
        flush_on_stop_doc=False,
        serializer=pickle.dumps,
    ):
        self._topic = topic
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

        logger.info("producer configuration: %s", self._producer_config)

        self._flush_on_stop_doc = flush_on_stop_doc
        self._producer = Producer(self._producer_config)
        self._serializer = serializer

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
            "KafkaProducer(topic=%s key=%s msg=[name=%s, doc=%s])",
            self._topic,
            self._key,
            name,
            doc,
        )
        self._producer.produce(
            topic=self._topic,
            key=self._key,
            value=self._serializer((name, doc)),
            callback=delivery_report,
        )
        if name == "stop" and self._flush_on_stop_doc:
            self.flush()

    def flush(self):
        """
        Flush all buffered messages to the broker(s).
        """
        logger.debug(
            "flushing Kafka Producer for topic % and key %s", self._topic, self._key
        )
        self._producer.flush()


class RemoteDispatcher(Dispatcher):
    """
    Dispatch documents received over the network from a Kafka server.

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
        optional function to deserialize data. Default is pickle.loads.

    Example
    -------

    Print all documents generated by remote RunEngines.

    >>> d = RemoteDispatcher(
    >>>         topics=["abc.def", "ghi.jkl"],
    >>>         bootstrap_servers='localhost:9092',
    >>>         group_id="xyz",
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
        deserializer=pickle.loads,
    ):
        super().__init__()

        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self.polling_duration = polling_duration
        self._deserializer = deserializer

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

        logger.info(
            "starting RemoteDispatcher with Kafka Consumer configuration:\n%s",
            self._consumer_config,
        )
        logger.info("subscribing to Kafka topic(s): %s", topics)

        self._consumer = Consumer(self._consumer_config)
        self._consumer.subscribe(topics=topics)
        self.closed = False

    def _poll(self, work_during_wait):
        while True:
            msg = self._consumer.poll(self.polling_duration)

            if msg is None:
                # no message was delivered
                # do some work before polling again
                work_during_wait()
            elif msg.error():
                logger.error("Kafka Consumer error: %s", msg.error())
            else:
                try:
                    name, doc = self._deserializer(msg.value())
                    logger.debug(
                        "RemoteDispatcher deserialized document with "
                        "topic %s for Kafka Consumer name: %s doc: %s",
                        msg.topic(),
                        name,
                        doc,
                    )
                    self.process(DocumentNames[name], doc)
                except Exception as exc:
                    logger.exception(exc)

    def start(self, work_during_wait=None):
        def no_work_during_wait():
            # do nothing between message deliveries
            pass

        if work_during_wait is None:
            work_during_wait = no_work_during_wait

        if self.closed:
            raise RuntimeError(
                "This RemoteDispatcher has already been "
                "started and interrupted. Create a fresh "
                f"instance with {repr(self)}"
            )
        try:
            self._poll(work_during_wait=work_during_wait)
        except Exception:
            self.stop()
            raise

    def stop(self):
        self._consumer.close()
        self.closed = True
