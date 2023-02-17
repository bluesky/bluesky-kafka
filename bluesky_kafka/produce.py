import logging

import msgpack
import msgpack_numpy as mpn


# this is the recommended way to modify the python msgpack
# package to handle numpy arrays with msgpack_numpy
mpn.patch()

logger = logging.getLogger(name="bluesky_kafka")


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


class BasicProducer:
    """
    Produce Kafka messages.

    This class is intended for two purposes:
        1) give bluesky users a simple way to produce general messages
        2) provide a parent class for Publisher and future DocumentProducer

    For guidance on flushing a BasicProducer see this:
        https://github.com/confluentinc/confluent-kafka-python/issues/137

    There is no default configuration. Consult Kafka documentation to determine
    appropriate producer configuration for specific situations.

    Parameters
    ----------
    topic : str
        Topic to which all messages will be published.
    bootstrap_servers : list of str
        List of Kafka server addresses as strings
        such as ``["broker1:9092", "broker2:9092", "127.0.0.1:9092"]``
    key : str
        Kafka "key" string. Specify any string to maintain message order. If None is specified
        no ordering will be imposed on messages.
    producer_config : dict, optional
        Dictionary of configuration information used to construct the underlying
        confluent_kafka.Producer.
    on_delivery : function(err, msg), optional
        A function to be called after a message has been delivered or after delivery has
        permanently failed.
    serializer : function, optional
        Function to serialize data. Default is msgpack.dumps.

    Example
    -------

    Send 10 messages to a broker at localhost:9092.

    import uuid
    from bluesky_kafka.produce import BasicProducer

    basic_producer = BasicProducer(
        topic="some.topic",
        bootstrap_servers=["localhost:9092"],
        key=str(uuid.uuid4())
    )

    ten_messages = list(range(10))
    produced_messages = []
    for message in ten_messages:
        basic_producer.produce(message)

    basic_producer.flush()

    """

    def __init__(
        self,
        topic,
        bootstrap_servers,
        key,
        producer_config=None,
        on_delivery=None,
        serializer=msgpack.dumps,
    ):
        from confluent_kafka import Producer as ConfluentProducer

        self.topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._key = key

        self._producer_config = dict()
        if producer_config is not None:
            self._producer_config.update(producer_config)

        if type(bootstrap_servers) is str:
            raise TypeError(
                "parameter `bootstrap_servers` must be a sequence of str, not str"
            )
        elif "bootstrap.servers" in self._producer_config:
            raise ValueError(
                "do not specify 'bootstrap.servers' in producer_config dictionary, "
                "use only the 'bootstrap_servers' parameter"
                f"\n{self}"
            )
        else:
            self._producer_config["bootstrap.servers"] = ",".join(bootstrap_servers)

        logger.debug("producer configuration: %s", self._producer_config)

        if on_delivery is None:
            self.on_delivery = default_delivery_report
        else:
            self.on_delivery = on_delivery

        self._producer = ConfluentProducer(self._producer_config)
        self._serializer = serializer

    def __str__(self):
        safe_config = dict(self._producer_config)
        if "sasl.password" in safe_config:
            safe_config["sasl.password"] = "****"
        return (
            f"{type(self)}("
            f"topic='{self.topic}', "
            f"key='{self._key}', "
            f"bootstrap_servers={self._bootstrap_servers}, "
            f"producer_config={safe_config}"
            ")"
        )

    def get_cluster_metadata(self, timeout=5.0):
        """
        A convenience method to return information about the Kafka cluster
        and this Producer's topic.

        Parameters
        ----------
        timeout: float, optional
            maximum time in seconds to wait before timing out, -1 for infinite timeout,
            default is 5.0s

        Returns
        -------
        cluster_metadata: confluent_kafka.admin.ClusterMetadata
        """
        cluster_metadata = self._producer.list_topics(topic=self.topic, timeout=timeout)
        return cluster_metadata

    def produce(self, message):
        """
        Produce a Kafka message.

        Parameters
        ----------
        message: serializable object
            this object will be serialized using the specified serializer and
            published as a Kafka message on this Producer's topic

        """
        logger.debug(
            "producing document to Kafka broker(s):"
            "topic: '%s'\n"
            "key:   '%s'\n"
            "doc:    %s",
            self.topic,
            self._key,
            message,
        )
        self._producer.produce(
            topic=self.topic,
            key=self._key,
            value=self._serializer(message),
            on_delivery=self.on_delivery,
        )
        # poll for delivery reports
        self._producer.poll(0)

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
