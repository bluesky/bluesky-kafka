import logging

import msgpack
import msgpack_numpy as mpn


# this is the recommended way to modify the python msgpack
# package to handle numpy arrays with msgpack_numpy
mpn.patch()

logger = logging.getLogger(name="bluesky_kafka")


class BasicConsumer:
    """
    A simple class for consuming Kafka messages, includes a polling loop.

    There are two intended usages:
      1. Subclass BasicConsumer and override BasicConsumer.process_message
      2. Instantiate BasicConsumer and provide a process_message function.

    There is no default configuration. Consult Kafka documentation to determine
    appropriate consumer configuration for specific situations.

    Parameters
    ----------
    topics : list of str
        List of existing topics as strings such as ["topic-1", "topic-2"]
    bootstrap_servers : list of str
        List of Kafka server addresses as strings
        such as ``["broker1:9092", "broker2:9092", "127.0.0.1:9092"]``
    group_id : str
        Required string identifier for the consumer's Kafka Consumer group.
    consumer_config : dict
        Specify additional configuration options to confluent_kafka.Consumer. Do not
        specify "bootstrap.servers" or "group.id". Use the parameters to this method instead.
        Consult the Confluent Python client documentation for supported options.
    polling_duration : float
        Time in seconds to wait for a message before running function work_during_wait
        in the _poll method. Default is 0.05.
    deserializer : function, optional
        Function to deserialize data. Default is msgpack.loads.
    process_message : function(consumer, topic, message), optional
        A function that procceses a deserialized message. This allows you to have custom message
        processing without the need to make a subclass. The function signature must match
        Consumer.process_message(consumer, topic, message) and return a boolean. If the return
        value is False, the polling loop will terminate.

    Example
    -------

    Print the first ten messages received from a broker at localhost:9092.

    import uuid
    from bluesky_kafka.consume import BasicConsumer

    consumed_messages = []
    def print_ten_messages(consumer, topic, message):
        print(f"consumed message: {message} from topic {topic}")
        consumed_messages.append(message)
        return len(consumed_messages) < 10

    consumer = BasicConsumer(
        topics=["some.topic"],
        bootstrap_servers=["localhost:9092"],
        group_id=str(uuid.uuid4()),
        consumer_config={
            # consume messages published before this consumer starts
            "auto.offset.reset": "earliest"
        },
        process_message=print_ten_messages
    )

    # runs until print_ten_messages returns False
    consumer.start_polling()

    """

    def __init__(
        self,
        topics,
        bootstrap_servers,
        group_id,
        consumer_config=None,
        polling_duration=0.05,
        deserializer=msgpack.loads,
        process_message=None,
    ):
        from confluent_kafka import Consumer as ConfluentConsumer

        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._deserializer = deserializer
        self._process_message = process_message
        self.polling_duration = polling_duration

        self._consumer_config = dict()
        if consumer_config is not None:
            self._consumer_config.update(consumer_config)

        if "group.id" in self._consumer_config:
            raise ValueError(
                "do not specify 'group.id' in consumer_config, use only the 'group_id' parameter"
            )
        else:
            self._consumer_config["group.id"] = group_id

        if type(bootstrap_servers) is str:
            raise TypeError(
                "parameter `bootstrap_servers` must be a sequence of str, not str"
            )
        elif "bootstrap.servers" in self._consumer_config:
            raise ValueError(
                "do not specify 'bootstrap.servers' in consumer_config dictionary, "
                "use only the 'bootstrap_servers' parameter"
            )
        else:
            self._consumer_config["bootstrap.servers"] = ",".join(bootstrap_servers)

        logger.debug(
            "BlueskyConsumer configuration:\n%s",
            self._consumer_config,
        )
        logger.debug("subscribing to Kafka topic(s): %s", topics)

        self._consumer = ConfluentConsumer(self._consumer_config)
        self._consumer.subscribe(topics=topics)
        self.closed = False

    def __str__(self):
        safe_config = dict(self._consumer_config)
        if "sasl.password" in safe_config:
            safe_config["sasl.password"] = "****"
        return (
            f"{type(self)}("
            f"topics={self._topics}, "
            f"consumer_config={safe_config}"
            ")"
        )

    def _poll(
        self,
        *,
        continue_polling=None,
        work_during_wait=None,
        on_message_error=None,
        on_exception=None,
    ):
        """
        This method is not intended to be called directly.

        This method defines the polling loop in which messages are received
        from Kafka brokers and processed with self._deserialize_and_process().

        This method will not return until the polling loop is terminated when
        one these conditions is met:
            1. a custom continue_polling function returns False
            2. a custom on_message_error function returns False
            3. a custom process_message function returns False
            4. the process_message method defined in a subclass returns False
            5. a KeyboardInterrupt exception is raised
            6. a custom on_exception function raises an exception

        Parameters
        ----------
        continue_polling : function(), optional
            a parameter-less function called before every call to Consumer.poll,
            the intention is to allow an outside force to terminate the polling loop

        work_during_wait : function(), optional
            a parameter-less function to be called between calls to Consumer.poll

        on_message_error : function(message), optional
            a one-parameter function to handle messages marked as being in an
            error state by a Kafka broker, if this function returns False the
            polling loop will terminate

        on_exception : function(exception), optional
            a one-parameter function to handle exceptions raised inside the polling loop
        """

        if continue_polling is None:

            def never_stop_polling():
                return True

            continue_polling = never_stop_polling

        if work_during_wait is None:

            def no_work_during_wait():
                # do nothing between message deliveries
                pass

            work_during_wait = no_work_during_wait

        if on_message_error is None:

            def ignore_message_error(message):
                return True

            on_message_error = ignore_message_error

        if on_exception is None:

            def ignore_exception(exception):
                pass

            on_exception = ignore_exception

        while continue_polling():
            try:
                msg = self._consumer.poll(self.polling_duration)
                if msg is None:
                    # no message was delivered
                    # do some work before polling again
                    work_during_wait()
                elif msg.error():
                    logger.error("Kafka Consumer error: %s", msg.error())
                    if on_message_error(msg) is False:
                        logger.info(
                            "breaking out of polling loop after on_message_error(msg) "
                            "returned False"
                        )
                        break

                elif self._deserialize_and_process(msg) is False:
                    logger.info(
                        "breaking out of polling loop after deserialize_and_process(msg) "
                        "returned False"
                    )
                    break
                else:
                    # poll again
                    pass
            except KeyboardInterrupt as keyboard_interrupt:
                logger.exception(keyboard_interrupt)
                raise
            except Exception as exc:
                logger.exception(exc)
                on_exception(exc)

    def _deserialize_and_process(self, msg):
        """
        Deserialize the Kafka message and pass it to process_message.

        Message processing is delegated to self.process_message(message).

        This method is not intended to be redefined by subclasses.

        Parameters
        ----------
        msg : Kafka message delivered by a broker

        Returns
        -------
        continue_polling : bool
            return True to continue polling, False to break out of the polling loop
        """
        message = self._deserializer(msg.value())
        logger.debug(
            "Consumer deserialized messager with topic '%s'\n" "and message %s\n",
            msg.topic(),
            message,
        )
        continue_polling = self.process_message(msg.topic(), message)
        return continue_polling

    def process_message(self, topic, message):
        """
        Subclasses can override this method to process messages.
        Alternatively a message-processing function with signature f(topic, message)
        can be specified at init time and it will be called here.

        If this method returns False the Consumer will break out of the
        polling loop.

        Parameters
        ----------
        topic : str
            the Kafka topic of the message
        message : object
            a deserialized message object

        Returns
        -------
        continue_polling : bool
            return False to break out of the polling loop, return True to continue polling
        """
        if self._process_message is None:
            raise NotImplementedError(
                "This class must either be subclassed to override the "
                "process_message method, or have a process function passed "
                "in at init time via the process_message parameter."
            )
        else:
            # _process_message has the additional consumer argument because it
            # may not be a free function without other access to the consumer
            continue_polling = self._process_message(self._consumer, topic, message)
            return continue_polling

    def start_polling(
        self,
        continue_polling=None,
        work_during_wait=None,
        on_message_error=None,
        on_exception=None,
    ):
        """Start the polling loop.

        This method will not return until the polling loop is terminated when
        one these conditions is met:
            1. a custom continue_polling function returns False
            2. a custom on_message_error function returns False
            3. a custom process_message function returns False
            4. the process_message method defined in a subclass returns False
            5. a KeyboardInterrupt exception is raised
            6. a custom on_exception function raises an exception

        Parameters
        ----------
        continue_polling: function(), optional
            a parameter-less function called before every call to Consumer.poll,
            the intention is to allow outside logic to stop the Consumer

        work_during_wait : function(), optional
            a parameter-less function to be called between calls to Consumer.poll

        on_message_error : function(message), optional
            a one-parameter function to handle messages marked as being in an
            error state by a Kafka broker, if this function returns False the
            polling loop will terminate

        on_exception : function(exception), optional
            a one-parameter function to handle exceptions raised inside the polling loop
        """
        if self.closed:
            raise RuntimeError(
                "This Consumer has been closed. It cannot be re-opened. Create a new one."
            )
        try:
            self._poll(
                continue_polling=continue_polling,
                work_during_wait=work_during_wait,
                on_message_error=on_message_error,
                on_exception=on_exception,
            )
        finally:
            logger.info("consumer polling loop has terminated")
            self.close()

    def close(self):
        """
        Close the underlying confluent_kafka.Consumer.
        """
        self._consumer.close()
        self.closed = True
