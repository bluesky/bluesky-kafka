===============================
bluesky-kafka
===============================

.. image:: https://img.shields.io/travis/bluesky/bluesky-kafka.svg
        :target: https://travis-ci.org/bluesky/bluesky-kafka

.. image:: https://img.shields.io/pypi/v/bluesky-kafka.svg
        :target: https://pypi.python.org/pypi/bluesky-kafka


Kafka integration for bluesky.

* Free software: 3-clause BSD license

Features
--------

* BlueskyConsumer
* MongoConsumer
* Publisher
* RemoteDispatcher

Release History
---------------

v0.3.0 (2020-09-03)
...................
* added BlueskyConsumer
* added MongoConsumer
* added supervisor configuration file for mongo_normalized_consumer.py
* rewrote RemoteDispatcher to use BlueskyConsumer
* changed default serialization method to MessagePack

upcoming changes
................
* added utils.py
* added BlueskyKafkaException
* broke tests into multiple files
* simplified produce/consume tests to run in one process
* configured live logging in pytest.ini

Test
----

Install docker and docker-compose.

Start a Kafka server:

::

  $ cd bluesky-kafka/scripts
  $ sudo docker-compose -f bitnami-kafka-docker-compose.yml up

Run tests:

::

  $ cd bluesky-kafka
  $ pytest

Optionally increase logging output to the console by specifying a logging level:

::

  $ pytest --log-cli-level=DEBUG

Run a Mongo Consumer Group
--------------------------

Create a conda environment:

::

  $ conda create -n consumers python=3.8
  $ conda activate consumers

Install packages:

::

  $ pip install bluesky-kafka supervisor

Setup environment variables:
mongo_uri reference: https://docs.mongodb.com/manual/reference/connection-string/
bootstrap_servers: comma-separated list of brokers.

::

  $ export BLUESKY_MONGO_URI="mongodb://username:password@machine1:port1,machine2:port2,machine3:port3
  $ export KAFKA_BOOTSTRAP_SERVERS="machine1:9092, machine2:9092, machine3:9092"

Update the bluesky_kafka/supervisor/supervisord.conf file with the correct path for your installation.

Start the consumer processes:

::

  $ supervisord -c bluesky_kafka/supervisor/supervisord.conf

Monitor the consumer processes:

::

  $ supervisorctl -c bluesky_kafka/supervisor/supervisorctl.conf
