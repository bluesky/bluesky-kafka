===============================
bluesky-kafka
===============================

.. image:: https://github.com/bluesky/bluesky-kafka/actions/workflows/tests.yml/badge.svg
        :target: https://github.com/bluesky/bluesky-kafka/actions

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
