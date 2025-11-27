===============================
bluesky-kafka
===============================

A fork of bluesky's Kafka integration (https://github.com/bluesky/bluesky-kafka/)

The original codebase has been archived so we maintaining our own version as we use it in a lot of
services at the Australian Synchrotron.

For now we are only publishing packages internally.

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



Remote Best Effort Callback
---------------------------

To run `bluesky.callbacks.best_effort.BestEffortCallback` consuming from a
Kafka topic use:

::

   import matplotlib.pyplot as plt

   from bluesky.callbacks.best_effort import BestEffortCallback
   from bluesky_kafka import RemoteDispatcher

   bec = BestEffortCallback

   kafka_config = {...}

   kafka_consumer = RemoteDispatcher(**kafka_config)
   kafka_consumer.subscribe(bec)
   kafka_consumer.start(work_during_wait=lambda: plt.pause(0.05))
