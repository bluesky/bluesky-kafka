===============================
bluesky-kafka
===============================

.. image:: https://img.shields.io/travis/bluesky/bluesky-kafka.svg
        :target: https://travis-ci.org/bluesky/bluesky-kafka

.. image:: https://img.shields.io/pypi/v/bluesky-kafka.svg
        :target: https://pypi.python.org/pypi/bluesky-kafka


Kafka integration for bluesky.

* Free software: 3-clause BSD license
* Documentation: (COMING SOON!) https://blueskyproject.io/bluesky-kafka.

Features
--------

* A Kafka callback for bluesky.

Test
----

Install docker and docker-compose.

Start a Kafka server:

::

  $ cd bluesky-kafka/scripts
  $ sudo docker-compose -f kafka-docker-compose.yml up

Run tests:

::

  $ cd bluesky-kafka
  $ pytest
