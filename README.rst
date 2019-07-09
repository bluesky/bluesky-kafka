===============================
bluesky-kafka
===============================

.. image:: https://img.shields.io/travis/jklynch/bluesky-kafka.svg
        :target: https://travis-ci.org/jklynch/bluesky-kafka

.. image:: https://img.shields.io/pypi/v/bluesky-kafka.svg
        :target: https://pypi.python.org/pypi/bluesky-kafka


Kafka integration for bluesky.

* Free software: 3-clause BSD license
* Documentation: (COMING SOON!) https://jklynch.github.io/bluesky-kafka.

Features
--------

* A Kafka callback for bluesky.

Test
----

Start a Kafka server:

::

  $ cd bluesky-kafka/scripts
  $ sudo docker-compose -f kafka-docker-compose.yml up

Run tests:

::

  $ cd bluesky-kafka
  $ pytest
