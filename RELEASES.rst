===============
Release History
===============

v0.8.0 (2021-12-16)
...................
* change ``BlueskyKafkaException`` base class from ``BaseException`` to ``Exception``

v0.7.0 (2021-12-09)
...................
* added producer configuration parameter support to functions in ``bluesky_kafka.utils.py``

v0.6.0 (2021-10-08)
...................
* added ``bluesky_kafka.tools.queue_thread.py``

v0.5.0 (2021-08-09)
...................
* added timeout parameter to ``utils.get_cluster_metadata()`` and ``utils.list_topics()``
* put release history in reverse chronological order

v0.4.0 (2021-04-09)
...................
* added ``continue_polling`` parameter to ``BlueskyConsumer.start()``
* added ``utils.py``
* added ``BlueskyKafkaException``
* split tests into multiple files
* create and clean up topics for each test
* simplified produce/consume tests to run in one process
* configured live logging in ``pytest.ini``
* switched from Travis CI to GitHub Workflow for continuous integration

v0.3.0 (2020-09-03)
...................
* added ``BlueskyConsumer``
* added ``MongoConsumer``
* added supervisor configuration file for ``mongo_normalized_consumer.py``
* rewrote ``RemoteDispatcher`` to use ``BlueskyConsumer``
* changed default serialization method to ``MessagePack``

