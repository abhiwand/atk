===============
Getting Started
===============

.. contents:: Table of Contents
    :local:

The |IAT| is a scalable database and software pipeline for big data analytics.
It gives users the tools to take raw data, clean it appropriately, build graphs
and examine the relationships between data.
It includes powerful algorithms for transforming your data, and a built-in
graph database to allow you to examine your data and run analytic tools on it.
The interface to the data is through a Python programmatic interface, where
users are able to iteratively manage data, features, graphs, models and results.

----------------
Before You Start
----------------

You'll want to know about `Apache Hadoop`_ and its various components.
The |IAT| uses |HDFS|, :term:`MapReduce`, and YARN, as well as `Apache Giraph`_
for graph-based machine learning.
The Titan graph database can be queried using the `Gremlin`_ graph query
language from TinkerPop.

--------
Features
--------

*   Import data from several different formats
*   Data cleaning tools to prepare your data by removing erroneous values,
    transforming value to a normalized state and constructing
    new features through manipulating existing values
*   Powerful algorithms for gaining deeper insight into your data.

------------------
Built-in Databases
------------------

The Analytics Toolkit stores data as either traditional table-based format,
or in a graph database, and it provides the tools to allow users to
easily manipulate data in either of these data formats.

---------------
Script Examples
---------------

|IA| ships with example scripts and data sets that exercise the various
features of the platform.
The example scripts can be found in the iauser's home directory usually
``/home/iauser``.

The examples are located in ``/home/iauser/examples``::

    -rwxr-xr-- 1 iauser iauser  904 Jul 30 04:20 als.py
    -rwxr-xr-- 1 iauser iauser  921 Jul 30 04:20 cgd.py
    -rwxr-xr-- 1 iauser iauser 1078 Jul 30 04:20 lbp.py
    -rwxr-xr-- 1 iauser iauser  707 Aug  7 18:21 lda.py
    -rwxr-xr-- 1 iauser iauser  930 Jul 30 04:20 lp.py
    -rwxr-xr-- 1 iauser iauser  859 Jul 30 04:20 movie_graph_5mb.py
    -rwxr-xr-- 1 iauser iauser  861 Jul 30 04:20 movie_graph_small.py
    -rwxr-xr-- 1 iauser iauser  563 Jul 30 04:20 pr.py

The datasets are located in ``/home/iauser/examples/datasets`` and
``hdfs://user/iauser/datasets/``::

    -rw-r--r--  ...  /user/iauser/datasets/README
    -rw-r--r--  ...  /user/iauser/datasets/apl.csv
    -rw-r--r--  ...  /user/iauser/datasets/lbp_edge.csv
    -rw-r--r--  ...  /user/iauser/datasets/lp_edge.csv
    -rw-r--r--  ...  /user/iauser/datasets/movie_sample_data_5mb.csv
    -rw-r--r--  ...  /user/iauser/datasets/movie_sample_data_small.csv
    -rw-r--r--  ...  /user/iauser/datasets/recommendation_raw_input.csv
    -rw-r--r--  ...  /user/iauser/datasets/test_lda.csv

The datasets in ``/home/iauser/examples/datasets`` are for reference the actual
data that is being used by the Python examples and the intelanalytics server
is in ``hdfs://user/iauser/datasets``.

To run any of the Python example scripts type::

    python <SCRIPT_NAME>.py

where ``<SCRIPT_NAME>`` is any of the scripts in ``/home/iauser/example``

You will need to login as the iauser first with::

    sudo su iauser

Make sure you are in the examples directory first::

    cd /home/iauser/examples
    python pr.py

Logs
====

If you need to debug changes to the scripts or peak behind the curtain, the log
files are located at ``/var/log/intelanalytics/rest-server/output.log``::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log

.. rubric:: footnotes

.. [*] Other names and brands may be claimed as the property of others.

.. _iPython: http://ipython.org/
.. _Apache Hadoop: http://hadoop.apache.org/docs/current/index.html 
.. _Apache Giraph: http://giraph.apache.org/ 
.. _Gremlin: https://github.com/tinkerpop/gremlin/wiki

