===============
Getting Started
===============

.. contents:: Table of Contents
    :local:

----------------
Before You Start
----------------

You'll want to know about `Apache Hadoop <http://hadoop.apache.org/>`__ and its
various components.
The |IAT| uses standards and open-source routines such as |HDFS|,
:term:`MapReduce`, YARN, as well as
`Apache Giraph <http://giraph.apache.org/>`__ for graph-based machine learning
and graph analytics.
The Titan graph database can be queried using the
`Gremlin <https://github.com/tinkerpop/gremlin/wiki>`__ graph query
language from TinkerPop.

--------
Features
--------

*   Import data from several different formats
*   Data cleaning tools to prepare the data by removing erroneous values,
    transforming values to a normalized state and constructing
    new features through manipulating existing values
*   Powerful algorithms for gaining deeper insight into the data.

------------------
Built-in Databases
------------------

The |IAT| stores data as either traditional table-based format,
or in a graph database, and it provides the tools to allow to
easily manipulate data in either of these data formats.

---------------
Script Examples
---------------

The |IAT| ships with example scripts and data sets that exercise the various
features of the platform.
The default location for the example scripts is *iauser*'s home directory
'/home/iauser'.

The examples are located in '/home/iauser/examples'::

    -rwxr-xr-- 1 iauser iauser  904 Jul 30 04:20 als.py
    -rwxr-xr-- 1 iauser iauser  921 Jul 30 04:20 cgd.py
    -rwxr-xr-- 1 iauser iauser 1078 Jul 30 04:20 lbp.py
    -rwxr-xr-- 1 iauser iauser  707 Aug  7 18:21 lda.py
    -rwxr-xr-- 1 iauser iauser  930 Jul 30 04:20 lp.py
    -rwxr-xr-- 1 iauser iauser  859 Jul 30 04:20 movie_graph_5mb.py
    -rwxr-xr-- 1 iauser iauser  861 Jul 30 04:20 movie_graph_small.py
    -rwxr-xr-- 1 iauser iauser  563 Jul 30 04:20 pr.py

The datasets are located in '/home/iauser/examples/datasets' and
'hdfs://user/iauser/datasets/'::

    -rw-r--r--  ...  /user/iauser/datasets/README
    -rw-r--r--  ...  /user/iauser/datasets/apl.csv
    -rw-r--r--  ...  /user/iauser/datasets/lbp_edge.csv
    -rw-r--r--  ...  /user/iauser/datasets/lp_edge.csv
    -rw-r--r--  ...  /user/iauser/datasets/movie_sample_data_5mb.csv
    -rw-r--r--  ...  /user/iauser/datasets/movie_sample_data_small.csv
    -rw-r--r--  ...  /user/iauser/datasets/recommendation_raw_input.csv
    -rw-r--r--  ...  /user/iauser/datasets/test_lda.csv

The datasets in '/home/iauser/examples/datasets' are for reference.
The actual data that is being used by the Python examples and the |IAT| server
is in the |HDFS| system.

To get access to the scripts, login as *iauser* and go to the example scripts
directory::

    $ sudo su iauser
    $ cd /home/iauser/examples

To run any of the Python example scripts type::

    $ python <SCRIPT_NAME>

where "<SCRIPT_NAME>" is any of the scripts.

Logs
====

Details about logs can be found in the :doc:`section on log files <ad_log>`.
