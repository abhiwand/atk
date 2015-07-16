===============
Getting Started
===============

.. contents:: Table of Contents
    :local:
    :backlinks: none

----------------
Before You Start
----------------

You'll want to know about `Apache Hadoop <http://hadoop.apache.org/>`__ and its
various components.
The |TA| uses standards and open-source routines such as |HDFS|,
:term:`MapReduce`, YARN, as well as
`Apache Giraph <http://giraph.apache.org/>`__ for graph-based machine learning
and graph analytics.
The Titan graph database can be queried using the
`Gremlin <https://github.com/tinkerpop/gremlin/wiki>`__ graph query
language from TinkerPop.

--------
Features
--------

*   Import routines read and convert data from several different formats
*   Data cleaning tools prepare the data by removing erroneous values,
    transforming values to a normalized state and constructing
    new features through manipulating existing values
*   Analysis and machine learning algorithms give deeper insight into the data

------------------
Built-in Databases
------------------

The |TA| stores data in either a traditional table-based format,
or in a graph format.
It provides the tools to allow to easily manipulate data in either of these
data formats.

---------------
Script Examples
---------------

The |TA| ships with example Python scripts and data sets that exercise the
various features of the platform.
The default location for the example scripts is *taproot*'s home directory
'/home/taproot'.

The examples are located in '/home/taproot/examples'::

    -rwxr-xr-- 1 taproot taproot  904 Jul 30 04:20 als.py
    -rwxr-xr-- 1 taproot taproot  921 Jul 30 04:20 cgd.py
    -rwxr-xr-- 1 taproot taproot 1078 Jul 30 04:20 lbp.py
    -rwxr-xr-- 1 taproot taproot  707 Aug  7 18:21 lda.py
    -rwxr-xr-- 1 taproot taproot  930 Jul 30 04:20 lp.py
    -rwxr-xr-- 1 taproot taproot  859 Jul 30 04:20 movie_graph_5mb.py
    -rwxr-xr-- 1 taproot taproot  861 Jul 30 04:20 movie_graph_small.py
    -rwxr-xr-- 1 taproot taproot  563 Jul 30 04:20 pr.py

The datasets are located in '/home/taproot/examples/datasets' and
'hdfs://user/taproot/datasets/'::

    -rw-r--r--  ...  /user/taproot/datasets/README
    -rw-r--r--  ...  /user/taproot/datasets/apl.csv
    -rw-r--r--  ...  /user/taproot/datasets/lbp_edge.csv
    -rw-r--r--  ...  /user/taproot/datasets/lp_edge.csv
    -rw-r--r--  ...  /user/taproot/datasets/movie_sample_data_5mb.csv
    -rw-r--r--  ...  /user/taproot/datasets/movie_sample_data_small.csv
    -rw-r--r--  ...  /user/taproot/datasets/recommendation_raw_input.csv
    -rw-r--r--  ...  /user/taproot/datasets/test_lda.csv

The datasets in '/home/taproot/examples/datasets' are for reference.
The actual data that is being used by the Python examples and the |TA| server
is in the |HDFS| system.

To get access to the scripts, login as *taproot* and go to the example scripts
directory::

    $ sudo su taproot
    $ cd /home/taproot/examples

To run any of the Python example scripts type::

    $ python <SCRIPT_NAME>

where "<SCRIPT_NAME>" is any of the scripts.

Logs
====

Details about logs can be found in the section on :doc:`log files </ad_log>`.
