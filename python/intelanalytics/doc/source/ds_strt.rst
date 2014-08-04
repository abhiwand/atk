===============
Getting Started
===============

The Intel® Analytics Toolkit (IAT) is a scalable database and software pipeline for big data analytics.
The Analytics Toolkit gives data scientists the tools to take raw data, clean it appropriately, build graphs,
examine the relationships between data, and view the data in different ways.
The Analytics Toolkit includes powerful algorithms for transforming your data, and a built-in
graph database to allow you to examine your data and run analytic tools on it.
The interface to the data is through a Python programmatic interface, where data scientists are able to iteratively
manage data, features, graphs,  models, results and visualizations.

--------
Features
--------

*   Import data from several different formats
*   Data cleaning tools to prepare your data by removing erroneous values, transforming value to a normalized state and constructing
    new features through manipulating existing values
*   Powerful algorithms for gaining deeper insight into your data.

------------------
Built-in Databases
------------------

The Analytics Toolkit stores data as either traditional table-based format in HBase, or in a graph database,
and it provides the tools to allow data scientists to easily manipulate data in either of these data formats.

----------------
Before You Start
----------------

You should be familiar with some of the other Python packages already available in the open source community.
The IAT provides a set of functionality exposed through a Python API, which can be accessed through `iPython`_ * notebooks.
You'll want to know about `Apache Hadoop`_ * and its various components.
We use HDFS, MapReduce, and YARN, as well as `Apache Giraph`_ * for graph-based machine learning.
The Tital graph database can be queried using the `Gremlin`_ * graph query language from TinkerPop™.

.. rubric:: footnotes

.. [*] Other names and brands may be claimed as the property of others.

.. _iPython: http://ipython.org/
.. _Apache Hadoop: http://hadoop.apache.org/docs/current/index.html 
.. _Apache Giraph: http://giraph.apache.org/ 
.. _Gremlin: https://github.com/tinkerpop/gremlin/wiki
