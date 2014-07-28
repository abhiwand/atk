===============
Getting Started
===============

The Intel® Analytics Toolkit is a scalable database and analytics tool for examining and analyzing "Big Data."
The Analytics Toolkit gives data scientists the tools to take raw data, clean it appropriately, build graphs,
examine the relationships between data, and view the data in different ways.
The Analytics Toolkit includes powerful algorithms for transforming your data and a built-in
graph database allows you to examine your data and run analytic tools on it.
The interface to the data is through a Python programmatic interface where data scientists will be able to iteratively
manage data, features, graphs,  models, results and visualizations.


--------
Features
--------

Import data from several different formats

Data cleaning tools to prepare your data by removing erroneous values, transforming value to a normalized state and constructing new features through manipulating existing values

Powerful algorithms for gaining deeper insight into your data.


------------------
Built-in Databases
------------------

The Analytics Toolkit holds data as either traditional table-based format in HBase or can store that data in a graph database.
It provides the functionality that allows data scientists to easily manipulate data in either of these data formats.

----------------
Before You Start
----------------

The Intel® Analytics Toolkit provides a set of functionality exposed through a Python API.

You'll want to know about `Apache™ Hadoop®`_ and its various components.

We use `Apache™ Giraph©`_ for graph-based machine learning.

When using the database with the `Gremlin®`_ graph query language from TinkerPop™.
You can write Gremlin® queries to investigate and manipulate your graphs.

You should be familiar with some of the other Python packages already available in the open source community.

+---------------------+-----------------------------+--------------------------------------------+
| **Python Package**  | **Description**             | **URL**                                    |
+---------------------+-----------------------------+--------------------------------------------+
| scipy               | scientific computing        | http://www.scipy.org                       |
+---------------------+-----------------------------+--------------------------------------------+
| numpy               | numeric computing           | http://www.numpy.org                       |
+---------------------+-----------------------------+--------------------------------------------+
| sympy               | symbolic math               | http://www.sympy.org                       |
+---------------------+-----------------------------+--------------------------------------------+
| pandas              | data structures             | http://pandas.pydata.org                   |
+---------------------+-----------------------------+--------------------------------------------+
| matplotlib          | plotting                    | http://matplotlib.org                      |
+---------------------+-----------------------------+--------------------------------------------+
| nltk                | natural language toolkit    | http://www.nltk.org                        |
+---------------------+-----------------------------+--------------------------------------------+
| jinja2              | templating engine           | http://jimja.pocoo.org                     |
+---------------------+-----------------------------+--------------------------------------------+
| bulbs               | graph data support          | http://bulbflow.com/docs                   |
+---------------------+-----------------------------+--------------------------------------------+
| happybase           | HBase support               | http://happybase.readthedocs.org/en/latest |
+---------------------+-----------------------------+--------------------------------------------+
| pydoop              | Hadoop support              | http://pydoop.sourceforge.net/docs         |
+---------------------+-----------------------------+--------------------------------------------+
| mrjob               | map reduce                  | http://pythonhosted.org/mrjob              |
+---------------------+-----------------------------+--------------------------------------------+


.. _Apache™ Hadoop®: http://hadoop.apache.org/docs/current/index.html 
.. _Apache™ Giraph©: http://giraph.apache.org/ 
.. _Gremlin®: https://github.com/tinkerpop/gremlin/wiki
