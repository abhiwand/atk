Getting Started
===============

The Intel® Data Platform: Analytics Toolkit is a scalable database and analytics tool for examining and analyzing "Big Data." The Analytics Toolkit gives data scientists the tools to take raw data, clean it appropriately, build graphs, examine the relationships between data, and view the data in different ways.
The Intel Data Platform: Analytics Toolkit includes powerful algorithms for transforming your data and a built-in graph database allows you to examine your data and run analytic tools on it.
The interface to the data is through a Python programmatic interface where data scientists will be able to iteratively manage data, features, graphs,  models, results and visualizations.


Features
--------

We add features to the Analytics Toolkit frequently, so check back for updates.

You can import data from several different formats.

The Analytics Toolkit provides data cleaning tools to prepare your data by removing erroneous values, transforming value to a normalized state and constructing new features through manipulating existing values.

We provide powerful algorithms for gaining deeper insight into your data.


Built-in Databases
------------------

The Intel Data Platform: Analytics Toolkit holds data as either traditional table-based format in HBase or can store that data in the Titan graph database. The Intel Data Platform: Analytics Toolkit provides the functionality that allows data scientists to easily manipulate data in either of these data formats.


Before You Start
----------------

You should be familiar with some of the Python packages already available in the open source community.

The Intel Data Platform: Analytics Toolkit provides a set of functionality exposed through a Python API. You can access this API through iPython notebooks, so familiarizing yourself with iPython will be helpful to you. http://ipython.org/

You'll want to know about Hadoop and its various components. We use HDFS, MapReduce, and YARN.
http://hadoop.apache.org/docs/current/index.html 

We use Apache® Giraph® for graph-based machine learning.
http://giraph.apache.org/ 

We use the Titan® Graph Database from Think Aurelius because it is flexible and highly scalable.
http://thinkaurelius.github.io/titan/ 

When using Titan, you can access the database with the Gremlin® graph query language from TinkerPop. You can write Gremlin queries to investigate and manipulate your graphs.
https://github.com/tinkerpop/gremlin/wiki

The toolkit notebooks run Python 2.7 and come with several python packages installed, ready to import in your interactive sessions:

