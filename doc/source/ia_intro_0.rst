---------
The |IAT|
---------

.. contents:: Table of Contents
    :local:

Overview
========
The |IAT| is a platform that simplifies applying :term:`graph analytics` and
:term:`machine learning` to big data.
The |IAT| assists in creating superior predictive models across a wide variety
of use cases and solutions.
The |IAT| combines :term:`feature engineering`, :term:`graph construction`,
:term:`graph analytics`, and :term:`machine learning` capabilities using an
extensible, modular framework.
All functionality operate at full scale, yet are invoked through a Python
programming environment that deals with the complexity of cluster computing and
parallel processing for the researcher.
The platform is fully extensible with a *plugin* architecture
to incorporate the full range of analytics and machine learning for any
solution need.
By unifying graph and entity-based machine learning, researchers can
incorporate "entitys nearby" relationships into a single workflow to yield
superior predictive models that better represent the contextual information in
the data, all with a unified workflow that frees the researchers from the
overhead of studying, integrating, and inefficiently iterating across a
diversity of formats and interfaces.

Python Interface
================
The |IAT| utilizes Python data science abstractions to make programming big
data analytic workflows using `Spark <https://spark.apache.org/>`__/`Hadoop
<https://hadoop.apache.org/>`__ clusters as familiar and accessible
as using popular desktop machine learning solutions such as Pandas and SciKit
Learn.
The scalable data frame representation is more familiar and intuitive to data
researchers compared to low level HDFS file and Spark RDD formats.
The |IAT| is a sophisticated programming library provided for data
investigation and analysis.
User-defined transformations and filters can be written in Python and applied
to terabytes (and more) of data using distributed processing.
Machine learning algorithms are invoked through higher-level data
science |API| abstractions, making writing parallel recommender systems
and training classifiers or clustering models accessible to a
broad population of researchers with existing mainstream data science
programming skills.
For more information, see the sections on :doc:`process flow <ds_dflw>`
and the :ref:`Python website <http://www.python.org>`.

Graph Pipeline
==============
Beyond the entity-based data representations and algorithms, a full graph
pipeline provides capabilities to apply graph methods to big data.
Graph representations are broadly useful, especially to link disparate data and
analyze the connections for powerful signals.
Such ensigns are easily lost using entity-based methods that function
strictly by reiteratively sampling the data.
Working with graph representations can often be more intuitive and
computationally efficient for data sets where the connections between data
points are more numerous and more important than the data points alone.
The |IAT| provides the representation of data sets as fully-scalable
property graph objects with vertices, edges, and optionally associated
properties.
The |IAT| programming library embodies the full capabilities to create and
analyze graph objects, including engineering features, linking data, performing rich
traveresal queries, and applying graph-based algorithms.
Because a researcher may need to analyze data using both its graph and
frame representations (for example, applying a clustering algorithm to a vertex
list), the |IAT| provides the seamless ability to move between both data
representations.

Graph Analytics
===============
Fully-scalable graph analytic algorithms are provided for uncovering central
influences and communities in the data set.
This ability is useful for exploring the data as well as engineering new
features based upon the relative position of an entity in the graph, thus
creating better, more predictive, machine learning behavior.

Machine Learning
================
Additional graphical machine learning algorithms, such as loopy belief
propagation, exploit the connections in the graph structure, providing powerful
new semi-supervised methods of discovering meaningful data communities.
See the section on :doc:`machine learning <ds_ml>` for further information.

Plugins
=======
In addition to the extensive set of capabilities provided, the platform is
fully extensible using a *plugin* architecture.
This allows developers to incorporate graph analytical tools into the
existing range of machine learning abilities, expanding the capabilities of the
|IAT| for new problem solutions.
Plugins are developed using a thin Scala wrapper, and the |IAT| framework
automatically generates the Python presentation of those added functions.
This can be used for a range of purposes, from developing custom algorithms for
specialized data types, building custom transformations for commonly used
functions that may be higher performance than a Python created UDF, or even
integrating a number of tools together to further consolidate the workflow.  
See the section on :doc:`Writing Plugins <ds_plugins>` for more information.

Installation
============
The |IAT| installs as an edge node on a Hadoop/Spark cluster and makes use of
a number of engines provided by the distribution.
This version of the |IAT| supports installation onto the Cloudera
distribution to ensure compatibility with the underlying Spark and Hadoop
engines.
This also allows the use of the Cloudera Manager to install the necessary
parcels and to provide the metadata store.
See the section on :doc:`installation` for more information.

