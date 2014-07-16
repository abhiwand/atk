.. _ia_intro_1_capabilities:
   
------------
Capabilities 
------------

Easier Development
==================

The Intel Analytics Toolkit makes analytics development easier for both experts and new practitioners due to a familiar, consistent, and unified
programming environment across the workflow, from data preparation to queries, analytics, and :term:`machine learning`.
Instead of switching between various low level languages and data formatting environments of the Hadoop platform, the analytics developer uses a
convenient and easy to learn Python based environment.
Data is referenced using a familiar “data frame” and API environment that mirrors the broadly used and easy to learn Python desktop analytics
programming environment.
However, the toolkit runs all analytics functionality at full cluster scale and therefore overcomes the memory limitations of desktop
Python analytic tools.
An extensive set of libraries is provided for preparing data models, including feature engineering and data segmentation.
Deploying a unified programming approach can improve productivity by enabling easier iteration and collaboration and by reducing the learning curve
for cluster scale analytics.

Integrated Graph Processing Pipeline
====================================

Graphs represent data as a network of entities and their connections, and analytics tools for graph data bring exciting new capabilities
for finding insights in data, particularly when the connections between entities are more numerous or more important than the entities alone.
For example, graph databases enable exploring linked data no matter how large the data set, as contrasted with databases requiring table
joins to traverse connections and consequently slow exponentially as data size increases.
Graph engines enable analyzing the entire network to reveal key information about its structure, such as central influences, embedded communities,
shortest paths of connections, and to mine the data for clustering and classification uses.

The Intel Analytics Toolkit provides an environment for creating and analyzing big data graphs across the workflow, from data extraction and
wrangling, to graph construction; from query and traversal (using a graph database), through analysis and machine learning (using the graph engine.)
With a complete graph pipeline of capabilities, exploring and productively deploying graph analytics to gain valuable insights becomes possible
and practical, without the brittle “do it yourself” approaches needed today, even if just to get started exploring graph analytics
possibilities.

.. _ia_intro_1_entity_based:

Unified Graph and Entity Analytics
==================================

The toolkit brings together graph analytics and entity based machine learning into a single framework.
Today, analytics developers must utilize and manage the deployment of many different types of tools - or forgo entire classes of advanced
capabilities that hold the key to uncovering insights.
For example, extracting an entity’s nearby relationships as features into machine learning model can improve predictive accuracy.
With a unified environment, data scientists can more easily advance toward new analytic capabilities in a platform more cost effective and
easier to manage than disparate tools.
Users can leverage the full spectrum of powerful analytic capabilities without constraining their sophistication and creativity. 

Scalable Distributed Algorithms
===============================

The toolkit includes an extensive set of scalable graph and entity based algorithms to create big data solutions incorporating
:term:`clustering`, :term:`classification`, :term:`recommendation systems`, :term:`topic modeling`, :term:`community structure detection`,
and other machine learning methods.
Invocation using simple Python API calls into the programming framework reduces the need for data scientists to be algorithm experts in order to
overcome variability in data formatting, configuration parameters, capabilities, and scalability sometimes encountered when using open source algorithms.

Extensibility
=============

The toolkit is based on a modular, extensible framework.
Data scientists can incorporate custom algorithms and integrate them seamless into the development workflow.
As the Hadoop technology stack expands, new capabilities can be added to the environment without burdening data scientist productivity to master
new programming models and data formats.

Full Scalability
================

The Intel Analytics Toolkit enables developers to utilize all available data, taking full advantage of the scalable engines provided in the
Hadoop-based data platform.
Every capability – including graph :term:`transaction processing`, classical :term:`machine learning`, graph analytic algorithms, and graph construction
- scale economically by adding more standard servers to the Hadoop cluster, equipping the developer with advanced analytics on all sizes of data.
