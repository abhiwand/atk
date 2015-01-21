------------
Capabilities 
------------

.. contents:: Table of Contents
    :local:

Easier Development
==================

.. outdated::

    The |IAT| makes analytics development easier for both experts and new practitioners due to its familiar, consistent, and unified programming environment across the workflow — from data preparation to queries, analytics, and :term:`machine learning`.
    Instead of switching between various low-level languages and data formatting environments of the Hadoop platform, the analytics developer uses a convenient and easy to learn Python based environment.
    Data is represented with a familiar “data frame”, and :abbr:`API (Application Programming Interface)` environment that mirrors the broadly-used and easy to learn Python desktop analytics programming environment.
    However, the |IAT| can run all analytics functionality at full cluster scale, overcoming the memory limitations of desktop Python analytic tools.
    An extensive set of libraries is provided for preparing data models, including feature engineering and data segmentation.
    Deploying a unified programming approach can improve productivity by enabling easier iteration and collaboration and by reducing the learning curve for cluster scale analytics.

The unified programming environment of the |IAT| makes development easier across the entire analytics workflow — from data ingress and wrangling, to querying, analysis, and visualization — meaning the analyst doesn't have to spend time switching between various standalone tools.
Data is represented as "data frames" (as in R, or Big Tables), and the analyst uses an easy-to-learn python-based analytics environment.
The |IAT| can run analyses at full cluster scale, overcoming the memory limitations of single-thread python-based analytics tools.

Integrated Graph Processing Pipeline
====================================

.. outdated::

    Graphs represent entities as a network of verticies and connections, and analytics tools for graph data bring exciting new capabilities for finding insights latent in data — particularly when the connections between entities are more numerous or more important than the entities alone.
    For example, graph databases\' enable exploring linked data no matter how large the data set, in contrast with relational databases which require table joins to traverse connections giving slowing performance as data size increases.
    Graph engines enable analyzing the entire network to reveal key information about its structure, such as central influences, embedded communities, shortest paths of connections, and mining data for clustering and :term:`classification` uses.

Graphs can represent entities as a connected network of vertices; analytics tools specifically-designed for graph data facilitate finding insights latent in data—a problem that is computationally difficult when the data in question is large, or the graph they form is densely connected.
For example, graph databases enable analysts to explore connected data without regard for the volume of information, in contrast with relational databases, which give slower performance, requiring table joins to traverse the connections in the network.
Graph engines enable the entire data set to reveal useful statistical information, such as centrality measures, the location of embedded communities, and the shortest paths between connections.

The |IAT| provides an environment for creating and analyzing big data graphs across the workflow, from data extraction and wrangling, to graph construction; from query and traversal, through analysis and machine learning.

.. outdated::

    With a complete graph pipeline of capabilities, exploring and productively deploying graph analytics to gain valuable insights becomes possible and practical, without the brittle “do it yourself” approaches needed today.

.. _Unified_Graph_and_Entity_Analytics:

Unified Graph and Entity Analytics
==================================

The |IAT| brings together graph analytics and entity-based machine learning into a single framework.
Today, analytics developers must utilize and manage the deployment of many different types of tools, or forgo entire classes of advanced capabilities that are key to understanding.
For example, incorporating an entity's nearby relationships into features for a machine learning model will yield predictive models that better represent contextual information in the data.
Having built better models, users can leverage the full spectrum of powerful analytic capabilities without constraining their sophistication and creativity. 

Scalable Distributed Algorithms
===============================

.. outdated::

    The |IAT| includes an extensive set of scalable graph-based and entity-based algorithms for creating big data solutions, incorporating maching learning methods such as :term:`clustering`, :term:`classification`, :term:`recommendation systems`, :term:`topic modeling`, :term:`community structure detection`.
    Invocation using simple Python :abbr:`API (Application Programming Interface)` calls into the programming framework reduces the need for data scientists to be algorithm experts in order to overcome variability in data formatting, configuration parameters, capabilities, and scalability sometimes encountered when using open source algorithms.

The machine learning algorithms incorporated into the |IAT| have been designed with scalability in mind.
Some analyses, such as topic modeling with Latent Dirichlet Allocation, can be computationally intensive, but are parallelizable in many steps of its procedure.
From the perspective of the user, writing parallel machine learning code can be frustrating and complicated.
In |IAT|, we've hidden the complexity of working with parallel code behind a set of easy-to-use Python functions, making writing parallel recommender systems or training classifiers accessible to analysts with a range of programming skills.

Extensibility
=============

.. outdated::

    The |IAT| is based on a modular, extensible framework.
    Data scientists can incorporate custom algorithms and integrate them seamless into the development workflow.
    As the Hadoop technology stack expands, new capabilities can be added to the environment without requiring that data scientists master new programming models and data formats.

The |IAT| has been designed with modularity in mind, from day 1.
We recognize that analysts have preferred methods and tools for getting a job done, and that there are trade-offs in the choices to be made in any analytical software pipeline design.
Users are able to switch out many components of the standard IAT deployment.
Importantly, this allows data scientists to write their own custom algorithms for specialized data types, such as are often found in bioinformatics or the security sectors.

.. outdated::

    Full Scalability
    =-==============

    The |IAT| enables developers to utilize all available data, taking full advantage of the scalable engines provided in the Hadoop-based data platform.
    Every capability – including graph :term:`transaction processing`, classical :term:`machine learning`, graph analytic algorithms, and graph construction, scale economically by adding more standard servers to the Hadoop cluster, equipping the developer with advanced analytics for data of all sizes.

