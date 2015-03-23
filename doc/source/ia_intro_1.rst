------------
Capabilities 
------------

.. contents:: Table of Contents
    :local:

Easier Development
==================

The unified programming environment of the |IAT| makes development easier
across the entire analytics workflow — from data ingress and wrangling, to
querying, and analysis — meaning the analyst doesn't have to
spend time switching between various standalone tools.
Data is represented as "data frames" (as in R, or Big Tables), and the analyst
uses an easy-to-learn Python-based analytics environment.
The |IAT| can run analyses at full cluster scale, overcoming the memory
limitations of single-thread Python-based analytics tools.

Integrated Graph Processing Pipeline
====================================

Graphs are a natural way of associating things by common attributes, see 
`ia_intro_1_01`_; analytics
tools specifically-designed for graph data facilitate finding insights latent
in data—a problem that is computationally difficult when the data in question
is large, or the graph they form is densely connected.
The structure of graph databases allow very efficient traversal of the graph
which is much slower in relational databases.
Graph engines enable the entire data set to reveal useful statistical
information, such as centrality measures, the location of embedded communities,
and the shortest paths between connections.

.. _ia_intro_1_01: 

.. figure:: ia_intro_1_01.*

    Simple Graph

The |IAT| provides an environment for creating and analyzing big data graphs
across the workflow, from data extraction and wrangling, to graph construction;
from query and traversal, through analysis and machine learning.

.. _Unified_Graph_and_Entity_Analytics:

Unified Graph and Entity Analytics
==================================

The |IAT| brings together graph analytics and entity-based machine learning
into a single framework.
Today, analytics developers must utilize and manage the deployment of many
different types of tools, or forgo entire classes of advanced capabilities that
are key to understanding.
For example, incorporating an entity's nearby relationships into features for a
machine learning model will yield predictive models that better represent
contextual information in the data.
Having built better models, users can leverage the full spectrum of powerful
analytic capabilities without constraining their sophistication and creativity. 

Scalable Distributed Algorithms
===============================

The machine learning algorithms incorporated into the |IAT| have been designed
with scalability in mind.
Some analyses, such as topic modeling with Latent Dirichlet Allocation, can be
computationally intensive, but are parallelizable in many steps of its
procedure.
From the perspective of the user, writing parallel machine learning code can be
frustrating and complicated.
In |IAT|, we've hidden the complexity of working with parallel code behind a
set of easy-to-use Python functions, making writing parallel recommender
systems or training classifiers accessible to analysts with a range of
programming skills.

Extensibility
=============

The |IAT| has been designed with modularity in mind, from day 1.
We recognize that analysts have preferred methods and tools for getting a job
done, and that there are trade-offs in the choices to be made in any analytical
software pipeline design.
Users are able to switch out many components of the standard |IAT| deployment.
Importantly, this allows users to write their own custom algorithms
for specialized data types, such as are often found in bioinformatics or the
security sectors.

