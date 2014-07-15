-------------------
0.8 Preview Release 
-------------------

This section describes the capabilities of the 0.8 version of the Intel Analytics Toolkit, and in particular the graph analytics capabilities.
Other capabilities referenced in the :ref:`capabilities <ia_intro_1_capabilities>` section (like :ref:`entity based <ia_intro_1_entity_based>` analytics) will be added soon after.

Data Import, Data Cleaning, and Feature Engineering
===================================================

The Intel Analytics Toolkit provides a baseline set of capabilities for importing data into the framework, enabling data scientists to operate on
the data using the friendlier “Big Data Frame” referenced through familiar Python APIs.
All functionality is performed on the cluster, at scale, using Apache Spark.   

Functionality provided includes:

* Parsing for CSV and JSON data types.   
* Cleaning functions to remove duplicates, drop rows, filter, copy data into a new columns and concatenate data columns.
* Summary calculations for data inspection, such as counts, central tendencies, distributions, percentiles, dispersions, binning.  
* Joining of multiple data sources based on record relationships such as intersection (inner join),  union (outer join), and lookup inclusion or exclusion (left and right joins)
* Date and time functions to convert formats and simplify engineering the desired feature, such as conversion to year/month/day/hour/minute/second/etc., filtering by date, and calculating durations
* String manipulation such as regular expression matching, tokenization, query and replace, upper/lower case manipulations, splitting and trimming
* Common math including absolute value, exponentiation, logarithms, square root, rounding, integer operations (such as division, modulo, floor and ceiling), and random number generation
* Overall and "GroupBy" level aggregation and evaluation through functions like averaging, counting total or unique values, summing, finding the min and max, variance, and standard deviation, plus advanced transforms like exponentially weighted average.  
* Most functions can be applied using filters of data ranges, numeric ranges, or value lookups. 
* Custom user developed parsing and transformations (:term:`lambda functions`, row-based) using Python  

Graph Construction
==================

For graph tools (in other words, graph databases, graph analytics and machine learning, or graph visualization) data is first structured into the network
of vertices and edges.
The Intel Analytics Toolkit makes this process simpler through pre-built routines for assembling even very large data sets into graphs using cluster
computing for high throughput.
The toolkit support flexibility of graph data structures, including fully flesible property graphs with arbitrary edges and vertices that can optionally have properties assigned, as well as bipartite graphs in which a graph edge
always connects two different classes of vertices, such as connecting “items” to “purchasers”.  

To build a graph, once feature engineering has been complete, the developer assigns which features to use for vertices, which to use for edges, their respective labeling, and any associated properties.
The toolkit routine then assembles the individual records into the properly formed graph using the computing cluster for fast throughput.
In this process, duplicate edges are removed if data is incorrectly replicated, and the graph is checked for correct form, to eliminate presence of dangling (in other words, mal-connected)
edges that can prevent analytic algorithms from operating.
The final graph is bulk loaded into the Intel Analytics Toolkit’s graph database.
Additionally, existing graphs can be updated using the graph construction routines.   

Modeling Set Preparation
========================

To assist the data scientist, the Intel Analytics Toolkit provides data preparation capabilities to subset the data into modeling sets,
using built-in methods to sample graph data while preserving key structural properties of the graph, or generating a graph data set with desired
weightings of the connectivity characteristics of the graph.
Additionally, data splitting capabilities enable the data scientists to designate test, training, and validation sets.  

Graph Query and Traversal Using (Graph Database)
================================================

The Intel Analytics Toolkit includes fully scalable graph capabilities that support full flexibility including user-defined edge and vertex types.
Developers can take advantage of this powerful, scalable database to develop applications using :term:`transactional functionality`,
which includes adding new vertices and edges, sorting, searching, and traversing graph elements based on logical properties of the graph.
Additionally the toolkit provides a friendly, persistent data store for the graph analytics and machine learning processing functionality.  

Commonly used queries are simplified into Python APIs for uses such as top co-occurrences, extracting sub-graphs, and finding shortest paths.
Complex, rich queries are supported through the broadly used :term:`Gremlin` graph query language.
Queries are returned as Python objects so that they are easily incorporated into the user’s workflow.
Some of the commonly used capabilities of :term:`Gremlin` queries include navigating the graph, updating vertex properties,
adding edges, removing vertices, and other transactional manipulations.
:term:`Gremlin` simplifies graph data query through succinct expressions that chain together a series of steps and logical function such as transform,
filtering, and branch to represent very complex graph traversals similar to using SQL for programming relational databases.    

This version of the toolkit uses the Titan [#f1]_ open source property graph database to enable storing and querying graph data.
HBase provides the underlying storage back end, letting Titan focus on providing indexing and query functionality.
Unlike many graph databases, Titan on Hbase is fully scalable, accommodating very large graphs and simplifying the development of applications by
reducing the need to query multiple databases.

Graph Analytics and Machine Learning
====================================

The Intel Analytics Toolkit provides a suite of graph algorithms that make it easy to apply collaborative :term:`clustering`,
:term:`classification`, :term:`collaborative filtering`, :term:`belief propagation`, and :term:`topic modeling`,
in addition to common graph statistics.
Each is easy to invoke using the Python environment and parameters for the desired algorithm configuration.
Each algorithm also provides necessary metrics, facilitating assessment of model performance, accuracy, and configuration of the model for its
intended usage (including :term:`confusion matrices`, ROC, :term:`K-S tests`, and accuracy metrics including :term:`precision/recall`
and :term:`F-measure`).

Graph mining and machine learning algorithms include:

* :term:`Loopy Belief Propagation` (LBP): Used for classification on sparse data and sometimes for removing noise from an image. It has a wide range of applications in structured prediction, such as influence spread in social networks where there are prior noisy predictions for a large set of random entities and similarity relationships exists between them.
* :term:`Gaussian Belief Propagation` (GaBP): Similar to LBP, GaBP provides better modeling for systems where the underlying distributions are Gaussian instead of discrete variables.
* :term:`Label Propagation` (LP): Used for many classification problems where a ‘similarity measure’ between instances can be exploited for inference.  It propagates labels from labeled data to unlabeled data in a graph that encodes similarity relationships across all data points.  As an example, in social network analysis, label propagation is used to probabilistically infer data fields that are blank by analyzing data about a user’s friends, family, likes and online behavior.  
* :term:`Alternating Least Squares` (ALS): Used in collaborative filtering, such as recommender systems.
* :term:`Conjugate Gradient Descent` (CGD): Also used in recommender systems, particularly those requiring rich item and user preferences because it consumes less memory than ALS.
* :term:`Topic Modeling` using :term:`Latent Dirichlet Allocation` (LDA): Used for topic and key word extraction, text processing, natural language processing, and clustering with non-exclusive membership.

For graph statistics, algorithms provided include:

* :term:`Average path length`
* :term:`Connected component`
* :term:`Vertex degree`
* :term:`Vertex degree distribution`
* Shortest path from a vertex to all other vertices
* :term:`Centrality (PageRank)`

The graph engine utilized in this release is Apache Giraph, which has been integrated with the complete graph processing pipeline to
provide out-of-the-box usability and utility substantially enhanced over naked open source distributions.
This allows data scientists to focus on the analytics efficiency and effectiveness.
As an example, the toolkit allows easy splitting of graph data into training, validation, and testing sets of data and persisting calculated
parameters such as edge weights for later query and use.
Future releases of the toolkit will incorporate new graph engines, enabling the data scientist to easily adopt the system without resetting
their learning curve.

Visualization
=============

In the 0.8 release, graph data visualization will be accommodated by 3rd party or open source tools (like Gephi) or user written routines.

Toolkit Deployment
==================

The toolkit relies on analytics "engines" and storage capabilities provided by the Hadoop data platform.
Major platform elements utilized by the toolkit include:

* Storage: HBase and HDFS
* Distributed processing:
    * Apache Spark and Mlib: Open source engine and algorithms for machine learning and real-time scoring
    * Apache Giraph: Open source engine for graph analytics algorithm processing

Consequently, the toolkit has version dependencies on the Hadoop cluster for Spark, HBase, and
additional data platform components.
Initially, Hadoop clusters, running CDH 5.0.3 are required to support the minimum versions of the platform components.
The toolkit installer checks for the proper dependencies existing in the Hadoop platform, and any other installation dependencies for the toolkit itself.
Most of the toolkit is installed as a "head unit" that can be installed on an edge node to the cluster if it is not desired to install on a cluster node itself.
Note that in the 0.8 beta release there are some libraries (particularly Python libraries) that need to be present on the cluster processing nodes.

.. rubric:: Footnotes

.. [#f1] Aurelius (thinkaurelius.com) is the creator of the Titan open source graph database
