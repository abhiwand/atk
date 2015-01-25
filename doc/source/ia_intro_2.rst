-------------------------
|version| Preview Release 
-------------------------

.. contents:: Table of Contents
    :local:

This section describes the capabilities of the |version| version of the |IAT| — in particular the graph analytics capabilities.
Some of the other functionality referenced in the :doc:`ia_intro_1` section (like :ref:`Unified_Graph_and_Entity_Analytics`) will be added in a future release.

Data Import, Data Cleaning, and Feature Engineering
===================================================

The |IAT| provides a baseline set of capabilities for importing data into the framework.
This enables data scientists to operate on the data using the friendlier "Big Data Frame" concept,
referenced through Python API documentation laid out in a familiar fashion.
All functionality is performed on the cluster, at scale,
using `Apache Spark <https://spark.apache.org/>`__.   

Functionality provided includes:

*   Parsing for |CSV| data format (see :ref:`Importing Data`)
*   Duplicate removal, dropping rows, data filtering, computing new columns from existing data,
    and concatenating columns (see :ref:`Clean the Data`)
*   Summary calculations for data inspection, such as counts, measures of :term:`central tendency`,
    distribution and percentile of data, dispersions, and binning (see :ref:`ds_dflw_frame_examine`)
*   Joining of multiple data sources based on record relationships, such as intersection (inner join),
    and lookup inclusion or exclusion (left and right joins) (see :ref:`Transform The Data`)
*   Overall-level and "Group By"-level aggregation, and evaluation through functions like averaging,
    counting total or unique values, summing, finding the min and max, :term:`variance <Bias-variance tradeoff>`,
    and standard deviation
*   Advanced transforms like exponentially weighted average (see :ref:`Transform The Data`)
*   Some functions can be applied using filters of data ranges, numeric ranges, or value lookups
*   Using a custom :term:`Python user function` for parsing and transformations (see :ref:`Transform The Data`)

Graph Construction
==================

.. outdated::

    To use graph tools, such as graph databases, graph analytics and :term:`machine learning`, or graph visualization, data must first be structured into a network of vertices and edges.
    The |IAT| makes this process simple, through pre-built routines for assembling data sets of all sizes into graphs, using cluster computing for high-throughput.

    The toolkit supports flexibility of graph data structures, including fully flexible graphs with arbitrary edges and vertices that can optionally have properties assigned, as well as bipartite graphs, in which a graph edge always connects two different classes of vertices, such as connecting "items" to "purchasers."

To use tools such as graph databases, graph analytics, or network visualization, data must first be represented as a network of vertices and edges — a process we refer to as graph construction.
The |IAT| makes this simple, using a distributed method for graph building that efficiently scales for large data sets.
The toolkit supports construction of a variety of graph types, including graphs having an arbitrary number of edges and vertices that can optionally have properties assigned, and bipartite graphs, in which a graph edge connects two different classes of vertices, such as connecting "items" to "purchasers."

To build a graph, developers assign which features to use for vertices, which to use for edges, their respective labeling, and any associated properties.
For further details about defining vertices and edges, see :ref:`ds_dflw_building_rules`.
The toolkit routine for populating a Titan [#f1]_ graph database then assembles the individual records into the properly-formed graph using the computing cluster for fast throughput.
For further details about building graphs, see :ref:`ds_dflw_building_a_graph`.
In this process, duplicate edges are removed if data is incorrectly replicated, and the graph is checked for correct form, to eliminate presence of mal-connected edges that can prevent analytic algorithms from operating.
The final graph is bulk-loaded into the |IA| Toolkit’s graph database.
Additionally, existing graphs can be updated using the graph construction routines.   

Modeling Set Preparation
========================

.. outdated::

    The |IAT| provides capabilities to subset the data into modeling sets using built-in methods to sample graph data while preserving key structural properties of the graph, or generating a graph data set with weighted edges.
    Additionally, data-splitting capabilities allow for designating test, training, and validation sets.  

Our tool allows users to partition data into subsets for the purposes of training and testing machine learning models.
Users can require subsetting to preserve statistical properties of the graph as a whole, so as to prevent training bias.

Graph Query and Traversal
=========================

.. outdated::

    The |IAT| includes fully-scalable graph capabilities that support full flexibility including user-defined edge and vertex types.
    Developers can take advantage of this powerful, scalable graph to develop applications using :term:`transactional functionality`, which includes adding new vertices and edges, sorting, searching, and traversing graph elements based on logical properties of the graph.
    Additionally, the toolkit provides a friendly, persistent data store for the graph analytics and machine learning processing functionality.

    Commonly-used queries are simplified into Python APIs for uses such as top co-occurrences, extracting sub-graphs, and finding shortest paths.
    Complex, rich queries are supported through the broadly-used :term:`Gremlin` graph query language.
    Queries are returned as Python objects so that they are easily incorporated into the user’s workflow.
    Some of the commonly-used capabilities of :term:`Gremlin` queries include navigating the graph, updating vertex properties, adding edges, and removing vertices.
    :term:`Gremlin` simplifies graph data query through succinct expressions that chain together a series of steps and logical functions such as transform, filtering, and branch to represent very complex graph traversals, similar to using SQL for programming relational databases.

Graph queries are easily constructed, using a Python-like syntax.
More complex query operations (e.g., updating vertex properties, or adding edges) can be executed directly with the Gremlin query language, which supports chaining together series of queries and transformations.
For both simple and complex queries, results are returned as Python objects which can be easily inspected and incorporated into an analytics workflow.

This version of the toolkit uses the Titan open source property graph database to enable storing and querying graph data.
:term:`HBase` provides the underlying storage back end, while Titan provides indexing and query functionality.
Unlike many graph databases, Titan on Hbase is fully scalable, accommodating very large graphs and simplifying the development of applications by reducing the need to query multiple databases.

Graph Analytics and Machine Learning
====================================

The |IAT| provides a suite of graph algorithms that make it easy to apply :term:`collaborative clustering`, :term:`classification`, :term:`collaborative filtering`, :term:`belief propagation`, and :term:`topic modeling`, in addition to common graph statistics.

Each algorithm provides metrics needed to assess its accuracy and the relative performance of parameter settings, including confusion matrices, receiver operating (ROC) curve, precision, recall, F-measure, and Kolmogorov-Smirnov test.
Graph mining and machine learning algorithms included in this release include the following:

.. outdated::

    Each algorithm also provides necessary metrics, facilitating assessment of model performance, accuracy, and configuration of the model for its intended usage (including :term:`Confusion Matrices`, :term:`ROC`, :term:`K-S Tests`, and accuracy metrics, including :term:`precision, recall, <precision/recall>` and :term:`F-Measure`).

    Graph mining and machine learning algorithms included in this release are:

Graphical Models
----------------
*   :term:`Loopy Belief Propagation` (LBP): For classification on sparse data and image denoising.
    It has a wide range of applications in structured prediction, such as influence spread in social networks, where there are prior noisy predictions for a large set of random entities and similarity relationships exists between them.
*   :term:`Label Propagation` (LP): Used for many classification problems where a ‘similarity measure’ between instances can be exploited for inference.
    LP propagates labels from labeled to unlabeled data in a graph that encodes similarity relationships across all data points.
    As an example, in social network analysis, label propagation can be used to probabilistically infer data fields that are blank by analyzing data about a user’s friends, family, likes, and online behavior.  

Colaborative Filtering
----------------------
*   :term:`Alternating Least Squares` (ALS): Used in collaborative filtering applications, such as recommender systems.
*   :term:`Conjugate Gradient Descent` (CGD): An optimization method used in recommender systems,
    particularly those requiring rich item and user preferences, because it consumes less memory than ALS.

Topic Modeling
--------------
*   :term:`Latent Dirichlet Allocation` (LDA): A :term:`topic modeling` algorithm used for topic and key word extraction.

For graph statistics, algorithms provided include:

*   :term:`Connected component`

The graph engine utilized in this release is Apache Giraph, which has been integrated with the complete
graph processing pipeline to provide out-of-the-box usability and substantially-enhanced features over standard open source distributions, allowing data scientists to focus on analytics, rather than on software-related overhead.
As an example, the toolkit allows easy splitting of graph data into training, validation,
and testing sets of data and persisting calculated parameters such as edge weights for later query and use.
Future releases of the toolkit will incorporate new graph engines, enabling the data scientist to easily adopt the system.

*   :term:`PageRank`
*   :term:`Triangle Count`

Visualization
=============

In the |version| release, graph data visualization will be accommodated by 3rd party or open source tools
(like Gephi) or user-written routines.

Toolkit Deployment
==================

The toolkit relies on analytics "engines" and storage capabilities provided by the Hadoop data platform.
Major platform elements utilized by the toolkit include:

*   Storage: HBase
*   Distributed processing
    *   Apache Spark and Mlib: Open source engine and algorithms for machine learning and real-time scoring
    *   Apache Giraph: Open source engine for graph analytics algorithm processing

Consequently, the toolkit has version dependencies on the Hadoop cluster for Spark, HBase, and
additional data platform components.
See the administrative secton :doc:`ad_inst_ia1` for specific version dependencies.
The toolkit is installed mainly as a "head unit" on a machine within or adjacent to the cluster. As of beta release |version|, there are some libraries (e.g., Python packages) that need to be installed on the server nodes as well.

.. outdated::

    Most of the toolkit is installed as a "head unit" that can be installed on an edge node to the cluster if it is not desired to install on a cluster node itself.
    Note that in the |version| beta release there are some libraries (particularly Python libraries) that need to be present on the server nodes.


.. [#f1] Aurelius (thinkaurelius.com) is the creator of the Titan open source graph database

.. TODO::
    Functionality provided includes:

    Files:
    Parsing for CSV: CsvFile
    Parsing for JSON: ------------  (PUF)

    Frame:
    Duplicate removal: drop_duplicates
    dropping rows: drop_rows
    data filtering: filter
    copying data into new columns: add_columns, group_by
    concatenating columns: join
    Summary calculations: group_by, accuracy, cumulative_sum
    data inspection, such as counts: group_by, cumulative count
    measures of central tendency: -----------   (Michael?)
    distribution of data: ecdf
    percentile of data: cumulative_percent_sum, cumulative_percent_count
    dispersions: ------------  (Michael?)
    binning: bin_column
    Joining of multiple data sources: join
    Date and time functions: --------------  (PUF)
    String manipulation: --------------  (PUF)
    Common math and calculations: ----------------  (PUF)
    Overall-level and "Group By": group_by

    Graph:
    data must first be structured into a network of vertices and edges: vertex_rule, edge_rule
    To build a graph, the developer assigns which features to use for vertices: vertex_rule
    which to use for edges: edge_rule
    their respective labeling, and any associated properties: vertex_rule, edge_rule
    routine then assembles the graph: graph
    capabilities to subset the data into modeling sets:   assign_sample
    sample graph data while preserving key structural properties of the graph: ------------- (sample.vertex_sample)
    generating a graph data set with weighted edges: --------------  edge_rule (specify the weight as an edge property)
    data-splitting capabilities: assign_sample
    transactional functionality: ------------ (We can do read queries using Gremlin, but we do not support transactional writes)
    adding new vertices and edges: -------------  (graph.append)
    sorting: ----------- (Not supported)
    searching: ----------  (query.gremlin)
    traversing graph elements based on logical properties of the graph: -----------  (query.grelim)
    finding shortest paths: (Not supported yet)
    Gremlin graph query language: ----------- (query.gremlin)
    use Titan to enable storing: --------- (default right now)
    use Titan to query graph data: ---------  (default right now)
    appl  Topic Modeling using Latent Dirichlet Allocation: ---------  (graph.ml.latent_dirichelet_allocation)
    Average path length: (Not supported)
    Connected component: graph.ml.connected_components
    Vertex degree: ---------  (query.gremlin - Get count of outgoing edges using Gremlin)
    Vertex degree distribution: ---------  (Not yet exposed in Python ... used internally)
    Shortest path from a vertex to all other vertices: Not yet supported
    Centrality / PageRank: ---------  graph.ml.page_rank

