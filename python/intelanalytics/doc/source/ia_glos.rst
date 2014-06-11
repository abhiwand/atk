Glossary
========

Alternating Least Squares
-------------------------

The "Alternating Least Squares with Bias for collaborative filtering algorithms" is an algorithm used by the Intel Data Platform: Analytics Toolkit.
You can find out more about it here: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797 and here: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf (see equation 5).

Average Path Length
-------------------

"Average path length is a concept in network topology that is defined as the average number of steps along the shortest paths for all possible pairs of network nodes.
It is a measure of the efficiency of information or mass transport on a network."
Lifted from: http://en.wikipedia.org/wiki/Average_path_length.

Belief Propagation
------------------

See :ref:`loopy_belief_propagation`.

Beysian Network
---------------

Borrowed from Wikipedia.
"A Beysian Network is a probabilistic graphic model that represents a set of random variables and their conditional dependencies through a directed acyclic graph (DAG).
For example, a Bayesian network could represent the probabilistic relationships between diseases and symptoms.
Given symptoms, the network can be used to compute the probabilities of the presence of various diseases."
Contrast with Markov Random Fields.
See http://en.wikipedia.org/wiki/Bayesian_network.

Bias-variance tradeoff
----------------------

Lifted from: http://en.wikipedia.org/wiki/Bias_variance#Bias-variance_tradeoff.
Main article: Bias-variance dilemma (http://en.wikipedia.org/wiki/Bias-variance_dilemma).

A first issue is the tradeoff between bias and variance.
Imagine that we have available several different, but equally good, training data sets.
A learning algorithm is biased for a particular input x if, when trained on each of these data sets, it is systematically incorrect when predicting the correct output for x.
A learning algorithm has high variance for a particular input x if it predicts different output values when trained on different training sets.
The prediction error of a learned classifier is related to the sum of the bias and the variance of the learning algorithm.
Generally, there is a tradeoff between bias and variance.
A learning algorithm with low bias must be "flexible" so that it can fit the data well.
But if the learning algorithm is too flexible, it will fit each training data set differently, and hence have high variance.
A key aspect of many supervised learning methods is that they are able to adjust this tradeoff between bias and variance (either automatically or by providing a bias/variance parameter that the user can adjust).

Bias vs Variance
----------------

In this context, "bias" means accuracy, while "variance" means accounting for outlier data points.

BigColumn
---------

An identifier for a single column in a BigFrame.

Example:

>>> bf = BigFrame(...) with a column named "price"
    bf['price'] is a BigColumn
    bf.price is a BigColumn

BigGraph
--------

A graph database object with functions to manipulate the data.

BigFrame
---------

A table database object with function to manipulate the data.

Congugate Gradient Descent
--------------------------

The "Congugate Gradient Descent with Bias for Collaborative Filtering algorithm is an algorithm used by the Intel Data Platform: Analytics Toolkit.
You can find out more about it here: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf (see equation 5).

Convergence
-----------

Where a calculation (often an iterative calculation) reaches a certain value.
See http://en.wikipedia.org/wiki/Convergence_(mathematics).

.. _directed_graph:

Directed Acyclic Graph (DAG)
----------------------------

Lifted from Wikipedia.
In mathematics and computer science, a directed acyclic graph (DAG), is a directed graph with no directed cycles.
That is, it is formed by a collection of vertices and directed edges, each edge connecting one vertex to another, such that there is no way to start at some vertex v and follow a sequence of edges that eventually loops back to v again.
Contrast with :ref:`undirected_graph`.
See http://en.wikipedia.org/wiki/Directed_acyclic_graph.

.. _glossary_edge:

Edge
----

An edge is the link between two vertices in a graph database.
Edges can have direction, or be undirected.
Edges are said to have a source and a destination, usually meaning the vertex to the left and the vertex to the right.
Each edge has a label, which is the edge's unique name, and a property map.
The property map may contain 0 or more properties.
An edge can be uniquely identified from its source, destination, and label.
See :ref:`glossary_vertex` in this glossary, and: https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model.

ETL - Extract, Transform, and Load
----------------------------------

Lifted from Wikipedia.
In computing, extract, transform, and load (ETL) refers to a process in database usage and especially in data warehousing that:

Extracts data from outside sources.

Transforms it to fit operational needs, which can include quality levels.

Loads it into the end target (database, more specifically, operational data store, data mart, or data warehouse).

ETL systems are commonly used to integrate data from multiple applications, typically developed and supported by different vendors or hosted on separate computer hardware.
The disparate systems containing the original data are frequently managed and operated by different employees.
For example a cost accounting system may combine data from payroll, sales and purchasing.
See http://en.wikipedia.org/wiki/Extract,_transform,_load.

Gaussian Random Fields
----------------------

Borrowed from Wikipedia.
A Gaussian random field (GRF) is a random field involving Gaussian probability density functions of the variables.
A one-dimensional GRF is also called a Gaussian process.

One way of constructing a GRF is by assuming that the field is the sum of a large number of plane, cylindrical, or spherical waves with uniformly distributed random phase.
Where applicable, the central limit theorem dictates that at any point, the sum of these individual plane-wave contributions will exhibit a Gaussian distribution.
This type of GRF is completely described by its power spectral density, and hence, through the Wiener-Khinchin theorem, by its two-point autocorrelation function, which is related to the power spectral density through a Fourier transformation.
For details on the generation of Gaussian random fields using Matlab, see the circulant embedding method for Gaussian random field.
See http://en.wikipedia.org/wiki/Gaussian_random_field.

Graph
-----

In mathematics, and more specifically in graph theory, a graph is a representation of a set of objects where some pairs of objects are connected by links.
The interconnected objects are represented by mathematical abstractions called vertices, and the links that connect some pairs of vertices are called edges.
Typically, a graph is depicted in diagrammatic form as a set of dots for the vertices, joined by lines or curves for the edges.
Graphs are one of the objects of study in discrete mathematics.
See: http://en.wikipedia.org/wiki/Graph_(mathematics).

Graph Analytics
---------------

Graph analytics are the broad category of useful calculations you use to examine a graph.
Examples of graph analytics may include:

*traversals* -- algorithmic walk throughs of the graph to determine optimal paths and relationship between vertices,

and

*statistics* -- that determine important attributes of the graph such as degrees of separation, number of triangular counts, centralities (highly influential nodes), and so on.

Some are user guided interactions, where the user navigates through the data connections, others are algorithmic, where a result is calculated by the software.

Graph learning is a class of graph analytics applying machine learning and data mining algorithms to graph data.
This means that calculations are iterated across the nodes of the graph to uncover patterns and relationships.
Thus, finding similarities based on relationships, or recursively optimizing some parameter across nodes.

Graph Database Directions
-------------------------

As a shorthand, graph database terminology uses relative directions, assumed to be from whatever vertex you are currently using. These directions are:

| left: The calling frame's index.
| right: The input frame's index.
| outer: A union of indexes.
| inner: An intersection of indexes.

So a direction like this: "The suffix to use from the left frame's overlapping columns" means to use the suffix from the calling frame's index.

Graph Element
-------------

A graph element is an object that can have any number of key-value pairs, that is, properties, associated with it.
Each element can have zero properties as well.

Gremlin
-------

Gremlin is a graph query language, akin to SQL, that enables users to manipulate and query a graph.
Gremlin works with the Titan Graph Database, though it is made by a different company.
See https://github.com/tinkerpop/gremlin/wiki.

.. _glossary_Ising_Smoothing_Parameter:
   
Ising Smoothing Parameter
-------------------------

The smoothing parameter in the Ising model.
See: http://en.wikipedia.org/wiki/Ising_model.
You can use any positive float number.
So 3, 2.5, 1, or 0.7 are all valid values.
A larger smoothing value implies stronger relationships between adjacent random variables in the graph.

Labeled Data vs Unlabeled Data
------------------------------

Borrowed from Wikipedia.
Supervised learning algorithms are trained on labeled examples, in other words, input where the desired output is known.
While Unsupervised learning algorithms operate on unlabeled examples, in other words, input where the desired output is unknown.
See: http://en.wikipedia.org/wiki/Machine_learning#Algorithm_types.

Many machine-learning researchers have found that unlabeled data, when used in conjunction with a small amount of labeled data, can produce considerable improvement in learning accuracy.
See http://en.wikipedia.org/wiki/Semi-supervised_learning.

.. _glossary_lambda:

Lambda
------

This is the tradeoff parameter, used in Label Propagation on Gaussian Random Fields.
The regularization parameter is a control on fitting parameters.
It is used in machine learning algorithms to prevent overfitting.
As the magnitude of the fitting parameter increases, there will be an increasing penalty on the cost function.
This penalty is dependent on the squares of the parameters as well as the magnitude of lambda.
Adapted from: http://openclassroom.stanford.edu/MainFolder/DocumentPage.php?course=MachineLearning&doc=exercises/ex5/ex5.html.

Latent Dirichlet Allocation
---------------------------

Borrowed from Wikipedia.
In natural language processing, latent Dirichlet allocation (LDA) is a generative model that allows sets of observations to be explained by unobserved groups that explain why some parts of the data are similar.
For example, if observations are words collected into documents, it posits that each document is a mixture of a small number of topics and that each word's creation is attributable to one of the document's topics.
LDA is an example of a topic model and was first presented as a graphical model for topic discovery by David Blei, Andrew Ng, and Michael Jordan in 2003.
See http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation.

.. _loopy_belief_propagation:

Loopy Belief Propagation
------------------------

Belief Propagation is an algorithm that makes inferences on graph models, like a Bayesian network or Markov Random Fields.
It is called Loopy when the algorithm runs iteratively until convergence.
See http://en.wikipedia.org/wiki/Loopy_belief_propagation.

Machine Learning
----------------

Machine learning is a branch of artificial intelligence.
It is about constructing and studying software that can "learn" from data.
The more iterations the software computes, the better it gets at making that calculation.

MapReduce
---------

MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm on a cluster.

A MapReduce program is composed of a map() procedure that performs filtering and sorting (such as sorting students by first name into queues, one queue for each name) and a reduce() procedure that performs a summary operation (such as counting the number of students in each queue, yielding name frequencies).
The "MapReduce System" (also called "infrastructure" or "framework") orchestrates by marshaling the distributed servers, running the various tasks in parallel, managing all communications and data transfers between the various parts of the system, and providing for redundancy and fault tolerance.
See http://en.wikipedia.org/wiki/Map_reduce.

Markov Random Fields (MRF)
--------------------------

Markov Random fields, or Markov Network, are an undirected graph model that may be cyclic.
This contrasts with Beysian Networks, which are directed and acyclic.
See http://en.wikipedia.org/wiki/Markov_random_field. 

Page Rank
---------

The PageRank algorithm, used to rank web pages in a web search.
See: http://en.wikipedia.org/wiki/PageRank.

Property Map
------------

A property map is a key-value map.
Both edges and vertices have property maps.
See: https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model.

RDF (Resource Description Framework)
------------------------------------

The Resource Description Framework (RDF) is a family of World Wide Web Consortium (W3C) specifications originally designed as a metadata data model.
It has come to be used as a general method for conceptual description or modeling of information that is implemented in web resources, using a variety of syntax notations and data serialization formats.
See: http://en.wikipedia.org/wiki/Resource_Description_Framework.

.. _semi-supervised_learning:

Semi-Supervised Learning
------------------------

In Semi-Supervised learning algorithms, most the input data are not labeled and a small amount are labeled.
The expectation is that the software "learns" to calculate faster than in either supervised or unsupervised algorithms.
See :ref:`supervised_learning`, and :ref:`unsupervised_learning`.

Simple Random Sampling (SRS)
----------------------------

In statistics, a simple random sample is a subset of individuals (a sample) chosen from a larger set (a population).
Each individual is chosen randomly and entirely by chance, such that each individual has the same probability of being chosen at any stage during the sampling process, and each subset of *k* individuals has the same probability of being chosen for the sample as any other subset of *k* individuals.[#f1]_
This process and technique is known as simple random sampling.
A simple random sample is an unbiased surveying technique.
See https://en.wikipedia.org/wiki/Simple_random_sampling.

Smoothing
---------

Smoothing means to reduce the "noise" in a data set.
"In smoothing, the data points of a signal are modified so individual points (presumably because of noise) are reduced, and points that are lower than the adjacent points are increased leading to a smoother signal."
See http://en.wikipedia.org/wiki/Smoothing and http://en.wikipedia.org/wiki/Relaxation_(iterative_method). 

Stratified Sampling
-------------------

In statistics, stratified sampling is a method of sampling from a population.
In statistical surveys, when subpopulations within an overall population vary, it is advantageous to sample each subpopulation (stratum) independently.
Stratification is the process of dividing members of the population into homogeneous subgroups before sampling.
The strata should be mutually exclusive: every element in the population must be assigned to only one stratum.
The strata should also be collectively exhaustive: no population element can be excluded.
Then simple random sampling or systematic sampling is applied within each stratum.
This often improves the representativeness of the sample by reducing sampling error.
It can produce a weighted mean that has less variability than the arithmetic mean of a simple random sample of the population.
See https://en.wikipedia.org/wiki/Stratified_sampling.

.. _supervised_learning:

Supervised Learning
-------------------

Supervised learning refers to algorithms where the input data are all labeled, and the outcome of the calculation is known.
These algorithms train the software to make a certain calculation.
See :ref:`unsupervised_learning`, and :ref:`semi-supervised_learning`.

.. _undirected_graph:

Undirected Graph
----------------

An undirected graph is one in which the edges have no orientation (direction).
The edge (a, b) is identical to the edge (b, a), in other words, they are not ordered pairs, but sets {u, v} (or 2-multisets) of vertices.
The maximum number of edges in an undirected graph without a self-loop is n(n - 1)/2.
Contrast with :ref:`directed_graph`.
See also: http://en.wikipedia.org/wiki/Undirected_graph#Undirected_graph. 

.. _unsupervised_learning:

Unsupervised Learning
---------------------

Unsupervised learning refers to algorithms where the input data are not labeled, and the outcome of the calculation is unknown.
In this case, the software needs to "learn" how to make the calculation.
See :ref:`supervised_learning`, and :ref:`semi-supervised_learning`.

.. _glossary_vertex:

Vertex (Vertices)
-----------------

A vertex is a data point in a graph database.
Each vertex has an ID and a property map.
In Giraph, a long integer is used as ID for each vertex.
The property map may contain 0 or more properties.
Each vertex is connected to others by edges.
See :ref:`glossary_edge` in this glossary, and: https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model.

.. rubric:: Footnotes

[#f1] Yates, Daniel S.; David S. Moore, Daren S. Starnes (2008). The Practice of Statistics, 3rd Ed. Freeman. ISBN 978-0-7167-7309-2.
