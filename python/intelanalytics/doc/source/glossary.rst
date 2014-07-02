========
Glossary
========


..  glossary::
    :sorted:

    Aggregation Functions

        Aggregation functions are mathematical functions which are computed over a single column within a specified set of rows.
        Aggregation functions supported are:

        * avg : The average or mean across the rows
        * count : The count of the rows
        * count_distinct : The count of unique rows
        * max : The largest value within the rows
        * min : The smallest or most negative value within the applicable rows
        * stdev : The standard deviation of the applicable rows, see `Wikipedia\: Standard Deviation`_
        * sum : The result of adding all the values together
        * var : The variance of the rows, see `Wikipedia\: Variance`_

    Alternating Least Squares

        The "Alternating Least Squares with Bias for collaborative filtering algorithms" is an algorithm used by the
        Intel Data Platform: Analytics Toolkit.

        For more information see:

            | `CiteSeerX\: Large-Scale Parallel Collaborative Filtering`_
            | `Factorization Meets the Neighborhood (pdf)`_ (see equation 5)

    Average Path Length

        From `Wikipedia\: Average Path Length`_:

            Average path length is a concept in network topology that is defined as the average number of steps along the
            shortest paths for all possible pairs of network nodes.
            It is a measure of the efficiency of information or mass transport on a network.

    Belief Propagation

        See :term:`Loopy Belief Propagation`.

    Beysian Network

         From `Wikipedia\: Bayesian Network`_:

            A Beysian Network is a probabilistic graphic model that represents a set of random variables and their conditional
            dependencies through a directed acyclic graph (DAG).
            For example, a Bayesian network could represent the probabilistic relationships between diseases and symptoms.
            Given symptoms, the network can be used to compute the probabilities of the presence of various diseases.

        \Contrast with :term:`Markov Random Fields`.
        

    Bias-variance tradeoff

        From `Wikipedia\: Bias-Variance Tradeoff`_:

            A first issue is the tradeoff between bias and variance. [#f2]_
            Imagine that we have available several different, but equally good, training data sets.
            A learning algorithm is biased for a particular input x if, when trained on each of these data sets,
            it is systematically incorrect when predicting the correct output for x.
            A learning algorithm has high variance for a particular input x if it predicts different output values
            when trained on different training sets.
            The prediction error of a learned classifier is related to the sum of the bias and the variance of the learning algorithm.
            Generally, there is a tradeoff between bias and variance.
            A learning algorithm with low bias must be "flexible" so that it can fit the data well.
            But if the learning algorithm is too flexible, it will fit each training data set differently, and hence have high variance.
            A key aspect of many supervised learning methods is that they are able to adjust this tradeoff between bias and variance
            (either automatically or by providing a bias/variance parameter that the user can adjust).

    Bias vs Variance

        In this context, "bias" means accuracy, while "variance" means accounting for outlier data points.

    BigColumn

        An identifier for a single column in a BigFrame.

    BigGraph

        A graph database object with functions to manipulate the data.

    BigFrame

        A table database object with function to manipulate the data.

    Congugate Gradient Descent

        The "Congugate Gradient Descent with Bias for Collaborative Filtering algorithm is an algorithm used by the Intel Data Platform:
        Analytics Toolkit.

        For more information: `Factorization Meets the Neighborhood (pdf)`_ (see equation 5).

    Convergence

        Where a calculation (often an iterative calculation) reaches a certain value.

        For more information see: `Wikipedia\: Convergence (mathematics)`_.

    Directed Acyclic Graph (DAG)

        From `Wikipedia\: Directed Acyclic Graph`_:

            In mathematics and computer science, a directed acyclic graph (DAG), is a directed graph with no directed cycles.
            That is, it is formed by a collection of vertices and directed edges, each edge connecting one vertex to another,
            such that there is no way to start at some vertex v and follow a sequence of edges that eventually loops back to v again.

        Contrast with :term:`Undirected Graph`.

    Edge

        An edge is the link between two vertices in a graph database.
        Edges can have direction, or be undirected.
        Edges are said to have a source and a destination, usually meaning the vertex to the left and the vertex to the right.
        Each edge has a label, which is the edge's unique name, and a property map.
        The property map may contain 0 or more properties.
        An edge can be uniquely identified from its source, destination, and label.

        For more information see: :term:`Vertex`, and `Tinkerpop\: Property Graph Model`_.

    ETL
    
        Extract, Transform, and Load

        From `Wikipedia\: Extract, Transform, and Load`_:

            In computing, extract, transform, and load (ETL) refers to a process in database usage and especially in data warehousing that:

        \ 
            * Extracts data from outside sources.
            * Transforms it to fit operational needs, which can include quality levels.
            * Loads it into the end target (database, more specifically, operational data store, data mart, or data warehouse).

        \ 
            ETL systems are commonly used to integrate data from multiple applications, typically developed and supported by different
            vendors or hosted on separate computer hardware.
            The disparate systems containing the original data are frequently managed and operated by different employees.
            For example a cost accounting system may combine data from payroll, sales and purchasing.


    Gaussian Random Fields

        From `Wikipedia\: Gaussian Random Fields`_:

            A Gaussian random field (GRF) is a random field involving Gaussian probability density functions of the variables.
            A one-dimensional GRF is also called a Gaussian process.

        \ 
            One way of constructing a GRF is by assuming that the field is the sum of a large number of plane, cylindrical, or
            spherical waves with uniformly distributed random phase.
            Where applicable, the central limit theorem dictates that at any point, the sum of these individual plane-wave
            contributions will exhibit a Gaussian distribution.
            This type of GRF is completely described by its power spectral density, and hence, through the Wiener-Khinchin theorem,
            by its two-point autocorrelation function, which is related to the power spectral density through a Fourier transformation.
            For details on the generation of Gaussian random fields using Matlab, see the circulant embedding method for Gaussian random field.

    Graph

        In mathematics, and more specifically in graph theory, a graph is a representation of a set of objects where some pairs
        of objects are connected by links.
        The interconnected objects are represented by mathematical abstractions called vertices, and the links that connect some
        pairs of vertices are called edges.
        Typically, a graph is depicted in diagrammatic form as a set of dots for the vertices, joined by lines or curves for the edges.
        Graphs are one of the objects of study in discrete mathematics.

        For more information see: `Wikipedia\: Graph (mathematics)`_.

    Graph Analytics

        Graph analytics are the broad category of useful calculations you use to examine a graph.
        Examples of graph analytics may include:

            traversals
                algorithmic walk throughs of the graph to determine optimal paths and relationship between vertices
            statistics
                important attributes of the graph such as degrees of separation, number of triangular counts,
                centralities (highly influential nodes), and so on

        Some are user guided interactions, where the user navigates through the data connections, others are algorithmic,
        where a result is calculated by the software.

        Graph learning is a class of graph analytics applying machine learning and data mining algorithms to graph data.
        This means that calculations are iterated across the nodes of the graph to uncover patterns and relationships.
        Thus, finding similarities based on relationships, or recursively optimizing some parameter across nodes.

    Graph Database Directions

        As a shorthand, graph database terminology uses relative directions, assumed to be from whatever vertex you are currently using.
        These directions are:

            | **left**: The calling frame's index
            | **right**: The input frame's index
            | **outer**: A union of indexes
            | **inner**: An intersection of indexes

        So a direction like this: "The suffix to use from the left frame's overlapping columns" means to use the suffix from the calling frame's index.

    Graph Element

        A graph element is an object that can have any number of key-value pairs, that is, properties, associated with it.
        Each element can have zero properties as well.

    Gremlin

        Gremlin is a graph query language, akin to SQL, that enables users to manipulate and query a graph.
        Gremlin works with the Titan Graph Database, though it is made by a different company.
        For more information see: `Gremlin Wiki`_.

    Ising Smoothing Parameter

        The smoothing parameter in the Ising model.
        For more information see: `Wikipedia\: Ising Model`_.

        You can use any positive float number.
        So 3, 2.5, 1, or 0.7 are all valid values.
        A larger smoothing value implies stronger relationships between adjacent random variables in the graph.

    Labeled Data vs Unlabeled Data

        From `Wikipedia\: Machine Learning / Algorithm Types`_:

            Supervised learning algorithms are trained on labeled examples, in other words, input where the desired output is known.
            While Unsupervised learning algorithms operate on unlabeled examples, in other words, input where the desired output is unknown.

        Many machine-learning researchers have found that unlabeled data, when used in conjunction with a small amount of labeled data,
        can produce considerable improvement in learning accuracy.

        For more information see: `Wikipedia\: Semi-Supervised Learning`_.

    Lambda

        Adapted from: `Stanford\: Machine Learning`_:

            This is the tradeoff parameter, used in Label Propagation on Gaussian Random Fields.
            The regularization parameter is a control on fitting parameters.
            It is used in machine learning algorithms to prevent overfitting.
            As the magnitude of the fitting parameter increases, there will be an increasing penalty on the cost function.
            This penalty is dependent on the squares of the parameters as well as the magnitude of lambda.

    Lambda Functions

        These are referred to in the API documentation.
        These are functions passed to other functions.
        An example of this would be adding a column to a BigFrame and telling the function responsible for the column addition
        what it should put into the new column based on data in other columns.
        A function must return the same type of data that the column definition supplies.
        For example, if a column is defined as a float within an array, the function must return the data as a float in an array.

    Latent Dirichlet Allocation

        From `Wikipedia\: Latent Dirichlet Allocation`_:

            In natural language processing, latent Dirichlet allocation (LDA) is a generative model that allows sets of
            observations to be explained by unobserved groups that explain why some parts of the data are similar.
            For example, if observations are words collected into documents, it posits that each document is a mixture of
            a small number of topics and that each word's creation is attributable to one of the document's topics.
            LDA is an example of a topic model and was first presented as a graphical model for topic discovery by
            David Blei, Andrew Ng, and Michael Jordan in 2003.

    Loopy Belief Propagation

        Belief Propagation is an algorithm that makes inferences on graph models, like a Bayesian network or Markov Random Fields.
        It is called Loopy when the algorithm runs iteratively until convergence.

        For more information see: `Wikipedia\: Belief Propagation`_.

    Machine Learning

        Machine learning is a branch of artificial intelligence.
        It is about constructing and studying software that can "learn" from data.
        The more iterations the software computes, the better it gets at making that calculation.

    MapReduce

        MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm on a cluster.
        It is composed of a map() procedure that performs filtering and sorting (such as sorting students by first name into queues,
        one queue for each name) and a reduce() procedure that performs a summary operation (such as counting the number of students
        in each queue, yielding name frequencies).
        The "MapReduce System" (also called "infrastructure" or "framework") orchestrates by marshaling the distributed servers,
        running the various tasks in parallel, managing all communications and data transfers between the various parts of the system,
        and providing for redundancy and fault tolerance.

        For more information see: `Wikipedia\: MapReduce`_.

    Markov Random Fields

        Markov Random fields, or Markov Network, are an undirected graph model that may be cyclic.
        This contrasts with Beysian Networks, which are directed and acyclic.

        For more information see: `Wikipedia\: Markov Random Field`_.

    PageRank

        The PageRank algorithm, used to rank web pages in a web search.

        For more information see: `Wikipedia\: PageRank`_.

    Property Map

        A property map is a key-value map.
        Both edges and vertices have property maps.

        For more information see: `Tinkerpop\: Property Graph Model`_.

    RDF
    
        The Resource Description Framework (RDF) is a family of World Wide Web Consortium (W3C) specifications originally
        designed as a metadata data model.
        It has come to be used as a general method for conceptual description or modeling of information that is implemented
        in web resources, using a variety of syntax notations and data serialization formats.

        For more information see: `Wikipedia\: Resource Description Framework`_.

    Semi-Supervised Learning

        In Semi-Supervised learning algorithms, most the input data are not labeled and a small amount are labeled.
        The expectation is that the software "learns" to calculate faster than in either supervised or unsupervised algorithms.

        For more information see: :term:`Supervised Learning`, and :term:`Unsupervised Learning`.

    Simple Random Sampling

        In statistics, a simple random sample (SRS) is a subset of individuals (a sample) chosen from a larger set (a population).
        Each individual is chosen randomly and entirely by chance, such that each individual has the same probability of being
        chosen at any stage during the sampling process, and each subset of *k* individuals has the same probability of being
        chosen for the sample as any other subset of *k* individuals. [#f1]_
        This process and technique is known as simple random sampling.
        A simple random sample is an unbiased surveying technique.

        For more information see: `Wikipedia\: Simple Random Sample`_.

    Smoothing

        Smoothing means to reduce the "noise" in a data set.
        "In smoothing, the data points of a signal are modified so individual points (presumably because of noise) are reduced,
        and points that are lower than the adjacent points are increased leading to a smoother signal."

        For more information see:

            | `Wikipedia\: Smoothing`_
            | `Wikipedia\: Relaxation (iterative method)`_

    Stratified Sampling

        In statistics, stratified sampling is a method of sampling from a population.
        In statistical surveys, when subpopulations within an overall population vary, it is advantageous to sample each
        subpopulation (stratum) independently.
        Stratification is the process of dividing members of the population into homogeneous subgroups before sampling.
        The strata should be mutually exclusive: every element in the population must be assigned to only one stratum.
        The strata should also be collectively exhaustive: no population element can be excluded.
        Then simple random sampling or systematic sampling is applied within each stratum.
        This often improves the representativeness of the sample by reducing sampling error.
        It can produce a weighted mean that has less variability than the arithmetic mean of a simple random sample of the population.

        For more information see: `Wikipedia\: Stratified Sampling`_.

    Supervised Learning

        Supervised learning refers to algorithms where the input data are all labeled, and the outcome of the calculation is known.
        These algorithms train the software to make a certain calculation.

        For more information see: :term:`Unsupervised Learning`, and :term:`Semi-Supervised Learning`.

    Undirected Graph

        An undirected graph is one in which the edges have no orientation (direction).
        The edge (a, b) is identical to the edge (b, a), in other words, they are not ordered pairs, but sets {u, v} (or 2-multisets) of vertices.
        The maximum number of edges in an undirected graph without a self-loop is n(n - 1)/2.

        Contrast with :term:`Directed Acyclic Graph (DAG)`.

        For more information see: `Wikipedia\: Undirected Graph`_.

    Unsupervised Learning

        Unsupervised learning refers to algorithms where the input data are not labeled, and the outcome of the calculation is unknown.
        In this case, the software needs to "learn" how to make the calculation.

        For more information see: :term:`Supervised Learning`, and :term:`Semi-Supervised Learning`.

    Vertex

        A vertex (plural: vertices) is a data point in a graph database.
        Each vertex has an ID and a property map.
        In Giraph, a long integer is used as ID for each vertex.
        The property map may contain 0 or more properties.
        Each vertex is connected to others by edges.

        For more information see: :term:`Edge`, and `Tinkerpop\: Property Graph Model`_.

.. _Wikipedia\: Standard Deviation: http://en.wikipedia.org/wiki/Standard_deviation
.. _Wikipedia\: Variance: https://en.wikipedia.org/wiki/Variance
.. _CiteSeerX\: Large-Scale Parallel Collaborative Filtering: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797
.. _Factorization Meets the Neighborhood (pdf): http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf
.. _Wikipedia\: Average Path Length: http://en.wikipedia.org/wiki/Average_path_length.
.. _Wikipedia\: Bayesian Network: http://en.wikipedia.org/wiki/Bayesian_network
.. _Wikipedia\: Bias-Variance Tradeoff: http://en.wikipedia.org/wiki/Bias_variance#Bias-variance_tradeoff
.. _Wikipedia\: Convergence (mathematics): http://en.wikipedia.org/wiki/Convergence_(mathematics)
.. _Wikipedia\: Directed Acyclic Graph: https://en.wikipedia.org/wiki/Directed_acyclic_graph
.. _Tinkerpop\: Property Graph Model: https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model
.. _Wikipedia\: Extract, Transform, and Load: http://en.wikipedia.org/wiki/Extract,_transform,_load
.. _Wikipedia\: Gaussian Random Fields: http://en.wikipedia.org/wiki/Gaussian_random_field
.. _Wikipedia\: Graph (mathematics): http://en.wikipedia.org/wiki/Graph_(mathematics)
.. _Gremlin Wiki: https://github.com/tinkerpop/gremlin/wiki
.. _Wikipedia\: Ising Model: http://en.wikipedia.org/wiki/Ising_model
.. _Wikipedia\: Machine Learning / Algorithm Types: http://en.wikipedia.org/wiki/Machine_learning#Algorithm_types
.. _Wikipedia\: Semi-Supervised Learning: http://en.wikipedia.org/wiki/Semi-supervised_learning
.. _Stanford\: Machine Learning: http://openclassroom.stanford.edu/MainFolder/DocumentPage.php?course=MachineLearning&doc=exercises/ex5/ex5.html
.. _Wikipedia\: Latent Dirichlet Allocation: http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation
.. _Wikipedia\: Belief Propagation: http://en.wikipedia.org/wiki/Loopy_belief_propagation
.. _Wikipedia\: MapReduce: http://en.wikipedia.org/wiki/Map_reduce
.. _Wikipedia\: Markov Random Field: http://en.wikipedia.org/wiki/Markov_random_field
.. _Wikipedia\: PageRank: http://en.wikipedia.org/wiki/PageRank
.. _Tinkerpop\: Property Graph Model: https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model
.. _Wikipedia\: Resource Description Framework: http://en.wikipedia.org/wiki/Resource_Description_Framework
.. _Wikipedia\: Simple Random Sample: https://en.wikipedia.org/wiki/Simple_random_sampling
.. _Wikipedia\: Smoothing: http://en.wikipedia.org/wiki/Smoothing
.. _Wikipedia\: Relaxation (iterative method): http://en.wikipedia.org/wiki/Relaxation_(iterative_method 
.. _Wikipedia\: Stratified Sampling: https://en.wikipedia.org/wiki/Stratified_sampling
.. _Wikipedia\: Undirected Graph: http://en.wikipedia.org/wiki/Undirected_graph#Undirected_graph

.. rubric:: Footnotes

.. [#f1] Yates, Daniel S.; David S. Moore, Daren S. Starnes (2008). The Practice of Statistics, 3rd Ed. Freeman. ISBN 978-0-7167-7309-2.
.. [#f2] S. Geman, E. Bienenstock, and R. Doursat (1992). Neural networks and the bias/variance dilemma. Neural Computation 4, 1–58.
