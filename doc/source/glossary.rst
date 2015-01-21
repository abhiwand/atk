========
Glossary
========


..  glossary::
    :sorted:

    ASCII

        A data type consisting of a character which be represented in the
        computer within 7 bits.

    Adjacency List

        A representation of a graph as a list.
        Each line of the list consists of a unique vertex identification, and
        a list of all of that vertex's neighboring vertices.

        Example adjacency list::

              Node   Connection List
            /------------------------/
               A         B, D
               B         A, C, D
               C         B 
               D         A, B

    Aggregation Function

        A mathematical function which is computed over a single column within
        a specified set of rows.
        Supported functions:

        * avg : The average or mean across the rows
        * count : The count of the rows
        * count_distinct : The count of unique rows
        * max : The largest value within the rows
        * min : The smallest or most negative value within the applicable rows
        * stdev : The standard deviation of the applicable rows, see
          `Wikipedia\: Standard Deviation`_
        * sum : The result of adding all the values together
        * var : The variance of the rows, see `Wikipedia\: Variance`_

    Alternating Least Squares

        A method used in some approaches to multidimensional scaling, where a
        goodness-of-fit measure for some data is minimized in a series of
        steps, each involving the application of the :term:`least squares`
        method of parameter estimation.

        See the :doc:`ALS section <ds_mlal_als>` on machine learning for an
        in-depth discussion of this method.

        .. outdated::
        
            The "Alternating Least Squares with Bias for collaborative filtering algorithms" is an algorithm used by the |IA| Toolkit.

            For more information see:

            | `CiteSeerX\: Large-Scale Parallel Collaborative Filtering`_
            | `Factorization Meets the Neighborhood (pdf)`_ (see equation 5)

    Average Path Length

        In network topology, the average number of steps along the shortest
        paths for all possible pairs of vertices.

        .. outdated::

            From `Wikipedia\: Average Path Length`_:

                Average path length is a concept in network topology that is
                defined as the average number of steps along the shortest
                paths for all possible pairs of network nodes.
                It is a measure of the efficiency of information or mass
                transport on a network.

    Belief Propagation

        See :term:`Loopy Belief Propagation`.

    Baysian Network

        A probabilistic graphical model representing the conditional
        dependencies amongst a set of random variables with a directed acyclic
        graph.
        
        .. outdated::

            From `Wikipedia\: Bayesian Network`_:

                A Baysian Network is a probabilistic graphic model that represents a set of random variables and their conditional dependencies through a directed acyclic graph (DAG).
                For example, a Bayesian network could represent the probabilistic relationships between diseases and symptoms.
                Given symptoms, the network can be used to compute the probabilities of the presence of various diseases.

            Contrast with :term:`Markov Random Fields`.
            
    Baysian Networks

        See :term:`Baysian Network`.

    Bias-variance tradeoff

        In supervised classifier training, the problem of minimizing two
        sources of prediction error: erroneous assumptions in the learning
        algorithm, and sensitivity to small details in the training data (in
        other words, over-fitting) when generalizing to a testing data set.

        .. outdated::

            From `Wikipedia\: Bias-Variance Tradeoff`_:

                A first issue is the tradeoff between bias and variance. [#f2]_
                Imagine that we have available several different, but equally good, training data sets.
                A learning algorithm is biased for a particular input x if, when trained on each of these data sets, it is systematically incorrect when predicting the correct output for x.
                A learning algorithm has high variance for a particular input x if it predicts different output values when trained on different training sets.
                The prediction error of a learned classifier is related to the sum of the bias and the variance of the learning algorithm.
                Generally, there is a tradeoff between bias and variance.
                A learning algorithm with low bias must be "flexible" so that it can fit the data well.
                But if the learning algorithm is too flexible, it will fit each training data set differently,and hence have high variance.
                A key aspect of many supervised learning methods is that they are able to adjust this tradeoff between bias and variance (either automatically or by providing a bias/variance parameter that the user can adjust).

    Bias vs Variance

        In this context, "bias" means accuracy, while "variance" means
        accounting for outlier data points.

    BigColumn

        An identifier for a single column in a Frame.

    TitanGraph

        A class object with the functionality to manipulate the data in a
        :term:`graph`.

    bool

        See :term:`Boolean`.

    Boolean

        A variable that can hold a single "True" or "False" value.
        In Python, it can also be "None" meaning that it is not defined.

    Booleans

        See :term:`Boolean`.

    bytearray

        A sequence of integers in the range 0 <= x < 256.

    Central Tendency

        A central or typical value for a probability distribution.
        It may also be called a center or location of the distribution.
        Colloquially, measures of central tendency are often called averages.

    Centrality

        From `Wikipedia\: Centrality`_:

        In graph theory and network analysis, centrality of a vertex measures
        its relative importance within a graph.
        Applications include how influential a person is within a social
        network, how important a room is within a building (space syntax), and
        how well-used a road is within an urban network.
        There are four main measures of centrality: degree, betweenness,
        closeness, and eigenvector.
        Centrality concepts were first developed in social network analysis,
        and many of the terms used to measure centrality reflect their
        sociological origin. [#f10]_

    Centrality (Katz)

        See :term:`Katz Centrality`.

    Centrality (PageRank)

        See :term:`Centrality`.

    Character-Separated Values

        A file containing tabular data (numbers and text) in plain-text form.
        The file can consist of any number of records, separated by a unique
        character.
        New line characters are ususally used for this purpose.
        Each record consists of one or more fields, separated by some unique
        character.
        Commas are usually used for this purpose.
        Tab characters are also quite common.
        
    Classification

        The process of predicting category membership for a set of
        observations based on a model learned from the known categorical
        groupings of another set of observations.

        .. outdated::

            From `Wikipedia\: Statistical Classification`_:

            In machine learning and statistics, classification is the problem of identifying to which of a set of categories (sub-populations) a new observation belongs, on the basis of a training set of data containing observations (or instances) whose category membership is known.

    Collaborative Clustering

        The unsupervised grouping of observations based on one or more
        character traits.

        .. outdated::

            From `Wikipedia\: Cluster Analysis`_:

            Cluster analysis or clustering is the task of grouping a set of objects in such a way, that objects in the same group (called a cluster) are more similar (in some sense or another) to each other than to those in other groups (clusters).
            It is a main task of exploratory data mining, and a common technique for statistical data analysis, used in many fields, including machine learning, pattern recognition, image analysis, information retrieval, and bioinformatics.

    Collaborative Filtering

        From `Wikipedia\: Collaborative Filtering`_:

        The process of filtering for information or patterns using techniques
        involving collaboration among multiple agents, viewpoints, data
        sources, etc. [#f5]_

    Community Structure Detection

        For complex networks, the process of identifying vertices that can be
        easily grouped into densely-connected sub-groupings.

        .. outdated::

            From `Wikipedia\: Community Structure`_:

            In the study of complex networks, a network is said to have community structure if the nodes of the network can be easily grouped into (potentially overlapping) sets of nodes such that each  set of nodes is densely connected internally.

    Connected Component

        In graph theory, a sub-graph in which any two vertices are
        interconnected but share no connections with other vertices in the
        sub-graph.

        .. outdated::

            From `Wikipedia\: Connected Component (Graph Theory)`_:

            In graph theory, a connected component (or just component) of an undirected graph is a subgraph
            in which any two vertices are connected to each other by paths, and which is connected to no Additional vertices in the supergraph.

    Confusion Matrices

        See :term:`Confusion Matrix`

    Confusion Matrix

        In machine learning, a table describing the performance of a
        supervised classification algorithm, in which each column corresponds
        to instances of a predicted class, while each row represents the
        instances of the true class.
        Also known as contingency table, error matrix, or misclassification
        matrix.

        .. outdated::

            From `Wikipedia\: Confusion Matrix`_:

            In the field of machine learning, a confusion matrix, also known as a contingency table or an error matrix [#f6]_ , is a specific table layout that allows visualization of the performance of an algorithm, typically a supervised learning one (in unsupervised learning it is usually called a matching matrix).
            Each column of the matrix represents the instances in a predicted class, while each row represents the instances in an actual class.
            The name stems from the fact that it makes it easy to see if the system is confusing two classes (i.e. commonly mislabeling one as another).

    Conjugate Gradient Descent

        The Congugate Gradient Descent with Bias for Collaborative Filtering
        algorithm is an algorithm used by the |IA| Toolkit.

        For more information: `Factorization Meets the Neighborhood (pdf)`_
        (see equation 5).

    Convergence

        Where a calculation (often an iterative calculation) reaches a certain
        value.

        For more information see: `Wikipedia\: Convergence (mathematics)`_.

    CSV
        See :term:`Character-Separated Values`

    dict

        See :term:`Dictionary`.

    Dictionary

        A data type composed of key/value pairs.

    Directed Acyclic Graph (DAG)

        In mathematics and computer science, a graph formed by a collection of
        vertices and directed edges, each edge connecting one vertex to
        another, such that there is no way to start at some vertex :math:`v`
        and follow a sequence of edges that eventually loops back to :math:`v`
        again.

        Contrast with :term:`Undirected Graph`.

        See `Wikipedia\: Directed Acyclic Graph`_.

    ECDF
    
        See :term:`Empirical Cumulative Distribution`

    Edge

        A connection — either directed or not — between two vertices in a
        graph.

        .. outdated::

            An edge is the link between two vertices in a graph database.
            Edges can have direction, or be undirected.
            Edges are said to have a source and a destination, usually meaning the vertex to the left and the vertex to the right.
            Each edge has a label, which is the edge's unique name, and a property map.
            The property map may contain 0 or more properties.
            An edge can be uniquely identified from its source, destination, and label.

            For more information see: :term:`Vertex`, and `Tinkerpop\: Property Graph Model`_.

    Edges

        See :term:`Edge`.

    EqualDepth

        See :term:`Equal Depth Binning`.

    EqualWidth

        See :term:`Equal Depth Binning`.

    Equal Depth Binning

        Equal width binning places column values into bins such that the
        values in each bin fall within the same interval and the interval
        width for each bin is equal.

        Equal depth binning attempts to place column values into bins such
        that each bin contains the same number of elements.

    Empirical Cumulative Distribution
    
        The :abbr:`ECDF (empirical cumulative distribution function)`
        :math:`\hat F_{n}(t)` is a step function with jumps :math:`i/n` at
        observation values, where :math:`i` is the number of tied observations
        at that value.
        Missing values are ignored.

        For observations :math:`x = (x_{1},x_{2}, ... x_{n})`,
        :math:`\hat F_{n}(t)` is the fraction of observations less than or
        equal to :math:`t`.
        
        .. math::

            \hat F_{n}(t) = \frac {x_{i} \le t}{n} = \frac {1}{n} \sum_{i=1}^{n} Indicator\{x_{i} \le t\}.

        where :math:`Indicator\{A\}` is the indicator of event :math:`A`.
        For a fixed :math:`t`, the indicator :math:`Indicator\{x_{i} \le t\}`
        is a Bernoulli random variable with parameter :math:`p = F(t)`, hence
        :math:`n \hat F_{n}(t)` is a binomial random variable with mean
        :math:`n F(t)` and variance :math:`n F(t)(1 - F(t))`.
        This implies that :math:`\hat F_{n}(t)` is an unbiased estimator for
        :math:`F(t)`.

    ETL
    
        Extract, Transform, and Load

        From `Wikipedia\: Extract, Transform, and Load`_:

            In computing, |ETL| refers to a process in database usage and
            especially in data warehousing that:

            *   Extracts data from outside sources
            *   Transforms it to fit operational needs, which can include
                quality levels
            *   Loads it into the end target (database, more specifically,
                operational data store, data mart, or data warehouse)

            |ETL| systems are commonly used to integrate data from multiple
            applications, typically developed and supported by different
            vendors or hosted on separate computer hardware.
            The disparate systems containing the original data are frequently
            managed and operated by different employees.
            For example a cost accounting system may combine data from
            payroll, sales and purchasing.


    F1 Score

        See :term:`F-Measure`.

    F-Measure
        
        In machine learning, a metric that quantifies a classifier's accuracy.
        Traditionally defined as the harmonic mean of precision and recall.
        Also known as F1 score.

        .. outdated::

            From `Wikipedia\: F1 score`_:

            In statistical analysis of binary classification, the F1 score (also F-score or F-measure) is a measure of a test's accuracy.

    F-Score

        See :term:`F-Measure`.

    float32

        A real non-integer number with 32 bits of precision as appropriate.

    float64

        A real non-integer number with 64 bits of precision as appropriate.

    frame (lower case f)

        A table database with rows and columns containing data.

    Frame (capital F)

        A class object with the functionality to manipulate the data in a
        :term:`frame`.

    GaBP

        See :term:`Gaussian Belief Propagation`.

    Gaussian Belief Propagation

        A special case of belief propagation when the underlying distributions
        are :term:`Gaussian <gaussian distribution>` (Weiss & Freeman [#f11]_).
        
        .. outdated::

            Gaussian belief propagation is a variant of the belief propagation algorithm when the underlying distributions are Gaussian.
            The first work analyzing this special model was the seminal work of Weiss and Freeman [#f11]_ .

    Gaussian Distribution
    Normal Distribution

        A group of values, where the probability of any specific value:
        
        *   will fall between two real limits,
        *   is evenly centered around the mean,
        *   approaches zero on either side of the mean.

        A Gaussian distribution is defined as:

        .. math::

            f(x,\mu,\sigma) = \frac{1}{\sigma\sqrt{2\pi}}e^{-\frac{(x-\mu)^{2}}{2\sigma^2}}

        *   :math:`\mu` is the mean of the distribution.
        *   :math:`\sigma` is the standard deviation.


    Gaussian Random Fields

        A random group of vertices displaying a :term:`Gaussian distribution`
        of one or more sets of properties.

        .. outdated::

            From `Wikipedia\: Gaussian Random Fields`_:

            A Gaussian random field (GRF) is a random field involving Gaussian probability density functions of the variables.
            A one-dimensional GRF is also called a Gaussian process.

            One way of constructing a GRF is by assuming that the field is the sum of a large number of plane, cylindrical, or spherical waves with uniformly distributed random phase.
            Where applicable, the central limit theorem dictates that at any point, the sum of these individual plane-wave contributions will exhibit a Gaussian distribution.
            This type of GRF is completely described by its power spectral density, and hence, through the Wiener-Khinchin theorem, by its two-point autocorrelation function, which is related to the power spectral density through a Fourier transformation.
            For details on the generation of Gaussian random fields using Matlab, see the circulant embedding method for Gaussian random field.

    Graph

        A representation of a set of vertices, where some pairs of objects are
        connected by edges.
        The links that connect some pairs of vertices are called edges.
        Typically, a graph is depicted in diagrammatic form as a set of dots
        for the vertices, joined by lines or curves for the edges.
        Graphs are one of the objects of study in discrete mathematics.

        For more information see: `Wikipedia\: Graph (mathematics)`_.

    Graph Analytics

        The broad category of methods used to examine the statistical and
        structural properties of a graph, including:

            traversals
                algorithmic walk throughs of the graph to determine optimal
                paths and relationship between vertices
            statistics
                important attributes of the graph such as degrees of
                separation, number of triangular counts, centralities (highly
                influential nodes), and so on

        Some are user-guided interactions, where the user navigates through
        the data connections, others are algorithmic, where a result is
        calculated by the software.

        Graph learning is a class of graph analytics applying machine learning
        and data mining algorithms to graph data.
        This means that calculations are iterated across the nodes of the
        graph to uncover patterns and relationships.
        Thus, finding similarities based on relationships, or recursively
        optimizing some parameter across nodes.

    Graph Database Directions

        As a shorthand, graph database terminology uses relative directions,
        assumed to be from whatever vertex you are currently using.
        These directions are:

            | **left**: The calling frame's index
            | **right**: The input frame's index
            | **inner**: An intersection of indexes

        So a direction like this: "The suffix to use from the left frame's
        overlapping columns" means to use the suffix from the calling frame's
        index.

    Graph Element

        A graph element is an object that can have any number of key-value
        pairs, that is, properties, associated with it.
        Each element can have zero properties as well.

    Gremlin

        A graph query language.
        Gremlin works with the Titan Graph Database, though it is made by a
        different company.
        For more information see: `Gremlin Wiki`_.

    HBase

        Apache HBase is the Hadoop database, a distributed, scalable, big data
        store.

    int32

        An integer is a member of the set of positive whole numbers {1, 2,
        3, . . . }, negative whole numbers {-1, -2, -3, . . . }, and zero {0}.
        Since a computer is limited, the computer representation of it can
        have 32 bits of precision.

    int64

        An integer is a member of the set of positive whole numbers {1, 2,
        3, . . . }, negative whole numbers {-1, -2, -3, . . . }, and zero {0}.
        Since a computer is limited, the computer representation of it can
        have 64 bits of precision.

    Ising Smoothing Parameter

        The smoothing parameter in the Ising model.
        For more information see: `Wikipedia\: Ising Model`_.

        You can use any positive float number, so 3, 2.5, 1, or 0.7 are all
        valid values.
        A larger smoothing value implies stronger relationships between
        adjacent random variables in the graph.

    Katz Centrality

        From `Wikipedia\: Katz Centrality`_:

        In Social Network Analysis (SNA) there are various measures of
        :term:`centrality` which determine the relative importance of an actor
        (or node) within the network.
        Katz centrality was introduced by Leo Katz in 1953 and is used to
        measure the degree of influence of an actor in a social network. [#f8]_
        Unlike typical centrality measures which consider only the shortest
        path (the geodesic) between a pair of actors, Katz centrality measures
        influence by taking into account the total number of walks between a
        pair of actors. [#f9]_

    |K-S| Tests

        See :term:`Kolmogorov–Smirnov Test`.

    |K-S| Test

        From `Wikipedia\: Kolmogorov–Smirnov Test`_:

        In statistics, the |K-S| test is a nonparametric test of the equality
        of continuous, one-dimensional probability distributions that can be
        used to compare a sample with a reference probability distribution
        (one-sample |K-S| test), or to compare two samples (two-sample |K-S|
        test).
        The |K-S| statistic quantifies a distance between the empirical
        distribution function of the sample and the cumulative distribution
        function of the reference distribution, or between the empirical
        distribution functions of two samples.

    Label Propagation

        A multi-pass process for grouping vertices.

        See :doc:`ds_mlal_lp`.

        .. outdated::

            Label propagation is a way of labeling things so that similar things get the same label.

            You start out with a few things that are labeled (with a "kind" or "class" marker).
            And a whole bunch of things that are unlabeled.
            The goal is compute labels for the unlabeled things so that things that are similar get the same
            label.

            Mathematically, similarity means that when you model these things as points in space, they are close.
            So, if you want to be all pretty and geometric about it, it's a way of taking a bunch of points,
            some of which are colored, and then coloring the uncolored ones,
            so that at the end the points that are close share the same color.

            Applications of this could include classifying customer profiles (or really any profile, of course),
            identifying communities of interacting agents, etc.

        For additional reference:
        `Learning from Labeled and Unlabeled Data with Label Propagation`_.

    Labeled Data vs Unlabeled Data

        From `Wikipedia\: Machine Learning / Algorithm Types`_:

            Supervised learning algorithms are trained on labeled examples, in
            other words, input where the desired output is known.
            While Unsupervised learning algorithms operate on unlabeled
            examples, in other words, input where the desired output is
            unknown.

        Many machine-learning researchers have found that unlabeled data, when
        used in conjunction with a small amount of labeled data, can produce
        considerable improvement in learning accuracy.

        For more information see: `Wikipedia\: Semi-Supervised Learning`_.

    Lambda

        Adapted from: `Stanford\: Machine Learning`_:

            This is the tradeoff parameter, used in :term:`Label Propagation`
            on :term:`Gaussian Random Fields`.
            The regularization parameter is a control on fitting parameters.
            It is used in machine learning algorithms to prevent overfitting.
            As the magnitude of the fitting parameter increases, there will be
            an increasing penalty on the cost function.
            This penalty is dependent on the squares of the parameters as well
            as the magnitude of lambda.
            
    Lambda Function

        An anonymous function or function literal in code.
        Lambda functions are used when a method requires a function as an input parameter and the function is coded directly in the method call.

        Further examples and explanations can be found at this page: :doc:`ds_apir`.

        Related term: :term:`User Functions <Python User Function>`.

        .. warning:: This term is often used where a :term:`python user function` is more accurate.
            A key distinction is that the lambda function is not referable by a name.

    Latent Dirichlet Allocation

        From `Wikipedia\: Latent Dirichlet Allocation`_:

            In natural language processing, latent Dirichlet allocation (LDA) is a generative model that allows sets of observations to be explained by unobserved groups that explain why some parts of the data are similar.
            For example, if observations are words collected into documents, it posits that each document is a mixture of a small number of topics and that each word's creation is attributable to one of the document's topics.
            LDA is an example of a topic model and was first presented as a graphical model for topic discovery by David Blei, Andrew Ng, and Michael Jordan in 2003.

    Least Squares

        A mathematical procedure for finding the best-fitting curve to a given set of points by minimizing the sum of the squares of the offsets ("the residuals") of the points from the curve.
        The sum of the squares of the offsets is used instead of the offset absolute values because this allows the residuals to be treated as a continuous differentiable quantity.
        However, because squares of the offsets are used, outlying points can have a disproportionate effect on the fit, a property which may or may not be desirable depending on the problem at hand.
        
    list

        A sequence of objects in a single-dimension array.

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
        It is composed of a map() procedure that performs filtering and sorting (such as sorting students by first name into queues, one queue for each name) and a reduce() procedure that performs a summary operation (such as counting the number of students in each queue, yielding name frequencies).
        The "MapReduce System" (also called "infrastructure" or "framework") orchestrates by marshaling the distributed servers, running the various tasks in parallel, managing all communications and data transfers between the various parts of the system, and providing for redundancy and fault tolerance.

        For more information see: `Wikipedia\: MapReduce`_.

    Markov Random Fields

        Markov Random fields, or Markov Network, are an undirected graph model that may be cyclic.
        This contrasts with :term:`Baysian Networks`, which are directed and acyclic.

        For more information see: `Wikipedia\: Markov Random Field`_.

    OLAP

        Online analytical processing.
        An approach to answering |MDA| queries swiftly.
        The term |OLAP| was created as a slight modification of the traditional database term |OLTP|.

        For more information see: `Wikipedia\: Online analytical processing`_.

    OLTP

        Online transaction processing.
        A class of information systems that facilitate and manage transaction-oriented applications.
        |OLTP| involves gathering input information, processing the information and updating existing information to reflect the gathered and processed information.

        For more information see: `Wikipedia\: Online transaction processing`_.

    PageRank

        An algorithm to measure the importance of vertices.

        PageRank works by counting the number and quality of edges to a vertex to determine a rough estimate of how important the vertex is.
        The underlying assumption is that more important vertices are likely to have more edges from other vertices.

        For more information see: `Wikipedia\: PageRank`_.

    PageRank Centrality

        See :term:`Centrality`.

    Precision/Recall

        From `Wikipedia\: Precision and Recall`_:

        In pattern recognition and information retrieval with binary classification, precision (also called positive predictive value) is the fraction of retrieved instances that are relevant, while recall (also known as sensitivity) is the fraction of relevant instances that are retrieved.
        Both precision and recall are therefore based on an understanding and measure of relevance.
        
    Property Map

        A property map is a key-value map.
        Both edges and vertices have property maps.

        For more information see: `Tinkerpop\: Property Graph Model`_.

    PUF

        See :term:`Python User Function`.

    Python User Function

        A Python User Function (PUF) is a python function written by the user on the client-side which can execute in a distributed fashion on the cluster.
        For further explanation, see :doc:`ds_apir`

        Further examples and explanations can be found at :doc:`ds_apir`.

        Related: :term:`Lambda Function`.


    Quantile

        One of the class of values of a variate which divides the members of a batch or sample into equal-sized subgroups of adjacent values or a probability distribution into distributions of equal probability.    

    Receiver Operating Characteristic
    
        From `Wikipedia\: Receiver Operating Characteristic`_:

        In signal detection theory, a receiver operating characteristic (ROC), or simply ROC curve, is a graphical plot which illustrates the performance of a binary classifier system as its discrimination threshold is varied.
        It is created by plotting the fraction of true positives out of the total actual positives (TPR = true positive rate) vs. the fraction of false positives out of the total actual negatives (FPR = false positive rate), at various threshold settings.
        TPR is also known as sensitivity or recall in machine learning.
        The FPR is also known as the fall-out and can be calculated as one minus the more well known specificity.
        The ROC curve is then the sensitivity as a function of fall-out.
        In general, if both of the probability distributions for detection and false alarm are known,
        the ROC curve can be generated by plotting the Cumulative Distribution Function (area under the probability distribution from -inf to +inf) of the detection probability in the y-axis versus the Cumulative Distribution Function of the false alarm probability in x-axis.
        
    Recommendation Systems

        From `Wikipedia\: Recommender System`_:

        Recommender systems or recommendation systems (sometimes replacing "system" with a synonym such as platform or engine) are a subclass of information filtering system that seek to predict the 'rating' or 'preference' that user would give to an item [#f3]_ [#f4]_ .


    Resource Description Framework

        A specific format for storing graphs.
        Vertices also referred to as resources, have property/value pairs describing the resource.
        A vertex is any object which can be pointed to by a URI.
        Properties are attributes of the vertex, and values are either specific values for the attribute, or the URI for another vertex.
        For example, information in a particular vertex, might include the property "Author".
        The value for the Author property could be either a string giving the name of the author, or a link to another resource describing the author. 
        Sets of properties are defined within RDF Vocabularies (or schemas).
        A vertex may include properties defined in different schemas.
        The properties within a resource description are associated with a certain schema definition using the XML namespace mechanism.

    ROC
    
        See :term:`Receiver Operating Characteristic`

    RDF

        See :term:`Resource Description Framework`

    Row Functions

        Refer to :term:`Lambda Function` and :term:`Python User Function`

    Semi-Supervised Learning

        In Semi-Supervised learning algorithms, most the input data are not labeled and a small amount are labeled.
        The expectation is that the software "learns" to calculate faster than in either supervised or unsupervised algorithms.

        For more information see: :term:`Supervised Learning`, and :term:`Unsupervised Learning`.

    Schema

        A computer structure that defines the structure of something else.

    Simple Random Sampling

        In statistics, a simple random sample (SRS) is a subset of individuals (a sample) chosen from a larger set (a population).
        Each individual is chosen randomly and entirely by chance, such that each individual has the same probability of being chosen at any stage during the sampling process, and each subset of *k* individuals has the same probability of being chosen for the sample as any other subset of *k* individuals [#f1]_.
        This process and technique is known as simple random sampling.
        A simple random sample is an unbiased surveying technique.

        For more information see: `Wikipedia\: Simple Random Sample`_.

    Smoothing

        Smoothing means to reduce the "noise" in a data set.
        "In smoothing, the data points of a signal are modified so individual points (presumably because of noise) are reduced, and points that are lower than the adjacent points are increased leading to a smoother signal."

        For more information see:

            | `Wikipedia\: Smoothing`_
            | `Wikipedia\: Relaxation (iterative method)`_

    Stratified Sampling

        In statistics, stratified sampling is a method of sampling from a population.
        In statistical surveys, when subpopulations within an overall population vary, it is advantageous to sample each subpopulation (stratum) independently.
        Stratification is the process of dividing members of the population into homogeneous subgroups before sampling.
        The strata should be mutually exclusive: every element in the population must be assigned to only one stratum.
        The strata should also be collectively exhaustive: no population element can be excluded.
        Then simple random sampling or systematic sampling is applied within each stratum.
        This often improves the representativeness of the sample by reducing sampling error.
        It can produce a weighted mean that has less variability than the arithmetic mean of a simple random sample of the population.

        For more information see: `Wikipedia\: Stratified Sampling`_.

    str

        A Python function for converting an object to type string.

        .. outdated::
            A string data type in Python using the :term:`ASCII` encoding.

    string

        A Python data type and a class for general string operations.

        .. outdated::
            A string data type in Python using the UTF-8 encoding.

    Supervised Learning

        Supervised learning refers to algorithms where the input data are all labeled, and the outcome of the calculation is known.
        These algorithms train the software to make a certain calculation.

        For more information see: :term:`Unsupervised Learning`, and :term:`Semi-Supervised Learning`.

    Topic Modeling

        Topic models provide a simple way to analyze large volumes of unlabeled text.
        A "topic" consists of a cluster of words that frequently occur together.
        Using contextual clues, topic models can connect words with similar meanings and distinguish between uses of words with multiple meanings.

    Transaction Processing

        From `Wikipedia\: Transaction Processing`_:

        In computer science, transaction processing is information processing that is divided into individual, indivisible operations, called transactions.
        Each transaction must succeed or fail as a complete unit; it cannot be only partially complete.

    Transactional Functionality

        See :term:`Transaction Processing`.

    Triangle Count

    Undirected Graph

        An undirected graph is one in which the edges have no orientation (direction).
        The edge (a, b) is identical to the edge (b, a), in other words, they are not ordered pairs, but sets {u, v} (or 2-multisets) of vertices.
        The maximum number of edges in an undirected graph without a self-loop is :math:`\dfrac{n (n - 1)}{2}`

        Contrast with :term:`Directed Acyclic Graph (DAG)`.

        For more information see: `Wikipedia\: Undirected Graph`_.

    Unicode

        A data type consisting of a string of characters designed to represent all characters in the world, a universal character set.

    Unsupervised Learning

        Unsupervised learning refers to algorithms where the input data are not labeled, and the outcome of the calculation is unknown.
        In this case, the software needs to "learn" how to make the calculation.

        For more information see: :term:`Supervised Learning`, and :term:`Semi-Supervised Learning`.

    Vertex

        A vertex is an object in a graph.
        Each vertex has an ID and a property map.
        In Giraph, a long integer is used as ID for each vertex.
        The property map may contain 0 or more properties.
        Each vertex is connected to others by edges.

        For more information see: :term:`Edge`, and `Tinkerpop\: Property Graph Model`_.

    Vertex Degree

        From `Wikipedia\: Vertex Degree`_:

        In graph theory, the degree (or valency) of a vertex of a graph is the number of edges incident to the vertex, with loops counted twice [#f7]_.
        The degree of a vertex :math:`v` is denoted :math:`\deg(v)`.
        The maximum degree of a graph :math:`G`, denoted by :math:`\Delta(G)`, and the minimum degree of a graph, denoted by :math:`\delta(G)`, are the maximum and minimum degree of its vertices.

    Vertex Degree Distribution

        From `Wikipedia\: Degree Distribution`_:

        In the study of graphs and networks, the degree of a node in a network is the number of connections it has to other nodes and the degree distribution is the probability distribution of these degrees over the whole network.

    Vertices

        See :term:`Vertex`.

.. _Wikipedia\: Standard Deviation: http://en.wikipedia.org/wiki/Standard_deviation
.. _Wikipedia\: Variance: https://en.wikipedia.org/wiki/Variance
.. _CiteSeerX\: Large-Scale Parallel Collaborative Filtering:
    http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797
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
.. _Stanford\: Machine Learning:
    http://openclassroom.stanford.edu/MainFolder/DocumentPage.php?course=MachineLearning&doc=exercises/ex5/ex5.html
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
.. _Wikipedia\: Cluster Analysis: http://en.wikipedia.org/wiki/Cluster_analysis
.. _Wikipedia\: Statistical Classification: http://en.wikipedia.org/wiki/Statistical_classification
.. _Wikipedia\: Recommender System: http://en.wikipedia.org/wiki/Recommendation_system
.. _How Computers Know What We Want — Before We Do: http://content.time.com/time/magazine/article/0,9171,1992403,00.html
.. _Wikipedia\: Topic Modeling: http://en.wikipedia.org/wiki/Topic_modeling
.. _Wikipedia\: Community Structure: http://en.wikipedia.org/wiki/Community_structure
.. _Wikipedia\: Transaction Processing: http://en.wikipedia.org/wiki/Transaction_processing
.. _Wikipedia\: Adjacency List: http://en.wikipedia.org/wiki/Edge_list
.. _Wikipedia\: Collaborative Filtering: http://en.wikipedia.org/wiki/Collaborative_filtering
.. _Wikipedia\: Confusion Matrix: http://en.wikipedia.org/wiki/Confusion_matrix
.. _Wikipedia\: Kolmogorov–Smirnov Test: http://en.wikipedia.org/wiki/K-S_Test
.. _Wikipedia\: Precision and Recall: http://en.wikipedia.org/wiki/Precision_and_recall
.. _Wikipedia\: F1 score: http://en.wikipedia.org/wiki/F-measure
.. _Wikipedia\: Connected Component (Graph Theory): http://en.wikipedia.org/wiki/Connected_component_%28graph_theory%29
.. _Wikipedia\: Vertex Degree: http://en.wikipedia.org/wiki/Vertex_degree
.. _Wikipedia\: Degree Distribution: http://en.wikipedia.org/wiki/Degree_distribution
.. _Wikipedia\: Katz Centrality: http://en.wikipedia.org/wiki/Katz_centrality
.. _Introduction to Social Network Methods: http://faculty.ucr.edu/~hanneman/nettext/
.. _Wikipedia\: Centrality: http://en.wikipedia.org/wiki/Centrality
.. _Learning from Labeled and Unlabeled Data with Label Propagation: http://lvk.cs.msu.su/~bruzz/articles/classification/zhu02learning.pdf
.. _Wikipedia\: Receiver Operating Characteristic: https://en.wikipedia.org/wiki/Receiver_operating_characteristic
.. _Wikipedia\: Online analytical processing: https://en.wikipedia.org/wiki/Online_analytical_processing
.. _Wikipedia\: Online transaction processing: https://en.wikipedia.org/wiki/Online_transaction_processing

.. rubric:: Footnotes

.. [#f1] Yates, Daniel S.; David S. Moore, Daren S. Starnes (2008). The Practice of Statistics, 3rd Ed. Freeman.
    ISBN 978-0-7167-7309-2.
.. outdated:: [#f2] S. Geman, E. Bienenstock, and R. Doursat (1992). Neural networks and the bias/variance dilemma.
    Neural Computation 4, 1–58.
.. [#f3] Francesco Ricci and Lior Rokach and Bracha Shapira (2011). Recommender Systems Handbook, pp. 1-35. Springer.
.. [#f4] Lev Grossman (2010). `How Computers Know What We Want — Before We Do`_. Time.
.. [#f5] Terveen, Loren; Hill, Will (2001). Beyond Recommender Systems: Helping People Help Each Other pp. 6.
    Addison-Wesley.
.. outdated:: [#f6] Stehman, Stephen V. (1997). Selecting and interpreting measures of thematic classification accuracy.
    Remote Sensing of Environment 62 (1): 77–89. doi:10.1016/S0034-4257(97)00083-7.
.. [#f7] Diestel, Reinhard (2005). Graph Theory (3rd ed.). Berlin, New York: Springer-Verlag. ISBN 978-3-540-26183-4.
.. [#f8] Katz, L. (1953). A New Status Index Derived from Sociometric Index. Psychometrika, 39-43.
.. [#f9] Hanneman, R. A., & Riddle, M. (2005). `Introduction to Social Network Methods`_.
.. [#f10] Newman, M.E.J. 2010. Networks: An Introduction. Oxford, UK: Oxford University Press.
.. [#f11] Weiss, Yair; Freeman, William T. (October 2001). "Correctness of Belief Propagation in Gaussian
    Graphical Models of Arbitrary Topology". Neural Computation 13 (10): 2173–2200.
    doi:10.1162/089976601750541769. PMID 11570995.

