===========================
Machine Learning Algorithms
===========================


In this release of the Analytics Toolkit, we support eight graphical algorithms in iGiraph.
From a functionality point of view, they fall into these categories: *Collaborative Filtering*, *Graph Analytics*, *Graphical Models*, and *Topic Modeling*.

* :ref:`Collaborative_Filtering`

    * :ref:`ALS`
    * :ref:`CGD`

* :ref:`Graph_Analytics`

    * :ref:`PR`

.. TODO::
    * :ref:`APL`
    * :ref:`CC`

* :ref:`Graphical_Models`

    * :ref:`LP`
    * :ref:`LBP`

* :ref:`Topic_Modeling`
    * :ref:`LDA`


.. _Collaborative_Filtering:

-----------------------
Collaborative Filtering
-----------------------

Collaborative Filtering is widely used in recommender systems.
For more information see: `Wikipedia\: Collaborative Filtering`_.

We support two methods in this category, :ref:`ALS` and :ref:`CGD`

.. _ALS:

:term:`Alternating Least Squares` (ALS)
=======================================

We use the :term:`Alternating Least Squares` with Bias for collaborative filtering algorithms.
For more information see: `Columbia Data Science\: Blog Week-7`_.

Our implementation is based on the following papers:
    Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan. `Large-Scale Parallel Collaborative Filtering for the Netflix Prize`_. 2008.
    Y. Koren. `Factorization Meets the Neighborhood\: a Multifaceted Collaborative Filtering Model`_. In ACM KDD 2008. (Equation 5)

This algorithm for collaborative filtering is widely used in recommendation systems to suggest items
(products, movies, articles, and so on) to potential users based on historical records of items that
all users have purchased, rated, or viewed.
The records are usually organized as a preference matrix P, which is a sparse matrix holding the preferences
(such as, ratings) given by users to items.
Within collaborative filtering approaches, ALS falls in the category of the matrix factorization/latent
factor model that infers user profiles and item profiles in low-dimension space, such that the original
matrix P can be approximated by a linear model.


The ALS Model
-------------

A typical representation of the preference matrix P in Giraph is a bipartite graph, where nodes at the
left side represent a list of users and nodes at the right side represent a set of items (such as, movies),
and edges encode the rating a user provided to an item.
To support training, validation, and test, a common practice in machine learning, each edge is also annotated by "TR", "VA" or "TE".

#..  image::
#    ds_mlal_als_1.png

After executing ALS on the input bipartite graph, each node in the graph will be associated with a
vector (f_* ) ? of length k, where k is the feature dimension is specified by the user, and a bias term b_*.
ALS optimizes (f_* ) ?  and b_* alternatively between user profiles and item profiles such that the following l2 regularized cost function is minimized:

#..  image::
#    ds_mlal_als_2.png

Here the first term strives to find (f_* ) ?'s and b_*'s that fit the given ratings, and the second term (l2 regularization) tries to avoid overfitting by penalizing the magnitudes of the parameters, and ? is a tradeoff parameter that balances the two terms and is usually determined by cross validation (CV).

#..  image:: ds_mlal_als_3.png
#    :height: 1 cm

After the parameters (f_* ) ? and b_* are determined, given an item mj the rating from user ui can be predicted by a simple linear model:

ALS Example Usage
-----------------

Input Data Format
~~~~~~~~~~~~~~~~~

The ALS algorithm takes an input data represented in CSV, JSON or XML format.
We use a CSV file as an example.
Each CSV file consists of at least five columns as shown in the example below.
The user column is a list of user IDs.
The movie column is a list of movie IDs.
The rating column records how the user rates the movie in each row.
The vertex_type labels the type of the source :term:`vertex` in each row.
It labels which nodes will be on the "left-side" and which nodes will be on the "right-side" in the bi-partite graph we are building.
The splits column specifies this row of data is for train, validation, or test.
We used TR, VA, TE for these three types of splits, respectively.

Data Import
~~~~~~~~~~~

To import the ALS input data, use the following iPython calls:

>>> from intelanalytics.table.bigdataframe import get_frame_builder
>>> fb = get_frame_builder()
>>> csvfile = '/user/hadoop/recommendation_raw_input.csv'
>>> frame = fb.build_from_csv('AlsFrame',
...                           csvfile,
...                           schema='/user:long,vertex_type:chararray,movie:long,rating:logn.splits:chararray',
...                           overwrite=True)

The example above loads the ALS input data from a CSV file.
The first line imports the needed python modules.
The second line gets the frame builder into the fb object.
The third line specifies the path to the input file.
The rest of the lines import the input data.
Here is a detailed description of the "build_from_csv" method.

The first argument is the name you want to give to the frame.
We used "AlsFrame" in this example.

The second argument specifies that this is a csv file.

The third argument is the schema of the input data.
You need to name each column, and specify the data type of each column in your input CSV input data.

The fourth argument is whether to overwrite the frame if you have imported data to the "AlsFrame" before.

Graph Construction
~~~~~~~~~~~~~~~~~~

After you import the raw data, you register which fields to use for source vertex, which fields to use for target vertex, and then construct a graph from your input data.

>>> from intelanalytics.graph.giggraph import get_graph_builder, GraphTypes
>>> gb = get_graph_builder(GraphTypes.Property, frame)
>>> gb.register_vertex('user', ['vertex_type'])
>>> gb.register_vertex('movie')
>>> gb.register_edge(('user', 'movie', 'rates'), ['splits', 'rating'])
>>> graph = gb.build("AlsGraph", overwrite=True)

In the example above, the first two lines import python modules related to graph construction, and get the graph builder object into gb.
The third to fifth lines register the graph.
Line three registers user column as the source vertex and registers the vertex property vertex_type to this vertex.
Line four registers movie column as the target vertex.
The fifth line registers an edge from user to movie, with the label rates.
Additionally, rating and splits are two edge properties registered for this algorithm.
Finally, line 6 builds a graph named AlsGraph based on the input data and graph registration.
The overwrite option overwrites a pre-existing graph with the same name.

Run ALS Algorithm
~~~~~~~~~~~~~~~~~

After graph construction, run the ALS algorithm as follows:

>>> report1 = graph.ml.als(
...             input_edge_property_list="rating",

In the example above, the first line calls to the algorithm.
The second line specifies which edge property you want to use for the ALS algorithm.
Line three specifies which edge label you want to use for this algorithm.
Line four specifies the property name for the vertex type, here we use vertex_type.
Line five specifies the property name for edge type, in this case, splits.
Line six specifies that at the most we want to run 20 super steps for this algorithm.
Line seven configures three feature dimensions for ALS.
Line eight sets the convergence threshold to 0.
Line nine sets als_lamda to 0.065.
Line ten specifies to output learning at each iteration.
Line eleven turns bias calculation on.
Line twelve specifies which vertex property names to use for ALS results.
Because we configured three feature dimensions: als_p0, als_p1, als_p2, the algorithm will store the results for feature dimension 0, 1, and 2 respectively.
Because bias term update is on, als_bias will store the bias term result.

Depending on your use case, you may want to save your ALS results in one vertex property with a vector value for each vertex, and not in separate vertex properties.
We also support this scenario, if you want to do it that way.
The example below shows how to use this feature.

The first eleven lines are the same as the previous example.
The difference is at Line twelve and Line thirteen.
Line twelve enables using a vector as a vertex property value.
Line thirteen specifies the property name to use to save the ALS results.
In this case, the result will be stored in als_results in a comma separated list.
The bias result will be stored in als_bias.

The code looks like this:

>>> Required Parameters:
>>> input_edge_property_list : List (comma-separated list of strings)
        The edge properties which contain the input edge 
        values. If you use more than one edge property, we expect a 
        comma-separated string list.
>>> input_edge_label : String
        The edge property which contains the edge label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex 
        values. If you use more than one vertex property, we expect a 
        comma-separated string list.
>>> vertex_type : String
        The vertex property which contains the vertex type.
>>> edge_type : String
        The edge property which contains edge type.
>>> num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
        Use on the fly when needed for your data sets.
>>> mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts.
        Use on the fly when needed for your data sets.
>>> vector_value : String, optional
        "True" means the algorithm supports a vector as a vertex value.
        "False" means the algorithm does not support a vector as a vertex value.
>>> num_worker : String, optional
        The number of Giraph workers.
        The default value is 15.
>>> max_supersteps : String, optional
        The number of super steps to run in Giraph.
        The default value is 10.
>>> feature_dimension : String, optional
        The feature dimension.
        The default value is 3.
>>> als_lambda : String, optional
        The regularization parameter:
        f = L2_error + lambda*Tikhonov_regularization
        The default value is 0.065.
>>> convergence_threshold : String, optional
        The convergence threshold which controls how small the change in 
        validation error must be in order to meet the convergence criteria.
        The default value is 0.
>>> learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each ALS iteration is composed of 2 super steps, the default 
        one iteration means two super steps.
>>> max_val : String, optional
        The maximum edge weight value.
        The default value is Float.POSITIVE_INFINITY.
>>> min_val : String, optional
        The minimum edge weight value.
        The default value is Float.NEGATIVE_INFINITY.
>>> bidirectional_check : String, optional
        If it is true, Giraph will check whether each edge is bidirectional.
            The default value is "False".
>>> bias_on : String, optional
        True means turn bias calculation on, and False means turn bias calculation off.
        The default value is false.
Returns

>>> output : AlgorithmReport

>>> After execution, the algorithm's results are stored in the database.
    The convergence curve is accessible through the report object.

For a more complete definition of the Lambda parameter, see :term:`Lambda`.

Example


>>> Graph.ml.als(
                input_edge_property_list="source",
                input_edge_label="link",
                output_vertex_property_list="als_results, als_bias",
                vertex_type="vertex_type",
                edge_type="edge_type",
                num_worker="3",
                max_supersteps="20",
                feature_dimension="3"
                als_lambda="0.065",
                convergence_threshold="0.0",
                learning_output_interval="1",
                max_val="5",
                min_val="1"
                bidirectional_check="false",
                bias_on="true"
    )


.. _CGD:

Conjugate Gradient Descent (CGD)
================================

See: http://en.wikipedia.org/wiki/Conjugate_gradient_method.

The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithm.

Our implementation is based on the following paper.

Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model. In ACM KDD 2008. (Equation 5)
http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

This algorithm for collaborative filtering is used in recommendation systems to suggest items (products, movies, articles, and so on) to potential users based on historical records of items that all users have purchased, rated, or viewed.
The records are usually organized as a preference matrix P, which is a sparse matrix holding the preferences (such as, ratings) given by users to items.
Similar to ALS, CGD falls in the category of matrix factorization/latent factor model that infers user profiles and item profiles in low-dimension space, such that the original matrix P can be approximated by a linear model.

Comparison between CGD and ALS
------------------------------

The CGD model is the same as that of ALS except that CGD employs the conjugate gradient descent instead of least squares in optimization.
Refer to the ALS discussion above for more details on the model.
CGD and ALS share the same bipartite graph representation and the same cost function.
The only difference between them is the optimization method.

ALS solves the optimization problem by least squares that requires a matrix inverse.
Therefore, it is computation and memory intensive.
But ALS, a 2nd-order optimization method, enjoys higher convergence rate and is potentially more accurate in parameter estimation.

On the otherhand, CGD is a 1.5th-order optimization method that approximates the Hessian of the cost function from the previous gradient information through N consecutive CGD updates.
This is very important in cases where the solution has thousands or even millions of components.
CGD converges slower than ALS but requires less memory.

Whenever feasible, ALS is a preferred solver over CGD, while CGD is recommended only when the application requires so much memory that it might be beyond the capacity of the system.

CGD Example Usage
-----------------

Input data format
~~~~~~~~~~~~~~~~~

The CGD algorithm takes input data represented in CSV, JSON or XML format.
In this example, we use a CSV file.
Each CSV file consists of at least five columns as shown in the table below.
The user column is a list of user IDs.
The movie column is a list of movie IDs.
The rating column records how the user rates the movie in each row.
The vertex_type labels the type of the source vertex in each row.
The splits column specifies if this row of data is for training, validation, or testing.
We used TR, VA, TE for these three types of splits, respectively.

Data import
~~~~~~~~~~~

To import the CGD data, use the following ipython calls that we provide, as shown below.

The example above shows how to load CGD input data from a CSV file.
The first line imports the related python modules.
The second line gets the frame builder into the fb object.
The third line specifies the path to the input file.
The rest of the lines import the input data.
Here is detailed description of the build_from_csv method.

The first argument is the name you want to give to the frame.
We use CgdFrame in this example.

The second argument is the path to your input file, in this case, /user/hadoop/cgd.csv.

The third argument is the schema of the input data.
You need to name each column, and specify the data type of each column in your input CSV input data.

The fourth argument is whether to overwrite the frame if you have imported data to CGDFrame frame before.


Graph Construction
~~~~~~~~~~~~~~~~~~

After you import the raw data, you register which fields to use for the source vertex, which fields to use for the target vertex, and then construct a graph from your input data.

In the example above, the first line imports the graph construction related python modules.
The second line gets the graph builder object into gb.
The third to fifth lines register your graph, that is, configure.
The third line registers the user column as the source vertex, and registers the vertex_type vertex property to this vertex.
The fourth line registers the movie column as the target vertex.
The fifth line registers that each edge from user to movie, with the label rates.
Also, rating and splits are two edge properties registered for this algorithm.
The sixth line builds a graph based on your input data and graph registration, with graph nameCgdGraph.
The overwrite=True in this line means that if you have previously built a graph with the same name, you want to overwrite the old graph.


Run CGD algorithm
~~~~~~~~~~~~~~~~~

After graph construction, run the CGD algorithm, as shown in the example below.

In the example above, the first line calls the algorithm.
The second line specifies which edge property you want to use for the CGD algorithm.
The third line specifies which edge label you want to use for this algorithm.
Line four specifies the property name for vertex type.
We registered vertex_type for the vertex type above.
Line five specifies the property name for edge type.
Previously, we registered splits for the edge type.
Line six specifies that at most we want to run 20 super steps for this algorithm.
Line seven configures three feature dimensions for CGD.
Line eight sets the convergence threshold to 0.
Line nine sets cgd_lamda to 0.065.
Line ten sets output learning to each iteration.
Line eleven turns bias calculation on.
Line twelve sets the run to three iterations in each super step.
Line thirteen specifies which vertex property names to use for the CGD results.
Because we configured three feature dimensions: cgd_p0, cgd_p1, and cgd_p2; CGD will store the results for feature dimension 0, 1, and 2 respectively.
Because bias term update is turned on, cgd_bias will store the bias term result.

Depending on your use case, you may want to save your CGD results in one vertex property with vector values for each vertex, and not in separate vertex properties.
We also support this scenario.
The example below shows how to use this feature.

The first twelve lines are the same as the previous example.
The difference is at lines thirteen and fourteen.
Line thirteen enables using vector as a vertex property value.
Line fourteen specifies the property name to use to save the CGD results.
In this case, the result will be stored in cgd_results in a comma separated list.
The bias result will be stored in cgd_bias.

>>> Required parameters:
>>> input_edge_property_list : List (comma-separated list of strings)
        The edge properties which contain the input edge values.
        If you use more than one edge property.
        We expect a comma-separated string list.
>>> input_edge_label : String
        The edge property which contains the edge label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex values.
        If you use more than one vertex property, we expect a
        comma-separated string list.
>>> vertex_type : String
        The vertex property which contains the vertex type.
>>> edge_type : String
        The edge property which contains the edge type.
>>> num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum, 
        use on the fly when needed for your data sets.
>>> mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts,
        use on the fly when needed for your data sets.
>>> vector_value: String, optional
        "True" means the algorithm supports a vector as a vertex value.
        "False" means the algorithm does not support a vector as a vertex value.
>>> num_worker : String, optional
        The number of Giraph workers.
        The default value is 15.
>>> max_supersteps :  String, optional
        The number of super steps to run in Giraph.
        The default value is 10.
>>> feature_dimension : String, optional
        The feature dimension.
        The default value is 3.
>>> cgd_lambda : String, optional
        The regularization parameter: 
        f = L2_error + lambda*Tikhonov_regularization
        The default value is 0.065.
>>> convergence_threshold : String, optional
        The convergence threshold which controls how small the change in validation 
        error must be in order to meet the convergence criteria.
        The default value is 0.
>>> learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each CGD iteration is composed by 2 super steps, the default one 
        iteration means two super steps.
>>> max_val : String, optional
        The maximum edge weight value.
        The default value is Float.POSITIVE_INFINITY.
>>> min_val : String, optional
        The minimum edge weight value.
        The default value is Float.NEGATIVE_INFINITY.
>>> bias_on : String, optional
        True means turn on bias calculation and False means turn off bias calculation.
        The default value is false.
>>> bidirectional_check : String, optional
        If it is true, Giraph will check whether each edge is bidirectional.
            The default value is "False".
>>> num_iters : 
        The number of CGD iterations in each super step.
        The default value is 5.
>>> After execution, the algorithm's results are stored in database.
    The convergence curve is accessible through the report object.
>>> Example
>>> Graph.ml.cgd(
               input_edge_property_list="rating",
               input_edge_label="rates",
               output_vertex_property_list="cgd_results, cgd_bias",
               vertex_type="vertex_type",
               edge_type="edge_type",
               num_worker="3",
               max_supersteps="20",
               feature_dimension="3",
               cgd_lambda="0.065",
               convergence_threshold="0.001",
               learning_output_interval="1",
               max_val="10",
               min_val="1",
               bias_on="false",
               num_iters="3")


.. _Graph_Analytics:

---------------
Graph Analytics
---------------
..TODO::
    We support three algorithms in this category, :ref:`APL`, :ref:`CC`, and :ref:`PR`

    .. _APL:

    Average Path Length (APL)
    = ========================

    The average path length algorithm calculates the average path length from a vertex to any other vertices.

    >>> Parameters
    >>> ----------
    >>> input_edge_label : String
            The edge property which contains the edge label.
    >>> output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values.
            If you use more than one vertex property, we expect a comma-separated string list.

    >>> num_mapper : String, optional
            A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
            Use on the fly when needed for your data sets.
    >>> mapper_memory : String, optional
            A reconfigured Hadoop parameter mapred.map.child.java.opts.
            Use on the fly when needed for your data sets.
    >>> convergence_output_interval : String, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
    >>> num_worker : String, optional
            The number of Giraph workers.
            The default value is 15.

    Returns


    Output : AlgorithmReport

    >>>     The algorith's results in the database.
            The progress curve is accessible through the report object.

    Example


    >>> graph.ml.avg_path_len(
                    input_edge_label="edge",
                    output_vertex_property_list="apl_num, apl_sum",
                    convergence_output_interval="1",
                    num_worker="3"
        )


    .. _CC:

    Connected Components (CC)
    = ========================

    The connected components algorithm finds all connected components in graph.
    The implementation is inspired by PEGASUS paper.

    >>> Parameters
    >>> ----------
    >>> input_edge_label : String
            The edge property which contains the edge label.
    >>> output_vertex_property_list : List (comma-separated string list)
            The vertex properties which contain the output vertex values.
            If you use more than one vertex property, we expect a comma-separated string list.

    >>> num_mapper : String, optional
            A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
            Use on the fly when needed for your data sets.
    >>> mapper_memory : String, optional
            A reconfigured Hadoop parameter mapred.map.child.java.opts.
            Use on the fly when needed for your data sets.
    >>> convergence_output_interval : String, optional
            The convergence progress output interval.
            The default value is 1, which means output every super step.
    >>> num_worker : String, optional
            The number of Giraph workers.
            The default value is 15.

    Returns


    >>>output : AlgorithmReport
        The algorithm's results in the database.
        The progress curve is accessible through the report object.

    Example


    >>> graph.ml.connected_components(
                    input_edge_label="connects",
                    output_vertex_property_list="component_id",
                    convergence_output_interval="1",
                    num_worker="3"
        )


.. _PR:

Page Rank (PR)
==============

This is the algorithm used by web search engines to rank the relevance of the pages returned by a query.
See: http://en.wikipedia.org/wiki/PageRank.

>>> Parameters
>>> input_edge_label : String
        The edge property which contains the edge label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex values.
        If you use more than one vertex property, we expect a comma-separated string list.
>>> num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
        Use on the fly when needed for your data sets.
>>> mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts.
        Use on the fly when needed for your data sets.
>>> num_worker : String, optional
        The number of Giraph workers.
        The default value is 15.
>>> max_supersteps : String, optional
        The number of super steps to run in Giraph.
        The default value is 20.
>>> convergence_threshold : String, optional
        The convergence threshold which controls how small the change in belief value 
        must be in order to meet the convergence criteria.
        The default value is 0.001.
>>> reset_probability : String, optional
        The probability that the random walk of a page is reset.
        The default value is 0.15.
>>> convergence_output_interval : String, optional
        The convergence progress output interval.
        The default value is 1, which means output every super step.

Returns

>>> output : AlgorithmReport
        The algorithm's results in database.
        The progress curve is accessible through the report object.

Example


>>> graph.ml.page_rank(self,
                      input_edge_label="edges",
                      output_vertex_property_list="page_rank",
                      num_worker="3",
                      max_supersteps="20",
                      convergence_threshold="0.001",
                      reset_probability="0.15",
                      convergence_output_interval="1"
     )


.. _Graphical_Models:

----------------
Graphical Models
----------------


The graphical models find more insights from structured noisy data.
We currently support :ref:`LP` and :ref:`LBP`

.. _LP:

Label Propagation (LP)
======================

Label propagation (LP) is a message passing technique for imputing or smoothing labels in partially labelled datasets. 
Labels are propagated from "labeled" data to unlabeled data along a graph encoding similarity relationships among data points.
The labels of known data can be probabilistic. 
In other words: a "known" point can be represented with fuzzy labels such as 90% label 0 and 10% label 1.
Distance between data points is represented by edge weights, with closer points having a stronger influence than points farther away. 
LP has been used in many contexts problems where a similarity measure between instances is available and can be exploited.
    
Our implementation is based on this paper: X. Zhu and Z. Ghahramani. 
Learning from labeled and unlabeled data with label propagation. 
Technical Report CMU-CALD-02-107, CMU, 2002. See: http://www.cs.cmu.edu/~zhuxj/pub/CMU-CALD-02-107.pdf
  
The Label Propagation Algorithm
     
In LP, all nodes start with a prior distribution of states and the initial messages that vertices pass to their neighbors are simply their prior beliefs. 
If certain observations have states that are known deterministically, they can be given a prior probability of 100% for their true state and 0% for 
all other states.
Unknown observations should be given uninformative priors.
    
Each node, :math:`i`, receives messages from their :math:`k` neighbors and updates their beliefs by taking a weighted average of their current beliefs
and a weighted average of the messages received from its neighbors.
    
The updated beliefs for node :math:`i` are:

.. math::

    updated\ beliefs_{i} = \lambda * (prior\ belief_{i} ) + (1 - \lambda ) * \sum_k w_{i,k} * previous\ belief_{k}

Where :math:`w_{i,k}` is the distance between nodes :math:`i` and :math:`k` such that the sum of all distances to neighbors sums to one,
and :math:`\lambda` is a learning parameter.
If :math:`\lambda` is greater than zero, updated probabilities will be anchored in the direction of prior beliefs.
The final distribution of state probabilities will also tend to be biased in the direction of the distribution of initial beliefs. 
For the first iteration of updates, nodes' previous beliefs are equal to the priors and in each future iteration,
previous beliefs are equal to their beliefs as of the last iteration.

All beliefs for every node will be updated in this fashion, including known observations, unless anchor_threshold is set.
The anchor_threshold parameter specifies a probability threshold above which beliefs should no longer be updated. 
Hence, with an anchor_threshold of 0.99, observations with states known with 100% certainty will not be updated by this algorithm.

This process of updating and message passing continues until the convergence criteria is met or the maximum number of super steps is reached 
without converging.
A node is said to converge if the total change in its cost function is below the convergence threshold.
The cost function for a node is given by:

.. math::

    cost = \sum_k w_{i,k} * \left [ \left ( 1 - \lambda \right ) * \left [ previous\ belief_{i}^{2} - w_{i,k} * previous\ belief_{i} * previous\
    belief_{k} \right ] + 0.5 * \lambda * \left ( previous\ belief_{i} - prior_{i} \right ) ^{2} \right ]

Convergence is a local phenomenon; not all nodes will converge at the same time. 
It is also possible for some (most) nodes to converge and others to never converge. 
The algorithm requires all nodes to converge before declaring that the algorithm has converged overall. 
If this condition is not met, the algorithm will continue up to the maximum number of super steps.

.. _LBP:

Loopy Belief Propagation (LBP)
==============================

See: http://en.wikipedia.org/wiki/Belief_propagation.

Loopy Belief Propagation (LBP) is a message passing algorithm for inferring state probabilities given a graph and a set of noisy initial
estimates of state probabilities.
The Intel Analytics Toolkit provides two implementations of LBP, which differ in their assumptions about the joint distribution of the data.
The standard LBP implementation assumes that the joint distribution of the data is given by a Boltzmann distribution, while Gaussian LBP
assumes that the data is continuous and distributed according to a multivariate normal distribution.
For more information about LBP, see: "K. Murphy, Y. Weiss, and M. Jordan, Loopy-belief Propagation for Approximate Inference: An Empirical Study, UAI 1999."

LBP has a wide range of applications in structured prediction, such as low-level vision and influence spread in social networks,
where we have prior noisy predictions for a large set of random variables and a graph encoding relationships between those variables.

The algorithm performs approximate inference on an undirected graph of hidden variables, where each variable is represented as a node,
and edges encode relations to its neighbors.
Initially, a prior noisy estimate of state probabilities is given to each node, then the algorithm infers the posterior distribution of
each node by propagating and collecting messages to and from its neighbors and updating the beliefs.

In graphs containing loops, convergence is not guaranteed, though LBP has demonstrated empirical success in many areas and in practice
often converges close to the true joint probability distribution.

Discrete Loopy Belief Propagation:
----------------------------------

LBP is typically considered a semi-supervised machine learning algorithm as
    1) there is typically no ground truth observation of states and
    #) the algorithm is primarily concerned with estimating a joint probability function rather than with classification or point prediction.

The standard (discrete) LBP algorithm requires a set of probability thresholds to be considered a classifier.
Nonetheless, the discrete LBP algorithm allows Test/Train/Validate splits of the data and the algorithm will treat "Train" observations
differently from "Test" and "Validate" observations.
Vertices labelled with "Test" or "Validate" will be treated as though they have uninformative (uniform) priors and are allowed to receive messages,
but not send messages.
This simulates a "scoring scenario" in which a new observation is added to a graph containing fully trained LBP posteriors,
the new vertex is scored based on received messages, but the full LBP algorithm is not repeated in full.
This behavior can be turned off by setting the ``ignore_vertex_type`` parameter to True.
When ``ignore_vertex_type=True``, all nodes will be considered "Train" regardless of their sample type designation.
The Gaussian (continuous) version of LBP does not allow Train/Test/Validate splits.

The standard LBP algorithm included with the toolkit assumes an ordinal and cardinal set of discrete states.
For notational convenience, we'll denote the value of state :math:`s_{i}` as :math:`i`, and the prior probability of state
:math:`s_{i}` as :math:`prior_{i}`.

Each node sends out initial messages of the form:

.. math::

   \ln \left ( \sum_{s_{j}} \exp \left ( - \frac { | i - j | ^{p} }{ n - 1 } * w * s + \ln (prior_{i}) \right ) \right )

Where :math:`w` is equal to the weight between the messages destination and origin vertices, :math:`s` is equal to the smoothing parameter,
:math:`p` is the power parameter, and :math:`n` is the number of states.
The larger the weight between two nodes or the higher the smoothing parameter, the more neighboring vertices are assumed to "agree" on states.
(Here, we represent messages as sums of log probabilities rather than products of non-logged probabilities as it makes it easier to subtract
messages in the future steps of the algorithm.)
Also note that the states are cardinal in the sense that the "pull" of state :math:`i` on state :math:`j` depends on the distance
between :math:`i` and :math:`j`.
The *power* parameter intensifies the rate at which the pull of distant states drop off.

In order for the algorithm to work properly, all edges of the graph must be bidirectional.
In other words, messages need to be able to flow in both directions across every edge.
Bidirectional edges can be enforced during graph building, but the LBP function provides an option to do an initial check for
bidirectionality using the ``bidirectional_check=True`` option.
If not all the edges of the graph are bidirectional, the algorithm will return an error.

For example, in a two state case in which a node has prior probabilities 0.8 and 0.2 for states 0 and 1 respectively, uniform weights of 1,
power of 1 and a smoothing parameter of 2, we would have a vector valued initial message equal to:
:math:`\textstyle \left [ \ln \left ( 0.2 + 0.8 e ^{-2} \right ), \ln \left ( 0.8 + 0.2 e ^{-2} \right ) \right ]`,
which gets sent to each of that node's neighbors.
Note that messages will typically not be proper probability distributions, hence each message is normalized so that the probability
of all states sum to 1 before being sent out.
For simplicity, we will consider all messages going forward as normalized messages.

After nodes have sent out their initial messages, they then update their beliefs based on messages that they have received from their neighbors,
denoted by the set :math:`k`.

Updated Posterior Beliefs:

.. math::

   \ln (newbelief) = \propto \exp \left [ \ln (prior) + \sum_k message _{k} \right ]

Note that the messages in the above equation are still in log form.
Nodes then send out new messages which take the same form as their initial messages,
with updated beliefs in place of priors and subtracting out the information previously received from the new message's recipient.
The recipient's prior message is subtracted out to prevent feedback loops of nodes "learning" from themselves.

In updating beliefs, new beliefs tend to be most influenced by the largest message.
Setting the ``max_product`` option to "True" ignores all incoming messages other than the strongest signal.
Doing this results in approximate solutions, but requires significantly less memory and run-time than the more exact computation.
Users should consider this option when processing power is a constraint and approximate solutions to LBP will be sufficient.

.. math::

   \ln \left ( \sum_{s_{j}} \exp \left ( - \frac { | i - j | ^{p} }{ n - 1 } * w * s + \ln (newbelief_{i}) -
   previous\ message\ from\ recipient \right ) \right )

This process of updating and message passing continues until the convergence criteria is met or the maximum number of super steps is
reached without converging.
A node is said to converge if the total change in its distribution (the sum of absolute value changes in state probabilities) is less than
the ``convergence_threshold`` parameter.
Convergence is a local phenomenon; not all nodes will converge at the same time.
It is also possible for some (most) nodes to converge and others to never converge.
The algorithm requires all nodes to converge before declaring that the algorithm has converged overall.
If this condition is not met, the algorithm will continue up to the maximum number of super steps.

Gaussian Loopy Belief Propagation:
----------------------------------

Gaussian Loopy Belief Propagation will be included in later releases, but is not available in 0.8.0.

.. _Topic_Modeling:

--------------
Topic Modeling
--------------


For Topic Modeling, see: http://en.wikipedia.org/wiki/Topic_model

.. _LDA:

Latent Dirichlet Allocation (LDA)
=================================

We currently support Latent Dirichlet Allocation (LDA) for our topic modeling.

See: http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation

This is an algorithm for topic modeling that discovers the hidden topics from a collection of documents and annotates the document according to those topics.
You can use resulting topical representation as a feature space in information retrieval tasks to group topically related words and documents and to organize, summarize and search the texts.
See the excellent demo of LDA on Wikipedia here: http://www.princeton.edu/~achaney/tmve/wiki100k/browse/topic-presence.html

Solving the latent topic assignment problem is an NP-Hard task.
There exist several approximate inference algorithms.
Our implementation is based on the CVB0 LDA algorithm, one of the state of the art LDA solvers, presented in "Y.W. Teh, D. Newman, and M. Welling, A Collapsed Variational Bayesian Inference Algorithm for Latent Dirichlet Allocation, NIPS 19, 2007.
http://www.gatsby.ucl.ac.uk/~ywteh/research/inference/nips2006.pdf

The LDA Model
-------------

A typical representation of LDA is a bipartite graph, where nodes on the left side represent a collection of documents and nodes on the right side represents a set of words (for example., vocabulary), and edges encode number of occurrences of a word in a corresponding document (see the example below).

The LDA Algorithm
-----------------

After the execution of LDA on the input bi-partite graph, each node in the graph will be associated with a vector of length k (such as, the number of topics specified by user).
For a document node d, p(ti|d) denotes the distribution over topics to document d, and ?_(i=1)^k??p(t_i?d)=1?.
For a word node w, p(w|ti) denotes the distribution over words to each topic ti.
Theoretically, p(w|ti) should be normalized such that ?_w??p(w?t_i )=1?.
But this normalization is ignored in the implementation because it requires normalizing scores across all the words, which incurs an additional map-reduce step.
This normalization is expensive but wouldn't bring us too much benefit because to identify the top words for a topic we only need a sort across all the words.

At a high-level, LDA extracts semantically similar words into a topic, such as "foods", "sports", and "geography", and it groups similar documents according to the extracted topics.
The underlying assumptions are intuitive: (1) words in the same documents are topically related; (2) documents that share common words are likely about similar topics.

LDA Example Usage
-----------------

Input data format
~~~~~~~~~~~~~~~~~

The LDA algorithm takes an input text corpus represented in CSV, JSON or XML format.
We use a CSV file in this example.
Each CSV file consists of at least four columns as shown in the table below.
The "doc" column is a list of document titles.
The "word" column is a list of words in these documents.
The "count" column records how many times a word appears in a given document.
The "vertex_type" labels the type of the source vertex in each row.

Data import
~~~~~~~~~~~

To import the LDA input data, you can use the following iPython calls:

The example above loads the LDA input data from a CSV file.
The first line imports the python modules.
The second line gets the frame builder into the fb object.
The third line specifies where the path to the input file.
The remainder of the lines perform the data import through the build_from_csv method:

The first argument is a name you want to give to the frame.
This example uses lda.

The second argument the path to your input file.
In this case: /user/hadoop/test_lda.csv.

The third argument is the schema of the input data.
You need to name each column, and specify the data type of each column in your input CSV input data.

The fourth argument is whether to overwrite the frame; true overwrites the frame, if you have imported data to the lda frame before.

Graph Construction
~~~~~~~~~~~~~~~~~~

After you import the raw data, you register which fields to use for the source vertex, which fields to use for the target vertex, and then construct a graph from your input data.

In the example above, the first line imports the python modules needed for graph construction.
The second line gets the graph builder object into gb.
The third to fifth lines register the graph.
Line 3 registers the doc column as the source vertex, and registers the vertex property vertex_type to this vertex.
Line 4 registers the word column as the target vertex, and line 5 registers an edge from doc to word, with the label has, and count as the edge property.
Finally, line 6 builds a graph named ldagraph based on the input data and graph registration.
The overwrite option specifies that an existing graph with this name will be overwritten.

Run LDA algorithm
~~~~~~~~~~~~~~~~~

After graph construction, we run the LDA algorithm as shown:

In example above, the first line starts the call to the algorithm.
The second and third lines specify the edge property and edge label to use.
Line 4 specifies the property name for the vertex type; in this example we register the property named vertex_type.
The fifth line sets the num_topics parameter used by LDA.
Line six specifies the vertex property names in which to save the LDA results; because we configure three topics, these three properties will store the normalized probability that the vertex belongs to topics 0, 1, and 2 respectively.
Finally, line seven specifies that we want to run at most five super steps for this algorithm.

It is possible to save the LDA results either in separate vertex properties, or in one vertex property with vector value for each vertex.
The example below shows this feature.

The first five lines are the same as the previous example.
The difference is at the sixth and seventh lines.
Line six enables using a vector as a vertex property value while line seven specifies the property name to use to save the LDA results.
In this case, the result will be stored in a comma separated list.
The eighth line is the same as the seventh line in previous example.


>>> Parameters

>>> input_edge_property_list : List (comma-separated list of strings)
        The edge properties which contain the input edge values.
        If you use more than one edge property, we expect a comma-separated string list.
>>> input_edge_label : String
        The edge property which contains the edge label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex values.
        If you use more than one vertex property, we expect a comma-separated string list.
>>> vertex_type : String
        The vertex property which contains the vertex type.

>>> num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
        Use on the fly when needed for your data sets.
>>> mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts.
        Use on the fly when needed for your data sets.
>>> vector_value : String, optional
        "True" means the algorithm supports a vector as a vertex value.
        "False" means the algorithm does not support a vector as a vertex value.
>>> num_worker : String, optional
        The number of workers.
        The default value is 15.
>>> max_supersteps :String, optional
        The number of super steps to run in Giraph.
        The default value is 20.
>>> alpha : String, optional
        The document-topic smoothing parameter.
        The default value is 0.1.
>>> beta : String, optional
        The term-topic smoothing parameter.
        The default value is 0.1.
>>> convergence_threshold : String, optional
        The convergence threshold which controls how small 
        the change in edge value must be in order to meet the 
        convergence criteria.
        The default value is false.
>>> evaluate_cost : String, optional
        True means turn cost evaluation on, and False means 
        turn cost evaluation off.
        The default value is false.
>>> max_val : String, optional
        The maximum edge weight value.
        The default value is Float.POSITIVE_INFINITY.
>>> min_val : String, optional
        The minimum edge weight value.
        The default value is Float.NEGATIVE_INFINITY.
>>> num_topics : String, optional
        The number of topics to identify.
        The default value is 10.

Returns

>>> output : AlgorithmReport
        The algorithm's results in the database.
        The convergence curve is accessible through the report object.

Example


>>> graph.ml.lda(
                input_edge_property_list="frequency",
                input_edge_label="has",
                output_vertex_property_list="lda_results",
                vertex_type="vertex_type",
                edge_type="edge_types",
                num_worker="3",
                max_supersteps="20",
                alpha="0.1",
                beta="0.1",
                convergence_threshold="0.0001",
                evaluate_cost="true",
                max_val=" Float.POSITIVE_INFINITY",
                min_val=" Float.NEGATIVE_INFINITY",
                num_topics="10"
    )

Perform Analytics on the Graph
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you have all your data in the Titan graph database, you are now able to perform additional analytics to view and explore your data.
This is where you look at what was once raw data and now has some form and much more information.
You will use the Machine Learning API calls to do this.
See the Machine Learning page for more details.

We built the interface in iPython notebooks, because many data scientists are already familiar with the Python interface and use of iPython notebooks.
For complex graph traversals and mutation operations, see: https://github.com/thinkaurelius/titan/wiki/Gremlin-Query-Language.

You do not have to read the graph.
Once you have the graph object, from the graph construction section, you can run Machine Learning algorithms on it immediately.
The first line of the code in Figure 1, prepares iPython for the upcoming visualizations.

In the second line, we name our report.
In this case, report1, but you can name it whatever you want.
The graph.ml.gd() method is the gradient descent algorithm.
You can call the other algorithms in the same way, such as: graph.ml.als() for the alternating least squares algorithm (using the appropriate parameters, as described in the API documentation).
In the graph.ml.gd() method call, we assign each of the parameters a value.
Refer to the API documentation and the Machine Learning Algorithms page.

Figure 1: Read from Graph Database and Run Machine Learning Algorithms.

After the algorithm has finished, you can use the report object to look at how the execution has performed.

In Figure 2, in line 64, we can view the start time so we can keep track of how long this takes.

In line 65, we can see the assigned graph name in report1, which is the underlying name of the Titan graph, that the algorithm has been run.

In line 67, you can see how the algorithm has performed and how with each iteration the cost has improved.


Figure 2, Graph Creation

In line 69 above, rmse_validate is a command that shows the root-mean-square error in each of the iterations on the validation data set.

Now you need to run the algorithm against the test data set to see how it performs using the data set aside for testing purposes.

In line 70, rmse_test determines the root-mean-square error on the test data.

In line 71 below, the graph.ml.als() command runs the alternating least squares algorithm on the same dataset.

Figure 3, Run Alternating Least Squares Algorithm

Once again, you can see the results of the ALS algorithm and how it performed.

Figure 4, Cost Training, Validation, and Testing 

Now we run the conjugated gradient descent algorithm on the same data set.

Figure 5, Run the Conjugated Gradient Descent Algorithm

The last commands you can run for this part of analytics are looking at the runs.

Figure 6, Cost, Validate, and Test

As you can see from the examples above, the Intel® Analytics Toolkit makes data transformations and running prebuilt algorithms easier and faster with the simple Python interface.

This last figure shows a recommendation based on trained learning.
We look at the recommendation for a user, in this case, 10001, and what the top 10 recommended movies and ratings are for that user.

For movie '-92,' the recommendation shows what are the top 10 users and their scores that will most enjoy this movie.

Finally, we deliberately entered an unknown value to the recommendation as an example of what our errors look like.


Figure 7, Trained Learning and Error Message

.. _Wikipedia\: Collaborative Filtering: http://en.wikipedia.org/wiki/Collaborative_filtering
.. _Columbia Data Science\: Blog Week-7: http://columbiadatascience.com/2012/10/18/week-7-hunch-com-recommendation-engines-svd-alternating-least-squares-convexity-filter-bubbles/
.. _Factorization Meets the Neighborhood\: a Multifaceted Collaborative Filtering Model: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf
.. _Large-Scale Parallel Collaborative Filtering for the Netflix Prize: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797

