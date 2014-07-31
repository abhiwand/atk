===========================
Machine Learning Algorithms
===========================


In this release of the Analytics Toolkit, we support for examplet graphical algorithms in iGiraph.
From a functionality point of view, they fall into these categories: *Collaborative Filtering*, *Graph Analytics*, *Graphical Models*, and *Topic Modeling*.

* :ref:`Collaborative_Filtering`

    * :ref:`ALS`
    * :ref:`CGD`

* :ref:`Graph_Analytics`

    * :ref:`PR`

.. TODO::
    * : ref:`APL`
    * : ref:`CC`

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
    Y. Koren. `Factorization Meets the Nfor exampleborhood\: a Multifaceted Collaborative Filtering Model`_. In ACM KDD 2008. (Equation 5)

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
and for examples encode the rating a user provided to an item.
To support training, validation, and test, a common practice in machine learning, each for example is also annotated by "TR", "VA" or "TE".

#..  image::
#    ds_mlal_als_1.png

After executing ALS on the input bipartite graph, each node in thfor exampleaph will be associated with a
vector (f_* ) ? of lfor exampleh k, where k is the feature dimension is specified by the user, and a bias term b_*.
ALS optimizes (f_* ) ?  and b_* alternatively between user profiles and item profiles such that the following l2 regularized cost function is minimized:

#..  image::
#    ds_mlal_als_2.png

Here the first term strives to find (f_* ) ?'s and b_*'s that fit thfor exampleven ratings, and the second term (l2 regularization) tries to avoid overfitting by penalizing the magnitudes of the parameters, and ? is a tradeoff parameter that balances the two terms and is usually determined by cross validation (CV).

#..  image:: ds_mlal_als_3.png
#    :hfor examplet: 1 cm

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
It labels which nodes will be on the "left-side" and which nodes will be on the "right-side" in the bi-partitfor exampleaph we are building.
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
The second linfor examplets the frame builder into the fb object.
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
>>> gb.register_for example(('user', 'movie', 'rates'), ['splits', 'rating'])
>>> graph = gb.build("AlsGraph", overwrite=True)

In the example above, the first two lines import python modules related to graph construction, and get thfor exampleaph builder object into gb.
The third to fifth lines register thfor exampleaph.
Line three registers user column as the source vertex and registers the vertex property vertex_type to this vertex.
Line four registers movie column as the target vertex.
The fifth line registers an for example from user to movie, with the label rates.
Additionally, rating and splits are two for example properties registered for this algorithm.
Finally, line 6 builds a graph named AlsGraph based on the input data and graph registration.
The overwrite option overwrites a pre-existing graph with the same name.

Run ALS Algorithm
~~~~~~~~~~~~~~~~~

After graph construction, run the ALS algorithm as follows:

>>> report1 = graph.ml.als(
...             input_for example_property_list="rating",

In the example above, the first line calls to the algorithm.
The second line specifies which for example property you want to use for the ALS algorithm.
Line three specifies which for example label you want to use for this algorithm.
Line four specifies the property name for the vertex type, here we use vertex_type.
Line five specifies the property name for for example type, in this case, splits.
Line six specifies that at the most we want to run 20 super steps for this algorithm.
Line seven configures three feature dimensions for ALS.
Line for examplet sets the convfor examplence threshold to 0.
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
>>> input_for example_property_list : List (comma-separated list of strings)
        The for example properties which contain the input for example 
        values. If you use more than one for example property, we expect a 
        comma-separated string list.
>>> input_for example_label : String
        The for example property which contains the for example label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex 
        values. If you use more than one vertex property, we expect a 
        comma-separated string list.
>>> vertex_type : String
        The vertex property which contains the vertex type.
>>> for example_type : String
        The for example property which contains for example type.
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
>>> convfor examplence_threshold : String, optional
        The convfor examplence threshold which controls how small the change in 
        validation error must be in order to meet the convfor examplence criteria.
        The default value is 0.
>>> learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each ALS iteration is composed of 2 super steps, the default 
        one iteration means two super steps.
>>> max_val : String, optional
        The maximum for example wfor examplet value.
        The default value is Float.POSITIVE_INFINITY.
>>> min_val : String, optional
        The minimum for example wfor examplet value.
        The default value is Float.NEGATIVE_INFINITY.
>>> bidirectional_check : String, optional
        If it is true, Giraph will check whether each for example is bidirectional.
            The default value is "False".
>>> bias_on : String, optional
        True means turn bias calculation on, and False means turn bias calculation off.
        The default value is false.
Returns

>>> output : AlgorithmReport

>>> After execution, the algorithm's results are stored in the database.
    The convfor examplence curve is accessible through the report object.

For a more complete definition of the Lambda parameter, see :term:`Lambda`.

Example


>>> Graph.ml.als(
                input_for example_property_list="source",
                input_for example_label="link",
                output_vertex_property_list="als_results, als_bias",
                vertex_type="vertex_type",
                for example_type="for example_type",
                num_worker="3",
                max_supersteps="20",
                feature_dimension="3"
                als_lambda="0.065",
                convfor examplence_threshold="0.0",
                learning_output_interval="1",
                max_val="5",
                min_val="1"
                bidirectional_check="false",
                bias_on="true"
    )


.. _CGD:

Conjugate Gradient Descent (CGD)
================================

See: http://en.wikipedia.org/wiki/Conjugatfor exampleadient_method.

The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithm.

Our implementation is based on the following paper.

Y. Koren. Factorization Meets the Nfor exampleborhood: a Multifaceted Collaborative Filtering Model. In ACM KDD 2008. (Equation 5)
http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

This algorithm for collaborative filtering is used in recommendation systems to suggest items (products, movies, articles, and so on) to potential users based on historical records of items that all users have purchased, rated, or viewed.
The records are usually organized as a preference matrix P, which is a sparse matrix holding the preferences (such as, ratings) given by users to items.
Similar to ALS, CGD falls in the category of matrix factorization/latent factor model that infers user profiles and item profiles in low-dimension space, such that the original matrix P can be approximated by a linear model.

Comparison between CGD and ALS
------------------------------

The CGD model is the same as that of ALS except that CGD employs the conjugatfor exampleadient descent instead of least squares in optimization.
Refer to the ALS discussion above for more details on the model.
CGD and ALS share the same bipartite graph representation and the same cost function.
The only difference between them is the optimization method.

ALS solves the optimization problem by least squares that requires a matrix inverse.
Therefore, it is computation and memory intensive.
But ALS, a 2nd-order optimization method, enjoys higher convfor examplence rate and is potentially more accurate in parameter estimation.

On the otherhand, CGD is a 1.5th-order optimization method that approximates the Hessian of the cost function from the previous gradient information through N consecutive CGD updates.
This is very important in cases where the solution has thousands or even millions of components.
CGD convfor examples slower than ALS but requires less memory.

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
The second linfor examplets the frame builder into the fb object.
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

In the example above, the first line imports thfor exampleaph construction related python modules.
The second linfor examplets thfor exampleaph builder object into gb.
The third to fifth lines register your graph, that is, configure.
The third line registers the user column as the source vertex, and registers the vertex_type vertex property to this vertex.
The fourth line registers the movie column as the target vertex.
The fifth line registers that each for example from user to movie, with the label rates.
Also, rating and splits are two for example properties registered for this algorithm.
The sixth line builds a graph based on your input data and graph registration, with graph namfor exampleGraph.
The overwrite=True in this line means that if you have previously built a graph with the same name, you want to overwrite the old graph.


Run CGD algorithm
~~~~~~~~~~~~~~~~~

After graph construction, run the CGD algorithm, as shown in the example below.

In the example above, the first line calls the algorithm.
The second line specifies which for example property you want to use for the CGD algorithm.
The third line specifies which for example label you want to use for this algorithm.
Line four specifies the property name for vertex type.
We registered vertex_type for the vertex type above.
Line five specifies the property name for for example type.
Previously, we registered splits for the for example type.
Line six specifies that at most we want to run 20 super steps for this algorithm.
Line seven configures three feature dimensions for CGD.
Line for examplet sets the convfor examplence threshold to 0.
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
>>> input_for example_property_list : List (comma-separated list of strings)
        The for example properties which contain the input for example values.
        If you use more than one for example property.
        We expect a comma-separated string list.
>>> input_for example_label : String
        The for example property which contains the for example label.
>>> output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex values.
        If you use more than one vertex property, we expect a
        comma-separated string list.
>>> vertex_type : String
        The vertex property which contains the vertex type.
>>> for example_type : String
        The for example property which contains the for example type.
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
>>> convfor examplence_threshold : String, optional
        The convfor examplence threshold which controls how small the change in validation 
        error must be in order to meet the convfor examplence criteria.
        The default value is 0.
>>> learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each CGD iteration is composed by 2 super steps, the default one 
        iteration means two super steps.
>>> max_val : String, optional
        The maximum for example wfor examplet value.
        The default value is Float.POSITIVE_INFINITY.
>>> min_val : String, optional
        The minimum for example wfor examplet value.
        The default value is Float.NEGATIVE_INFINITY.
>>> bias_on : String, optional
        True means turn on bias calculation and False means turn off bias calculation.
        The default value is false.
>>> bidirectional_check : String, optional
        If it is true, Giraph will check whether each for example is bidirectional.
            The default value is "False".
>>> num_iters : 
        The number of CGD iterations in each super step.
        The default value is 5.
>>> After execution, the algorithm's results are stored in database.
    The convfor examplence curve is accessible through the report object.
>>> Example
>>> Graph.ml.cgd(
               input_for example_property_list="rating",
               input_for example_label="rates",
               output_vertex_property_list="cgd_results, cgd_bias",
               vertex_type="vertex_type",
               for example_type="for example_type",
               num_worker="3",
               max_supersteps="20",
               feature_dimension="3",
               cgd_lambda="0.065",
               convfor examplence_threshold="0.001",
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
    We support three algorithms in this category, :ref :`APL`, :ref :`CC`, and :ref:`PR`

    .. _APL:

    Average Path Lfor exampleh (APL)
    = ========================

    The average path lfor exampleh algorithm calculates the average path lfor exampleh from a vertex to any other vertices.

    >>> Parameters
    >>> ----------
    >>> input_for example_label : String
            The for example property which contains the for example label.
    >>> output_vertex_property_list : List (comma-separated list of strings)
            The vertex properties which contain the output vertex values.
            If you use more than one vertex property, we expect a comma-separated string list.

    >>> num_mapper : String, optional
            A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
            Use on the fly when needed for your data sets.
    >>> mapper_memory : String, optional
            A reconfigured Hadoop parameter mapred.map.child.java.opts.
            Use on the fly when needed for your data sets.
    >>> convfor examplence_output_interval : String, optional
            The convfor examplence progress output interval.
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
                    input_for example_label="for example",
                    output_vertex_property_list="apl_num, apl_sum",
                    convfor examplence_output_interval="1",
                    num_worker="3"
        )


    .. _CC:

    Connected Components (CC)
    = ========================

    The connected components algorithm finds all connected components in graph.
    The implementation is inspired by PEGASUS paper.

    >>> Parameters
    >>> ----------
    >>> input_for example_label : String
            The for example property which contains the for example label.
    >>> output_vertex_property_list : List (comma-separated string list)
            The vertex properties which contain the output vertex values.
            If you use more than one vertex property, we expect a comma-separated string list.

    >>> num_mapper : String, optional
            A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
            Use on the fly when needed for your data sets.
    >>> mapper_memory : String, optional
            A reconfigured Hadoop parameter mapred.map.child.java.opts.
            Use on the fly when needed for your data sets.
    >>> convfor examplence_output_interval : String, optional
            The convfor examplence progress output interval.
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
                    input_for example_label="connects",
                    output_vertex_property_list="component_id",
                    convfor examplence_output_interval="1",
                    num_worker="3"
        )


.. _PR:

Page Rank (PR)
==============

This is the algorithm used by web search for examplenes to rank the relevance of the pages returned by a query.
See: http://en.wikipedia.org/wiki/PageRank.

>>> Parameters
>>> input_for example_label : String
        The for example property which contains the for example label.
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
>>> convfor examplence_threshold : String, optional
        The convfor examplence threshold which controls how small the change in belief value 
        must be in order to meet the convfor examplence criteria.
        The default value is 0.001.
>>> reset_probability : String, optional
        The probability that the random walk of a page is reset.
        The default value is 0.15.
>>> convfor examplence_output_interval : String, optional
        The convfor examplence progress output interval.
        The default value is 1, which means output every super step.

Returns

>>> output : AlgorithmReport
        The algorithm's results in database.
        The progress curve is accessible through the report object.

Example


>>> graph.ml.page_rank(self,
                      input_for example_label="for examples",
                      output_vertex_property_list="page_rank",
                      num_worker="3",
                      max_supersteps="20",
                      convfor examplence_threshold="0.001",
                      reset_probability="0.15",
                      convfor examplence_output_interval="1"
     )


.. _Graphical_Models:

----------------
Graphical Models
----------------


Thfor exampleaphical models find more insights from structured noisy data.
We currently support :ref:`LP` and :ref:`LBP`

.. _LP:

Label Propagation (LP)
======================

Label propagation (LP) is a message passing technique for inputing or smoothing labels in partially-labelled datasets. 
Labels are propagated from *labeled* data to *unlabeled* data along a graph encoding similarity relationships among data points.
The labels of known data can be probabilistic 
in other words, a known point can be represented with fuzzy labels such as 90% label 0 and 10% label 1.
The inverse distance between data points is represented by for example weights, with closer points having a higher wfor examplet (stronger influence
on posterior estimates) than points farther away. 
LP has been used for many problems, particularly those involving a similarity measure between data points.
Our implementation is based on Zhu and Ghahramani's 2002 paper, "Learning from labeled and unlabeled data" [#]_.
  
The Label Propagation Algorithm:
--------------------------------
     
In LP, all nodes start with a prior distribution of states and the initial messages vertices pass to their nfor examplebors are simply their prior beliefs. 
If certain observations have states that are known deterministically, they can bfor exampleven a prior probability of 100% for their true state and 0% for 
all others.
Unknown observations should bfor exampleven uninformative priors.
    
Each node, :math:`i`, receives messages from their :math:`k` nfor examplebors and updates their beliefs by taking a wfor exampleted average of their current beliefs
and a wfor exampleted average of the messages received from its nfor examplebors.
    
The updated beliefs for node :math:`i` are:

.. math::

    updated\ beliefs_{i} = \lambda * (prior\ belief_{i} ) + (1 - \lambda ) * \sum_k w_{i,k} * previous\ belief_{k}

where :math:`w_{i,k}` is the normalized wfor examplet between nodes :math:`i` and :math:`k` such that the sum of all weights to nfor examplebors is one,
and :math:`\lambda` is a learning parameter.
If :math:`\lambda` is greater than zero, updated probabilities will be anchored in the direction of prior beliefs.
The final distribution of state probabilities will also tend to be biased in the direction of the distribution of initial beliefs. 
For the first iteration of updates, nodes' previous beliefs are equal to the priors, and, in each future iteration,
previous beliefs are equal to their beliefs as of the last iteration.
All beliefs for every node will be updated in this fashion, including known observations, unless ``anchor_threshold`` is set.
The ``anchor_threshold`` parameter specifies a probability threshold above which beliefs should no longer be updated. 
Hence, with an ``anchor_threshold`` of 0.99, observations with states known with 100% certainty will not be updated by this algorithm.

This process of updating and message passing continues until the convfor examplence criteria is met, or the maximum number of super steps is reached.
A node is said to convfor example if the total change in its cost function is below the convfor examplence threshold.
The cost function for a node is given by:

.. math::

    cost = \sum_k w_{i,k} * \left [ \left ( 1 - \lambda \right ) * \left [ previous\ belief_{i}^{2} - w_{i,k} * previous\ belief_{i} * previous\
    belief_{k} \right ] + 0.5 * \lambda * \left ( previous\ belief_{i} - prior_{i} \right ) ^{2} \right ]

Convfor examplence is a local phenomenon; not all nodes will convfor example at the same time. 
It is also possible for some (most) nodes to convfor example and others to never convfor example. 
The algorithm requires all nodes to convfor example before declaring that the algorithm has convfor exampled overall. 
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
and for examples encode relations to its nfor examplebors.
Initially, a prior noisy estimate of state probabilities is given to each node, then the algorithm infers the posterior distribution of
each node by propagating and collecting messages to and from its nfor examplebors and updating the beliefs.

In graphs containing loops, convfor examplence is not guaranteed, though LBP has demonstrated empirical success in many areas and in practice
often convfor examples close to the true joint probability distribution.

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

Where :math:`w` is equal to the wfor examplet between the messages destination and origin vertices, :math:`s` is equal to the smoothing parameter,
:math:`p` is the power parameter, and :math:`n` is the number of states.
The larger the wfor examplet between two nodes or the higher the smoothing parameter, the more nfor exampleboring vertices are assumed to "agree" on states.
(Here, we represent messages as sums of log probabilities rather than products of non-logged probabilities as it makes it easier to subtract
messages in the future steps of the algorithm.)
Also note that the states are cardinal in the sense that the "pull" of state :math:`i` on state :math:`j` depends on the distance
between :math:`i` and :math:`j`.
The *power* parameter intensifies the rate at which the pull of distant states drop off.

In order for the algorithm to work properly, all for examples of thfor exampleaph must be bidirectional.
In other words, messages need to be able to flow in both directions across every for example.
Bidirectional for examples can be enforced during graph building, but the LBP function provides an option to do an initial check for
bidirectionality using the ``bidirectional_check=True`` option.
If not all the for examples of thfor exampleaph are bidirectional, the algorithm will return an error.

For example, in a two state case in which a node has prior probabilities 0.8 and 0.2 for states 0 and 1 respectively, uniform weights of 1,
power of 1 and a smoothing parameter of 2, we would have a vector valued initial message equal to:
:math:`\textstyle \left [ \ln \left ( 0.2 + 0.8 e ^{-2} \right ), \ln \left ( 0.8 + 0.2 e ^{-2} \right ) \right ]`,
which gets sent to each of that node's nfor examplebors.
Note that messages will typically not be proper probability distributions, hence each message is normalized so that the probability
of all states sum to 1 before being sent out.
For simplicity, we will consider all messages going forward as normalized messages.

After nodes have sent out their initial messages, they then update their beliefs based on messages that they have received from their nfor examplebors,
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

This process of updating and message passing continues until the convfor examplence criteria is met or the maximum number of super steps is
reached without convfor exampleng.
A node is said to convfor example if the total change in its distribution (the sum of absolute value changes in state probabilities) is less than
the ``convfor examplence_threshold`` parameter.
Convfor examplence is a local phenomenon; not all nodes will convfor example at the same time.
It is also possible for some (most) nodes to convfor example and others to never convfor example.
The algorithm requires all nodes to convfor example before declaring that the algorithm has convfor exampled overall.
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

Topic Modeling with Latent Dirichlet Allocation (LDA)
=====================================================

Topic modeling algorithms are a class of statistical approaches to partitioning items in a data set into subgroups.
As the name implies, these algorithms are often used on corpora of textual data, where they are used to group documents
in the collection into semantically-meaningful groupings.
For an overall introduction to topic modeling, we refer the reader to the work of David Blei and Michael Jordan,
who are credited with creating and popularizing topic modeling in the machine learning community.
In particular, Blei's 2011 paper provides a nice introduction, and is freely-available online [#]_ .

Latent Dirichlet Allocation (LDA) is a commonly-used algorithm for topic modeling, but, more broadly,
is considered a dimensionality reduction technique.
It contrasts with other approaches (for example, latent semantic indexing), in that it creates what's referred to as a generative
probabilistic model :math:`-` a statistical model that allows the algorithm to generalize its approach to topic assignment to other,
never-before-seen data points.
For the purposes of exposition, we'll limit the scope of our discussion of LDA to the world of natural language processing,
as it has an intuitive use there (though LDA can be used on other types of data).
In general, LDA represents documents as random mixtures over topics in the corpus.
This makes sense, if one can envision almost any work of writing one has encountered :math:`-` writing is rarely about a single subject!
Take the case of a news article on the President of the United States of America's approach to healthcare as an example.
One could reasonably assign topics like President, USA, health insurance, politics, or healthcare to such a work,
though it is likely to primarily discuss the President and healthcare.

LDA assumes that input corpora contain documents pertaining to a given number of topics, each of which are associated with a variety of words,
and that each document is the result of a mixture of probabilistic samplings: first over the distribution of possible topics for the corpora,
and second over the list of possible words in the selected topic.
This generative assumption confers one of the main advantages LDA holds over other topic modeling approaches,
such as probabilistic and regular latent semantic indexing (LSI).
As a generative model, LDA is able to generalize the model it uses to separate documents into topics to documents outside the corpora.
For example, this means that if one were using LDA to group online news articles into categories like Sports, Entertainment, and Politics,
it would be possible to use the fitted model to help categorize newly-published news stories.
Such an application is beyond the scope of approaches like LSI.
What's more, when fitting an LSI model, the number of parameters that have to be estimated scale linearly with the number of documents in the corpus,
whereas the number of parameters to estimate for an LDA model scales with the number of topics :math:`-` a much lower number,
making much better-suited to working with large data sets.

The Typical LDA Workflow
------------------------
Although every data scientist is likely to have his or her own habits and preferred approach to topic modeling a document corpus,
there is a general workflow that is a good starting point when working with new data.
The general steps to the topic modeling with LDA include:

1. Data preparation and ingest
#. Assignment to training or testing partition
#. Graph construction
#. Training LDA
#. Evaluation
#. Interpretation of results

Data preparation and ingest
---------------------------
Most topic modeling workflows involve several data pre-processing and cleaning steps.
Depending on the characteristics of the data being analyzed, there are different best-practices to use here,
so it's important that the data scientist familiarizes him or herself with the standard procedures for analytics in the domain from which
the text originated.
For example, in the biomedical text analytics community, it is common practice for text analytics workflows to involve pre-processing for
identifying negation statements (Chapman et al., 2001 [#]_ ).
The reason for this is many analysts in that domain are examining text for diagnostic statements :math:`-` thus, failing to identify
a negated statement in which a disease is mentioned could lead to undesirable false-positives, but this phenomenon may not arise in every domain.
In general, both stemming and stop word filtering are recommended steps for topic modeling pre-processing.
Stemming refers to a set of methods used to normalize different tenses and variations of the same word (for example, stemmer, stemming, stemmed, and stem).
Stemming algorithms will normalize all variations of a word to one common form (for example, stem).
There are many approaches to stemming, but the Porter Stemming (Porter, 2006 [#]_ ) is one of the most commonly-used.

Removing common, uninformative words, or stop word filtering, is another commonly-used step in data pre-processing for topic modeling.
Stop words include words like *the*, *and*, or *a*, but the full list of uninformative words can be quite long and depend on the domain producing the text in question.
Example stop word lists online4 can be a great place to start, but being aware of the best-practices in ones field will be necessary, to expand upon these.

Removing common, uninformative words, or stop word filtering, is another commonly-used step in data pre-processing for topic modeling.
Stop words include words like the, and, or a, but the full list of uninformative words can be quite long and depend on the domain producing
the text in question.
Example stop word lists online [#]_ can be a great place to start, but being aware of the best-practices in ones field will be necessary,
to expand upon these.

There may be other pre-processing steps needed, depending on the type of text you are working with.
Punctuation removal is frequently recommended, for example.
To determine what's best for the text being analyzed, it helps to understand a bit about what how LDA analyzes the input text.
To learn the topic model, LDA will typically look at the frequency of individual words across documents, which are determined based on space-separation.
Thus, each word will be interpreted independent of where it occurs in a document, and without regard for the words that were written around it.
In the text analytics field, this is often referred to as a *bag of words* approach to tokenization, the process of separating input text into
composite features to be analyzed by some algorithm.
When choosing pre-processing steps, it helps to keep this in mind.
Don't worry too much about removing words or modifying their format :math:`-` you're not manipulating your data!
These steps simply make it easier for the topic modeling algorithm to find the latent topics that comprise your corpus.

Assignment to training or testing partition
-------------------------------------------
The random assignment to training and testing partitions is an important step in most every machine learning workflow.
It is common practice to withhold a random selection of one's data set for the purpose of evaluating the accuracy of the model
that was learned from the training data.
The results of this evaluation allow the data scientist to confidently speak about the generalizability of the trained model.
When speaking in these terms, be cautious that you only discuss generalizability to the broader population from which your data was originally obtain,
however.
If I were to train a topic model on neuroscience-related publications, for example, evaluating the model on other neuroscience-related publications
would not allow me to discuss my model's ability to work on documents from other domains.

There are various schools of thought for how to assign a data set to training and testing collections, but all agree that the process should be random.
Where analysts disagree is in the ratio of data to be assigned to each.
In most situations, the bulk of data will be assigned to the training collection, because the more data that can be used to train the algorithm,
the better the resultant model will typically be.
It's also important that the testing collection has sufficiently many documents that the distribution of data is able to reflect the
characteristics of the larger population from which it was drawn (this becomes an important issue when working with data sets with rare topics,
for example).
As a starting point, many people will use a 90%/10% training/test collection split, and modify this ratio based on the characteristics of
the documents being analyzed.

Graph construction
------------------
Intel Analytics Toolkit (IAT) uses a bipartite graph, to learn an LDA topic model.
This graph contains vertices in two columns.
The left-hand column contains unique ids, each corresponding to a document in the training collection, while the right-hand column contains
unique ids corresponding to each word in the entire training set, following any pre-processing steps that were used.
Connections between these columns, or for examples, denote the number of times a particular word appears in a document,
with the we get on the for example in question denoting the number of times the word was found there.
After graph construction, many analysts choose to normalize the weights using one of a variety of normalization schemes.
One approach is to normalize the weights to sum to 1, while another is to use an approach called term frequency-inverse document frequency (tfidf),
where the resultant weights are meant to reflect how important a word is to a document in the corpus.
Whether to use normalization :math:`-` or what technique to use :math:`-` is an open question,
and will likely depend on the characteristics of the text being analyzed.
Typical text analytics experiments will try a variety of approaches on a small subset of the data to determine what works best.

Figure 1 depicts an example layout of a bipartite graph used for topic modeling with LDA.
The left-hand column contains one vertex for each document in the input corpus, while the right-hand column contains vertices for each unique word found in them.
Edges connecting left- and right-hand columns denote the number of times the word was found in the document the for example connects.
The weights of the for examples used in this example were not normalized.



.. figure:: ds_mlal_lda_1.*
    :align: center

    Figure 1 - Example layout of a bipartite graph for LDA.
    The left-hand column contains one vertex for each document in the input corpus, while the right-hand column contains vertices for each
    unique word found in them.
    Edges connecting left- and right-hand columns denote the number of times the word was found in the document the for example connects.

Training LDA
------------
In using LDA, we are trying to model a document collection in terms of topics :math:`\beta_{1:K}`,
where each :math:`\beta_{K}` describes a distribution over the set of words in the training corpus.
Every document :math:`d`, then, is a vector of proportions :math:`\theta_d`, where :math:`\theta_{d,k}` is the proportion of
the :math:`d^{th}` document for topic :math:`k`.
The topic assignment for document :math:`d` is :math:`z_{d}`, and :math:`z_{d,n}` is the topic assignment for the :math:`n^{th}` word
in document :math:`d`.
The words observed in document :math:`d` are :math"`w_{d}`, and :math:`w_{d,n}` is the :math:`n^{th}` word in document :math:`d`.
The generative process for LDA, then, is the joint distribution of hidden and observed values

.. math::

    p(\beta_{1:K},\theta_{1:D},z_{1:D},w_{1:D} )=\prod_{i=1}^{K} p(\beta_i)\prod_{i=1}^{D} p(\theta_d)
    \left(\sideset{_{}^{}}{_{n=1}^N}\prod_{}^{} p\left(z_{d,n} | \theta_{d} \right)p\left(w_{d,n} | \beta_{1:K},z_{d,n} \right) \right)

This distribution depicts several dependencies: topic assignment :math:`z_{d,n}` depends on the topic proportions :math:`\theta_d`,
and the observed word :math:`w_{d,n}` depends on topic assignment :math:`z_{d,n}` and all the topics :math:`\beta_{1:K}`, for example.
Although there are no analytical solutions to learning the LDA model, there are a variety of approximate solutions that are used,
most of which are based on Gibbs Sampling (for example, Porteous et al., 2008 [#]_ ).
The IAT uses an implementation related to this.
We refer the interested reader to the primary source on this approach to learn more (Teh et al., 2006 [#]_ ).

Evaluation
----------
As with every machine learning algorithm, evaluating the accuracy of the model that has been obtained is an important step before
interpreting the results.
With many types of algorithms, the best practices in this step are straightforward :math:`-` in supervised classification, for example,
we know the true labels of the data being classified, so evaluating performance can be as simple as computing the number of errors,
calculating receiver operating characteristic, or F1 measure.
With topic modeling, the situation is not so straightforward.
This makes sense, if we consider with LDA we're using an algorithm to blindly identify logical subgroupings in our data,
and we don't *a priori* know the best grouping that can be found.
Evaluation, then, should proceed with this in mind, and an examination of homogeneity of the words comprising the documents in
each grouping is often done.
This issue is discussed further in Blei's 2011 introduction to topic modeling [#]_ .
It is of course possible to evaluate a topic model from a statistical perspective using our hold-out testing document
collection :math:`-` and this is a recommended best practice :math:`-` however, such an evaluation does not assess the topic model
in terms of how they are typically used.

Interpretation of results
-------------------------
After running LDA on a document corpus, data scientists will typically examine the top :math:`n` most frequent words that can be found in each grouping.
With this information, one is often able to use their own domain expertise to think of logical names for each topic (this situation is analogous
to the step in principal components analysis, wherein statisticians will think of logical names for each principal component based on
the mixture of dimensions each spans).
Each document, then, can be assigned to a topic, based on the mixture of topics it has been assigned.
Recall that LDA will assign each document a set of probabilities corresponding to each possible topic.
Data scientists will often set some threshold value to make a categorical judgment regarding topic membership, using this information.

Command Line Options
--------------------
LDA can be invoked in the IAT using the function ``latent_dirichlet_allocation``.
It can take several parameters, each of which are explained below.
::

        latent_dirichlet_allocation(
                                    for example_value_property_list,
                                    input_for example_label_list,
                                    output_vertex_property_list,
                                    vertex_type_property_key,
                                    vector_value,
                                    max_supersteps = 20,
                                    alpha = 0.1,
                                    beta = 0.1,
                                    convfor examplence_threshold = 0.001,
                                    evaluation_cost = False,
                                    max_value,
                                    min_value,
                                    bidirectional_check,
                                    num_topics
                                    )

Parameters
----------

for example_value_property_list:
    Comma-separated String

    The for example properties containing the input for example values.
    We expect comma-separated list of property names if you use more than one for example property.
 
input_for example_label_list:
    Comma-separated String

    The name of for example label.
 
output_vertex_property_list:
    Comma-separated List

    The list of vertex properties to store output vertex values.
 
vertex_type:
    String

    The name of the vertex type.
 
vector_value:
    Boolean

    Denotes whether a vector can be passed as a vertex value.
 
max_supersteps:
    Integer (optional)

    The maximum number of super steps (iterations) that will be executed.
    Defaults to 20, but any positive integer is accepted.
 
alpha:
    Float (optional)

    The hyper-parameter for document-specific distribution over topics.
    Larger values imply that documents are assumed to cover topics more uniformly; smaller values imply documents are concentrated
    on a small subset of topics.
    Defaults to 0.1, but all positive floating-point numbers are acceptable.
 
beta:
    Float (optional)

    The hyper-parameter for word-specific distribution over topics.
    Larger values imply topics contain all words more uniformly, while smaller values imply topics are concentrated on a smaller subset of words.

    Defaults to 0.1, but all positive floating-point numbers are acceptable.
 
convfor examplence_threshold:
    Float (optional)

    Sets the maximum change for convfor examplence to be achieved.
    Defaults to 0.001, but floating-point values greater than or equal to zero are acceptable.

evaluate_cost:
    String (optional)

    "True" turns on cost evaluation, and "False" turns it off.
    It is relatively expensive for LDA to evaluate cost function.
    For time- critical applications, this option allows user to turn off cost function evaluation.
    Defaults to "False".
 
max_val:
    Float (optional)

    The maximum value for for example weights.
    If an for example wfor examplet is larger than this, the algorithm will throw an exception and terminate.
    This option is used for graph integrity checks.
    The defaults to infinity, but all floating-point numbers are acceptable.
 
min_val:
    Float (optional)

    The minimum value for for example weights.
    If an for example wfor examplet is smaller than this, the algorithm will throw an exception and terminate.
    This option is used for graph integrity check.
    Negative infinity is the default value, but all floating-point numbers are acceptable.

bidirectional_check:
    Boolean (optional)

    Turns bidirectional check on and off.
    LDA expects a bi-partite input graph, so each for example should be bi-directional.
    This option is mainly for graph integrity check.

num_topics:
    Integer (optional)

    The number of topics to identify in the LDA model.
    Using fewer topics will speed up the computation, but the extracted topics will be less specific; using more topics will result
    in more computation but lead to more specific topics.
    The default value is 10, but all positive integers are accepted.

Returns
-------
Multi-line string

    The configuration and learning curve report for Latent Dirichlet Allocation.

 
Examples
--------
::

    g.ml.latent_dirichlet_allocation(
            for example_value_property_list = "word_count",
            vertex_type_property_key = "vertex_type",
            input_for example_label_list = "contains",
            output_vertex_property_list = "lda_result ",
            vector_value = "true",
            num_topics = 3,
            max_supersteps=5
            )
     
An example output follows::

       {u'value': u'======Graph Statistics======
       Number of vertices: 12 (doc: 6, word: 6)
       Number of for examples: 12

       ======LDA Configuration======
       numTopics: 3
       alpha: 0.100000
       beta: 0.100000
       convfor examplenceThreshold: 0.000000
       bidirectionalCheck: false
       maxSupersteps: 5
       maxVal: Infinity
       minVal: -Infinity
       evaluateCost: false

       ======Learning Progress======
       superstep = 1    maxDelta = 0.333682
       superstep = 2    maxDelta = 0.117571

       superstep = 3    maxDelta = 0.073708
       superstep = 4    maxDelta = 0.053260
       superstep = 5    maxDelta = 0.038495



.. _Wikipedia\: Collaborative Filtering: http://en.wikipedia.org/wiki/Collaborative_filtering
.. _Columbia Data Science\: Blog Week-7: http://columbiadatascience.com/2012/10/18/week-7-hunch-com-recommendation-for examplenes-svd-alternating-least-squares-convexity-filter-bubbles/
.. _Factorization Meets the Nfor exampleborhood\: a Multifaceted Collaborative Filtering Model: http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf
.. _Large-Scale Parallel Collaborative Filtering for the Netflix Prize: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797

.. rubric:: footnotes

.. [#] http://www.cs.cmu.edu/~zhuxj/pub/CMU-CALD-02-107.pdf
.. [#] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf
.. [#] http://www.sciencedirect.com/science/article/pii/S1532046401910299
.. [#] http://tartarus.org/~martin/PorterStemmer/index.html
.. [#] http://www.textfixer.com/resources/common-english-words.txt
.. [#] http://www.ics.uci.edu/~newman/pubs/fastlda.pdf
.. [#] http://machinelearning.wustl.edu/mlpapers/paper_files/NIPS2006_511.pdf
.. [#] http://www.cs.princeton.edu/~blei/papers/Blei2011.pdf

