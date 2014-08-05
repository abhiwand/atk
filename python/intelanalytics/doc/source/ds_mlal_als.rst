Alternating Least Squares (ALS)
===============================

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

To import the ALS input data, use the following iPython calls::

    from intelanalytics.table.bigdataframe import get_frame_builder
    fb = get_frame_builder()
    csvfile = '/user/hadoop/recommendation_raw_input.csv'
    frame = fb.build_from_csv('AlsFrame',
                              csvfile,
                              schema='/user:long,vertex_type:chararray,movie:long,rating:logn.splits:chararray',
                              overwrite=True)

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

    from intelanalytics.graph.giggraph import get_graph_builder, GraphTypes
    gb = get_graph_builder(GraphTypes.Property, frame)
    gb.register_vertex('user', ['vertex_type'])
    gb.register_vertex('movie')
    gb.register_edge(('user', 'movie', 'rates'), ['splits', 'rating'])
    graph = gb.build("AlsGraph", overwrite=True)

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

    report1 = graph.ml.als(
                input_edge_property_list="rating",

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

    Required Parameters:

    input_edge_property_list : List (comma-separated list of strings)
        The edge properties which contain the input edge 
        values. If you use more than one edge property, we expect a 
        comma-separated string list.

    input_edge_label : String
        The edge property which contains the edge label.

    output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex 
        values. If you use more than one vertex property, we expect a 
        comma-separated string list.

    vertex_type : String
        The vertex property which contains the vertex type.

    edge_type : String
        The edge property which contains edge type.

    num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum.
        Use on the fly when needed for your data sets.

    mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts.
        Use on the fly when needed for your data sets.

    vector_value : String, optional
        "True" means the algorithm supports a vector as a vertex value.
        "False" means the algorithm does not support a vector as a vertex value.

    num_worker : String, optional
        The number of Giraph workers.
        The default value is 15.

    max_supersteps : String, optional
        The number of super steps to run in Giraph.
        The default value is 10.

    feature_dimension : String, optional
        The feature dimension.
        The default value is 3.

    als_lambda : String, optional
        The regularization parameter:
        f = L2_error + lambda*Tikhonov_regularization
        The default value is 0.065.

    convergence_threshold : String, optional
        The convergence threshold which controls how small the change in 
        validation error must be in order to meet the convergence criteria.
        The default value is 0.

    learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each ALS iteration is composed of 2 super steps, the default 
        one iteration means two super steps.

    max_val : String, optional
        The maximum edge weight value.
        The default value is Float.POSITIVE_INFINITY.

    min_val : String, optional
        The minimum edge weight value.
        The default value is Float.NEGATIVE_INFINITY.

    bidirectional_check : String, optional
        If it is true, Giraph will check whether each edge is bidirectional.
            The default value is "False".

    bias_on : String, optional
        True means turn bias calculation on, and False means turn bias calculation off.
        The default value is false.

Returns

    output : AlgorithmReport

    After execution, the algorithm's results are stored in the database.
    The convergence curve is accessible through the report object.

For a more complete definition of the Lambda parameter, see :term:`Lambda`.

Example


    Graph.ml.als(
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


