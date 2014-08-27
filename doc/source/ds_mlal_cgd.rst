Conjugate Gradient Descent (CGD)
================================

See: http://en.wikipedia.org/wiki/Conjugate_gradient_method.

The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering algorithm.

Our implementation is based on the following paper.

Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model. In ACM KDD 2008. (Equation 5)
http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

This algorithm for collaborative filtering is used in :term:`recommendation systems` to suggest items (products, movies, articles, and so on) to potential users based on historical records of items that all users have purchased, rated, or viewed.
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

    Required parameters:

    input_edge_property_list : List (comma-separated list of strings)
        The edge properties which contain the input edge values.
        If you use more than one edge property.
        We expect a comma-separated string list.

    input_edge_label : String
        The edge property which contains the edge label.

    output_vertex_property_list : List (comma-separated list of strings)
        The vertex properties which contain the output vertex values.
        If you use more than one vertex property, we expect a
        comma-separated string list.

    vertex_type : String
        The vertex property which contains the vertex type.

    edge_type : String
        The edge property which contains the edge type.

    num_mapper : String, optional
        A reconfigured Hadoop parameter mapred.tasktracker.map.tasks.maximum, 
        use on the fly when needed for your data sets.

    mapper_memory : String, optional
        A reconfigured Hadoop parameter mapred.map.child.java.opts,
        use on the fly when needed for your data sets.

    vector_value: String, optional
        "True" means the algorithm supports a vector as a vertex value.
        "False" means the algorithm does not support a vector as a vertex value.

    num_worker : String, optional
        The number of Giraph workers.
        The default value is 15.

    max_supersteps :  String, optional
        The number of super steps to run in Giraph.
        The default value is 10.

    feature_dimension : String, optional
        The feature dimension.
        The default value is 3.

    cgd_lambda : String, optional
        The regularization parameter: 
        f = L2_error + lambda*Tikhonov_regularization
        The default value is 0.065.

    convergence_threshold : String, optional
        The convergence threshold which controls how small the change in validation 
        error must be in order to meet the convergence criteria.
        The default value is 0.

    learning_output_interval : String, optional
        The learning curve output interval.
        The default value is 1.
        Because each CGD iteration is composed by 2 super steps, the default one 
        iteration means two super steps.

    max_val : String, optional
        The maximum edge weight value.
        The default value is Float.POSITIVE_INFINITY.

    min_val : String, optional
        The minimum edge weight value.
        The default value is Float.NEGATIVE_INFINITY.

    bias_on : String, optional
        True means turn on bias calculation and False means turn off bias calculation.
        The default value is false.

    bidirectional_check : String, optional
        If it is true, Giraph will check whether each edge is bidirectional.
            The default value is "False".

    num_iters : 
        The number of CGD iterations in each super step.
        The default value is 5.

    After execution, the algorithm's results are stored in database.
    The convergence curve is accessible through the report object.
    
    Example

    Graph.ml.cgd(
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
                num_iters="3"
                )


