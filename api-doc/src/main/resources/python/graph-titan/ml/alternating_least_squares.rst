ALS

The Alternating Least Squares with Bias for collaborative filtering
algorithms.
The algorithms presented in:

1.  Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan.
    Large-Scale Parallel Collaborative Filtering for the Netflix Prize.
    2008.
#.  Y. Koren.
    Factorization Meets the Neighborhood: a Multifaceted Collaborative
    Filtering Model.
    In ACM KDD 2008. (Equation 5)

Parameters
----------
edge_value_property_list : comma-separated string
    The edge properties which contain the input edge values.
    We expect comma-separated list of property names if you use
    more than one edge property.

input_edge_label_list : comma-separated string
    Name of edge label

output_vertex_property_list : comma-separated string
    The list of vertex properties to store output vertex values

vertex_type_property_key : string
    The name of vertex property which contains vertex type.
    Vertices must have a property to identify them as either left-side
    ("L") or right-side ("R").

edge_type_property_key : string
    The name of edge property which contains edge type

vector_value : string (optional)
    True means a vector as vertex value is supported,
    False means a vector as vertex value is not supported.
    The default value is False.

max_supersteps : integer, (optional)
    The maximum number of super steps (iterations) that the algorithm will
    execute.
    The default value is 20.

convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of super steps.
    The valid value range is all float and zero.
    The default value is 0.

als_lambda : float (optional)
    The tradeoff parameter that controls the strength of regularization.
    Larger value implies stronger regularization that helps prevent
    overfitting but may cause the issue of underfitting if the value is
    too large.
    The value is usually determined by cross validation (CV).
    The valid value range is all positive float and zero.
    The default value is 0.065.

feature_dimension : integer (optional)
    The length of feature vector to use in ALS model.
    Larger value in general results in more accurate parameter estimation,
    but slows down the computation.
    The valid value range is all positive integer.
    The default value is 3.

learning_curve_output_interval : integer (optional)
    The learning curve output interval.
    Since each ALS iteration is composed by 2 super steps,
    the default one iteration means two super steps.

validate_graph_structure : boolean (optional)
    Checks if the graph meets certain structural requirements before starting
    the algorithm.

    At present, this checks that at every vertex, the in-degree equals the
    out-degree. Because ALS expects an undirected graph, this is a necessary
    but not sufficient condition.

bias_on : boolean (optional)
    True means turn on the update for bias term and False means turn off
    the update for bias term.
    Turning it on often yields more accurate model with minor performance
    penalty; turning it off disables term update and leaves the value of
    bias term to be zero.
    The default value is False.

max_value : float (optional)
    The maximum edge weight value.
    If an edge weight is larger than this
    value, the algorithm will throw an exception and terminate.
    This optioni is mainly for graph integrity check.
    Valid value range is all float.
    The default value is Infinity.

min_value : float (optional)
    The minimum edge weight value.
    If an edge weight is smaller than this value,
    the algorithm will throw an exception and terminate.
    This option is mainly for graph integrity check.
    Valid value range is all float.
    The default value is -Infinity.

Returns
-------
Multiple line string
    The configuration and learning curve report for ALS

Notes
-----
Vertices must be identified as left-side ("L") or right-side ("R").
See vertex rules.

Examples
--------
.. only:: html

    For example, if your left-side vertices are users, and you want to get
    a movie recommendation for user 1, the command to use is::

        g.ml.alternating_least_squares(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "als_result", edge_type_property_key = "splits", vector_value = "true", als_lambda = 0.065, bias_on = False, min_value = 1, max_value = 5)::

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 10070 (left: 9569, right: 501)\\nNumber of edges: 302008 (train: 145182, validate: 96640, test: 60186)\\n\\n======ALS Configuration======\\nmaxSupersteps: 20\\nfeatureDimension: 3\\nlambda: 0.065000\\nbiasOn: False\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: False\\nmaxVal: 5.000000\\nminVal: 1.000000\\nlearningCurveOutputInterval: 1\\n\\n======Learning Progress======\\nsuperstep = 2\\tcost(train) = 838.720244\\trmse(validate) = 1.220795\\trmse(test) = 1.226830\\nsuperstep = 4\\tcost(train) = 608.088979\\trmse(validate) = 1.174247\\trmse(test) = 1.180558\\nsuperstep = 6\\tcost(train) = 540.071050\\trmse(validate) = 1.166471\\trmse(test) = 1.172131\\nsuperstep = 8\\tcost(train) = 499.134869\\trmse(validate) = 1.164236\\trmse(test) = 1.169805\\nsuperstep = 10\\tcost(train) = 471.318913\\trmse(validate) = 1.163796\\trmse(test) = 1.169215\\nsuperstep = 12\\tcost(train) = 450.420300\\trmse(validate) = 1.163993\\trmse(test) = 1.169224\\nsuperstep = 14\\tcost(train) = 433.511180\\trmse(validate) = 1.164485\\trmse(test) = 1.169393\\nsuperstep = 16\\tcost(train) = 419.403410\\trmse(validate) = 1.165008\\trmse(test) = 1.169507\\nsuperstep = 18\\tcost(train) = 407.212140\\trmse(validate) = 1.165425\\trmse(test) = 1.169503\\nsuperstep = 20\\tcost(train) = 396.281966\\trmse(validate) = 1.165723\\trmse(test) = 1.169451'}::

.. only:: latex

    For example, if your left-side vertices are users, and you want to get
    a movie recommendation for user 1, the command to use is::

        g.ml.alternating_least_squares(
            edge_value_property_list = "rating",
            vertex_type_property_key = "vertex_type",
            input_edge_label_list = "edge",
            output_vertex_property_list = "als_result",
            edge_type_property_key = "splits",
            vector_value = "true",
            als_lambda = 0.065,
            bias_on = False,
            min_value = 1,
            max_value = 5)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\n
        Number of vertices: 10070 (left: 9569, right: 501)\\n
        Number of edges: 302008 (train: 145182, validate: 96640, test: 60186)\\n
        \\n
        ======ALS Configuration======\\n
        maxSupersteps: 20\\n
        featureDimension: 3\\n
        lambda: 0.065000\\n
        biasOn: False\\n
        convergenceThreshold: 0.000000\\n
        bidirectionalCheck: False\\n
        maxVal: 5.000000\\n
        minVal: 1.000000\\n
        learningCurveOutputInterval: 1\\n
        \\n
        ======Learning Progress======\\n
        superstep = 2\\t
            cost(train) = 838.720244\\t
            rmse(validate) = 1.220795\\t
            rmse(test) = 1.226830\\n
        superstep = 4\\t
            cost(train) = 608.088979\\t
            rmse(validate) = 1.174247\\t
            rmse(test) = 1.180558\\n
        superstep = 6\\t
            cost(train) = 540.071050\\t
            rmse(validate) = 1.166471\\t
            rmse(test) = 1.172131\\n
        superstep = 8\\t
            cost(train) = 499.134869\\t
            rmse(validate) = 1.164236\\t
            rmse(test) = 1.169805\\n
        superstep = 10\\t
            cost(train) = 471.318913\\t
            rmse(validate) = 1.163796\\t
            rmse(test) = 1.169215\\n
        superstep = 12\\t
            cost(train) = 450.420300\\t
            rmse(validate) = 1.163993\\t
            rmse(test) = 1.169224\\n
        superstep = 14\\t
            cost(train) = 433.511180\\t
            rmse(validate) = 1.164485\\t
            rmse(test) = 1.169393\\n
        superstep = 16\\t
            cost(train) = 419.403410\\t
            rmse(validate) = 1.165008\\t
            rmse(test) = 1.169507\\n
        superstep = 18\\t
            cost(train) = 407.212140\\t
            rmse(validate) = 1.165425\\t
            rmse(test) = 1.169503\\n
        superstep = 20\\t
            cost(train) = 396.281966\\t
            rmse(validate) = 1.165723\\t
            rmse(test) = 1.169451'}

    Report may show zero edges and/or vertices if parameters were supplied wrong, or if the graph was not the expected input::

        ======Graph Statistics======
        Number of vertices: 12673 (left: 12673, right: 0)
        Number of edges: 0 (train: 0, validate: 0, test: 0)

