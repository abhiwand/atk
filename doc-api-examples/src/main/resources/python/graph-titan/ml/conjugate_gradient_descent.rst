Personalized suggestions relyng on Collaborating Filtering.

The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering
algorithms.

CGD implementation of the algorithm presented in Y. Koren.
Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering
Model.
In ACM KDD 2008. (Equation 5)


Parameters
----------
edge_value_property_list : list of str
    The edge properties which contain the input edge values.
    A comma-separated list of property names when declaring
    more than one edge property.
input_edge_label_list : list of str
    The name of edge label.
output_vertex_property_list : list of str
    The list of vertex properties to store output vertex values.
vertex_type_property_key : str
    The name of vertex property which contains vertex type.
    Vertices must have a property to identify them as either left-side
    ("L") or right-side ("R").
edge_type_property_key : str
    The name of edge property which contains edge type.
vector_value : str (optional)
    "True" means a vector as vertex value is supported,
    "False" means a vector as vertex value is not supported.
    Default is "False".
max_supersteps : int (optional)
    The maximum number of supersteps (iterations) that the algorithm
    will execute.
    Default is 20.
convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits
    before it reaches the maximum number of supersteps.
    The valid value range is all float and zero.
    Default is 0.
cgd_lambda : float (optional)
    The tradeoff parameter that controls the strength of regularization.
    Larger value implies stronger regularization that helps prevent
    overfitting but may cause the issue of underfitting if the value is too
    large.
    The value is usually determined by cross validation (CV).
    The valid value range is all positive float and zero.
    Default is 0.065.
feature_dimension : int (optional)
    The length of feature vector to use in CGD model.
    Larger value in general results in more accurate parameter estimation,
    but slows down the computation.
    The valid value range is all positive int.
    Default is 3.
learning_curve_output_interval : int (optional)
    The learning curve output interval.
    Each CGD iteration is composed of 2 supersteps.
    Default is 1 (means two supersteps).
validate_graph_structure : bool (optional)
    Checks if the graph meets certain structural requirements before starting
    the algorithm: at every vertex, the in-degree equals the out-degree.
    This algorithm is intended for undirected graphs.
    Therefore, this is a necessary, but insufficient, check for valid input.
bias_on : bool (optional)
    True means turn on the update for bias term and False means turn off
    the update for bias term.
    Turning it on often yields more accurate model with minor performance
    penalty.
    Turning it off disables term update and treats the value of
    bias term as 0.
    Default is False.
max_value : float (optional)
    The maximum edge weight value.
    If an edge weight is larger than this value, the algorithm will throw an
    exception and terminate.
    This option is mainly for graph integrity check.
    Valid value range is all float.
    Default is Infinity.
min_value : float (optional)
    The minimum edge weight value.
    If an edge weight is smaller than this value, the algorithm will throw an
    exception and terminate.
    This option is mainly for graph integrity check.
    Valid value range is all float.
    Default is -Infinity.

Returns
-------
str
    The configuration and learning curve report for CGD in the format of a
    multiple-line string.


Notes
-----
Vertices must be identified as left-side ("L") or right-side ("R").


Examples
--------
.. only:: html

    .. code::

        >>> g.ml.conjugate_gradient_descent(edge_value_property_list = "rating", vertex_type_property_key = "vertex_type", input_edge_label_list = "edge", output_vertex_property_list = "cgd_result", edge_type_property_key = "splits", vector_value = "true", cgd_lambda = 0.065, num_iters = 3)

.. only:: latex

    .. code::

        >>> g.ml.conjugate_gradient_descent(
        ...     edge_value_property_list = "rating",
        ...     vertex_type_property_key = "vertex_type",
        ...     input_edge_label_list = "edge",
        ...     output_vertex_property_list = "cgd_result",
        ...     edge_type_property_key = "splits",
        ...     vector_value = "true",
        ...     cgd_lambda = 0.065,
        ...     num_iters = 3)

The expected output is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 20140 (left: 10070, right: 10070)\nNumber of edges: 604016 (train: 554592, validate: 49416, test: 8)\n\n======CGD Configuration======\nmaxSupersteps: 20\nfeatureDimension: 3\nlambda: 0.065000\nbiasOn: false\nconvergenceThreshold: 0.000000\nbidirectionalCheck: false\nnumCGDIters: 3\nmaxVal: Infinity\nminVal: -Infinity\nlearningCurveOutputInterval: 1\n\n======Learning Progress======\nsuperstep = 2\tcost(train) = 21828.395401\trmse(validate) = 1.317799\trmse(test) = 3.663107\nsuperstep = 4\tcost(train) = 18126.623261\trmse(validate) = 1.247019\trmse(test) = 3.565567\nsuperstep = 6\tcost(train) = 15902.042769\trmse(validate) = 1.209014\trmse(test) = 3.677774\nsuperstep = 8\tcost(train) = 14274.718100\trmse(validate) = 1.196888\trmse(test) = 3.656467\nsuperstep = 10\tcost(train) = 13226.419606\trmse(validate) = 1.189605\trmse(test) = 3.699198\nsuperstep = 12\tcost(train) = 12438.789925\trmse(validate) = 1.187416\trmse(test) = 3.653920\nsuperstep = 14\tcost(train) = 11791.454643\trmse(validate) = 1.188480\trmse(test) = 3.670579\nsuperstep = 16\tcost(train) = 11256.035422\trmse(validate) = 1.187924\trmse(test) = 3.742146\nsuperstep = 18\tcost(train) = 10758.691712\trmse(validate) = 1.189491\trmse(test) = 3.658956\nsuperstep = 20\tcost(train) = 10331.742207\trmse(validate) = 1.191606\trmse(test) = 3.757683'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 20140 (left: 10070, right: 10070)\n
        Number of edges: 604016 (train: 554592, validate: 49416, test: 8)\n
        \n
        ======CGD Configuration======\n
        maxSupersteps: 20\n
        featureDimension: 3\n
        lambda: 0.065000\n
        biasOn: false\n
        convergenceThreshold: 0.000000\n
        bidirectionalCheck: false\n
        numCGDIters: 3\n
        maxVal: Infinity\n
        minVal: -Infinity\n
        learningCurveOutputInterval: 1\n
        \n
        ======Learning Progress======\n
        superstep = 2\tcost(train) = 21828.395401\t
            rmse(validate) = 1.317799\trmse(test) = 3.663107\n
        superstep = 4\tcost(train) = 18126.623261\t
            mse(validate) = 1.247019\trmse(test) = 3.565567\n
        superstep = 6\tcost(train) = 15902.042769\t
            mse(validate) = 1.209014\trmse(test) = 3.677774\n
        superstep = 8\tcost(train) = 14274.718100\t
            mse(validate) = 1.196888\trmse(test) = 3.656467\n
        superstep = 10\tcost(train) = 13226.419606\t
            mse(validate) = 1.189605\trmse(test) = 3.699198\n
        superstep = 12\tcost(train) = 12438.789925\t
            mse(validate) = 1.187416\trmse(test) = 3.653920\n
        superstep = 14\tcost(train) = 11791.454643\t
            mse(validate) = 1.188480\trmse(test) = 3.670579\n
        superstep = 16\tcost(train) = 11256.035422\t
            mse(validate) = 1.187924\trmse(test) = 3.742146\n
        superstep = 18\tcost(train) = 10758.691712\t
            mse(validate) = 1.189491\trmse(test) = 3.658956\n
        superstep = 20\tcost(train) = 10331.742207\t
            mse(validate) = 1.191606\trmse(test) = 3.757683'}

Report may show zero edges and/or vertices if parameters were supplied
wrong, or if the graph was not the expected input:

.. code::

    ======Graph Statistics======
    Number of vertices: 12673 (left: 12673, right: 0)
    Number of edges: 0 (train: 0, validate: 0, test: 0)
