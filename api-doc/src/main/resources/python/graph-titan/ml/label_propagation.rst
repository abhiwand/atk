Label Propagation on Gaussian Random Fields.

This algorithm is presented in X. Zhu and Z. Ghahramani.
Learning from labeled and unlabeled data with label propagation.
Technical Report CMU-CALD-02-107, CMU, 2002.

Parameters
----------
vertex_value_property_list : list of string
    The vertex properties which contain prior vertex values if you use more
    than one vertex property.

edge_value_property_list : list of string
    The edge properties which contain the input edge values.
    We expect comma-separated list of property names if you use more than
    one edge property.

input_edge_label_list : list of string
    The name of edge label

output_vertex_property_list : list of string
    The list of vertex properties to store output vertex values

vector_value : boolean
    True means a vector as vertex value is supported,
    False means a vector as vertex value is not supported

max_supersteps : integer (optional)
    The maximum number of super steps that the algorithm will execute.
    The valid value range is all positive integer.
    The default value is 10.

convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of super steps.
    The valid value range is all float and zero.
    The default value is 0.001.

anchor_threshold : float (optional)
    The parameter that determines if a node's initial prediction from
    external classifier will be updated or not.
    If a node's maximum initial prediction value is greater than this
    threshold, the node will be treated as anchor node, whose final
    prediction will inherit from prior without update.
    This is for the case where we have confident initial predictions on some
    nodes and don't want the algorithm updates those nodes.
    The valid value range is [0, 1].
    The default value is 1.0

lp_lambda : float (optional)
    The tradeoff parameter that controls much influence of external
    classifier's prediction contribution to the final prediction.
    This is for the case where an external classifier is available that can
    produce initial probabilistic classification on unlabled examples, and
    the option allows incorporating external classifier's prediction into
    the LP training process.
    The valid value range is [0.0,1.0].
    The default value is 0.

validate_graph_structure : boolean (optional)
    Checks if the graph meets certain structural requirements before starting
    the algorithm.

    At present, this checks that at every vertex, the in-degree equals the
    out-degree. Because this algorithm is for undirected graphs, this is a
    necessary but not sufficient, check for valid input.

Returns
-------
Multiple line string
    The configuration and learning curve report for Label Propagation

Examples
--------
.. only:: html

    ::

        g.ml.label_propagation(vertex_value_property_list = "input_value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lp_posterior",   vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, lp_lambda = 0.5, bidirectional_check = False)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 600\\nNumber of edges: 15716\\n\\n======LP Configuration======\\nlambda: 0.000000\\nanchorThreshold: 0.900000\\nconvergenceThreshold: 0.000000\\nmaxSupersteps: 10\\nbidirectionalCheck: false\\n\\n======Learning Progress======\\nsuperstep = 1\\tcost = 0.008692\\nsuperstep = 2\\tcost = 0.008155\\nsuperstep = 3\\tcost = 0.007809\\nsuperstep = 4\\tcost = 0.007544\\nsuperstep = 5\\tcost = 0.007328\\nsuperstep = 6\\tcost = 0.007142\\nsuperstep = 7\\tcost = 0.006979\\nsuperstep = 8\\tcost = 0.006833\\nsuperstep = 9\\tcost = 0.006701\\nsuperstep = 10\\tcost = 0.006580'}

.. only:: latex

    ::

        g.ml.label_propagation(vertex_value_property_list = "input_value", \\
        edge_value_property_list  = "weight", input_edge_label_list = "edge", \\
        output_vertex_property_list = "lp_posterior",   vector_value = "true", \\
        max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, \\
        lp_lambda = 0.5, bidirectional_check = False)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\n
        Number of vertices: 600\\n
        Number of edges: 15716\\n
        \\n
        ======LP Configuration======\\n
        lambda: 0.000000\\n
        anchorThreshold: 0.900000\\n
        convergenceThreshold: 0.000000\\n
        maxSupersteps: 10\\n
        bidirectionalCheck: false\\n
        \\n
        ======Learning Progress======\\n
        superstep = 1\\tcost = 0.008692\\n
        superstep = 2\\tcost = 0.008155\\n
        superstep = 3\\tcost = 0.007809\\n
        superstep = 4\\tcost = 0.007544\\n
        superstep = 5\\tcost = 0.007328\\n
        superstep = 6\\tcost = 0.007142\\n
        superstep = 7\\tcost = 0.006979\\n
        superstep = 8\\tcost = 0.006833\\n
        superstep = 9\\tcost = 0.006701\\n
        superstep = 10\\tcost = 0.006580'}

