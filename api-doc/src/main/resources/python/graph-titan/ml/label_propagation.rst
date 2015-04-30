Label Propagation on Gaussian Random Fields.

This algorithm is presented in X. Zhu and Z. Ghahramani.
Learning from labeled and unlabeled data with label propagation.
Technical Report CMU-CALD-02-107, CMU, 2002.


Parameters
----------
prior_vector: list of str
    The vertex properties which contain prior vertex values when more than one
    vertex property is used.
distance : list of str
    The edge properties which contain the input edge values.
    A comma-separated list of property names when more than one edge property
    is used.
lp_result : list of str
    The list of vertex properties to store output vertex values.
max_supersteps : int (optional)
    The maximum number of supersteps that the algorithm will execute.
    The valid value range is all positive int.
    The default value is 10.
convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of supersteps.
    The valid value range is all float and zero.
    The default value is 0.001.
anchor_threshold : float (optional)
    The parameter that determines if a node's initial prediction from
    external classifier will be updated or not.
    If a node's maximum initial prediction value is greater than this
    threshold, the node will be treated as anchor node, whose final
    prediction will inherit from prior without update.
    This is for the case where there is confident initial predictions on some
    nodes and it is desirable that the algorithm does not update those nodes.
    The valid value range is [0, 1].
    The default value is 1.0.
lp_lambda : float (optional)
    The tradeoff parameter that controls how much influence an external
    classifier's prediction contributes to the final prediction.
    This is for the case where an external classifier is available that can
    produce initial probabilistic classification on unlabled examples, and
    the option allows incorporating external classifier's prediction into
    the LP training process.
    The valid value range is [0.0,1.0].
    The default value is 0.

Returns
-------
str
    The configuration and learning curve report for Label Propagation in the format of a multiple-line string.


Examples
--------
.. only:: html

    .. code::

        >>> g.ml.label_propagation(vertex_value_property_list = "input_value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lp_posterior",   vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, lp_lambda = 0.5, bidirectional_check = False)

.. only:: latex

    .. code::

        >>> g.ml.label_propagation(
        ... vertex_value_property_list = "input_value",
        ... edge_value_property_list  = "weight",
        ... input_edge_label_list = "edge",
        ... output_vertex_property_list = "lp_posterior",
        ... vector_value = "true",
        ... max_supersteps = 10,
        ... convergence_threshold = 0.0,
        ... anchor_threshold = 0.9,
        ... lp_lambda = 0.5,
        ... bidirectional_check = False)


The expected output is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 600\nNumber of edges: 15716\n\n======LP Configuration======\nlambda: 0.000000\nanchorThreshold: 0.900000\nconvergenceThreshold: 0.000000\nmaxSupersteps: 10\nbidirectionalCheck: false\n\n======Learning Progress======\nsuperstep = 1\tcost = 0.008692\nsuperstep = 2\tcost = 0.008155\nsuperstep = 3\tcost = 0.007809\nsuperstep = 4\tcost = 0.007544\nsuperstep = 5\tcost = 0.007328\nsuperstep = 6\tcost = 0.007142\nsuperstep = 7\tcost = 0.006979\nsuperstep = 8\tcost = 0.006833\nsuperstep = 9\tcost = 0.006701\nsuperstep = 10\tcost = 0.006580'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 600\n
        Number of edges: 15716\n
        \n
        ======LP Configuration======\n
        lambda: 0.000000\n
        anchorThreshold: 0.900000\n
        convergenceThreshold: 0.000000\n
        maxSupersteps: 10\n
        bidirectionalCheck: false\n
        \n
        ======Learning Progress======\n
        superstep = 1\tcost = 0.008692\n
        superstep = 2\tcost = 0.008155\n
        superstep = 3\tcost = 0.007809\n
        superstep = 4\tcost = 0.007544\n
        superstep = 5\tcost = 0.007328\n
        superstep = 6\tcost = 0.007142\n
        superstep = 7\tcost = 0.006979\n
        superstep = 8\tcost = 0.006833\n
        superstep = 9\tcost = 0.006701\n
        superstep = 10\tcost = 0.006580'}

