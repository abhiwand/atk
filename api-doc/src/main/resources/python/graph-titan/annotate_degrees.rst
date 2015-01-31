    Annotate Degrees.

    Extended Summary
    ----------------
    Creates a new graph which is the same as the input graph, with the addition that every vertex of the graph
    has its :term:`degree` stored in a user-specified property.

    Parameters
    ----------
    output_graph_name : str
        The name of the new graph to which the results are written.
    output_property_name : str
        The name of the new property to which the
    degree_option : str (optional)
        Indicator for the definition of degree to be used for the calculation.
        Legal values:
            "out" (default value) : Degree is calculated as the out-degree.
            "in" : Degree is calculated as the in-degree.
            "undirected" : Degree is calculated as the undirected degree. (Assumes that the edges are all undirected.)
            (Any prefix of the strings "out", "in", "undirected" will select the corresponding option.)
    input_edge_labels : list of str (optional)
        If this list is provided, only edges whose labels are included in the given set will be considered in the degree
        calculation. In the default situation (when no list is provided), all edges will be used in the degree
        calculation, regardless of label.


    Return
    ------

    graph : BigGraph
        A graph that is a copy of the input graph with the addition that every vertex of the graph
        has its :term:`degree` stored in a user-specified property.

    Examples
    --------
    Suppose you have a graph like this:

    g.query.gremlin('g.V [ 0 .. 1]')

        Out[12]:
        {u'results': [{u'_id': 19456,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 545413,
        u'source': 6961},
        {u'_id': 19968,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 511316,
        u'source': 31599}],
        u'run_time_seconds': 1.822}

    h = g.annotate_degrees('degreed_graph', 'degree')

    h.query.gremlin('g.V [ 0 .. 1]')

        Out[14]:
        {u'results': [{u'_id': 16384,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 594142,
        u'degree': 3,
        u'source': 35562,
        u'titanPhysicalId': 169968},
        {u'_id': 25088,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 676474,
        u'degree': 1,
        u'source': 31035,
        u'titanPhysicalId': 292368}],
        u'run_time_seconds': 1.428}


        .. versionadded:: 1.0
