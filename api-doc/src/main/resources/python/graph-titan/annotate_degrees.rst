    Annotate Degrees.

    Extended Summary
    ----------------
    Creates a new graph which is the same as the input graph, with the addition that every vertex of the graph
    has its :term:`degree` stored in a user-specified property.

    If the user specifies a property containing edge weights, the weighted :term `degree` is calculated.
    This is defined as follows: Each edge has a weight (that comes from the user-specified property).
    The weighted degree of a vertex is the sum of the weight of the edges incident to that vertex.

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
        If this list is provided, only edges whose labels are included in the given set will be consided in the degree
        calculation. In the default situation (when no list is provided), all edges will be used in the degree
        calculation, regardless of label.

    edge_weight_property : str (optional)
        The name of the edge property that contains the weights of edges.
        If this field is not provided, the calculation defaults to unweighted degree: All edges have weight 1.

    edge_weight_default : double (optional)
        Default weight to use for an edge if the edge does not possess a property of key edge_weight_property.
        Only used if edge_weight_property is provided and the calculation is therefore weighted.

    Returns
    -------
    graph : graph
        a graph object that is a copy of the input graph with the addition that every vertex of the graph
        has its (weighted) :term:`degree` stored in a user-specified property.

    Examples
    --------
    Suppose you have a graph like this:

    >>> gt.query.gremlin("g.V [0..2]")
    {u'results': [{u'_vid': 4, u'source': 3, u'_type': u'vertex', u'_id': 30208, u'_label': u'vertex'},
     {u'_vid': 3, u'source': 2, u'_type': u'vertex', u'_id': 19992, u'_label': u'vertex'},
      {u'_vid': 1, u'source': 1, u'_type': u'vertex', u'_id': 23384, u'_label': u'vertex'}],
       u'run_time_seconds': 2.165}

    You can calculate its in-degrees as follows:
    >>> gt.annotate_degrees("dt_indegrees", "in_degree", "i")
    {u'graph': u'dt_indegrees'}

    And check out the newly annotated in-degrees as:
    >>> t = ia.get_graph('dt_indegrees')
    >>> t.query.gremlin("g.V [0..2]")

    {u'results': [ {u'_label': u'vertex', u'_type': u'vertex', u'source': 2, u'in_degree': 1, u'titanPhysicalId': 19992,
                    u'_vid': 3, u'_id': 16928}, {u'_label': u'vertex', u'_type': u'vertex', u'source': 3,
                    u'in_degree': 1, u'titanPhysicalId': 30208, u'_vid': 4, u'_id': 32640},
                    {u'_label': u'vertex', u'_type': u'vertex', u'source': 1, u'in_degree': 0,
                    u'titanPhysicalId': 23384, u'_vid': 1, u'_id': 18872}],
                     u'run_time_seconds': 2.16}

        .. versionadded:: 1.0
