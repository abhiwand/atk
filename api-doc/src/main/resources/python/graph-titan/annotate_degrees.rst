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
    The return value is a dictionary containing one key value pair.

    graph : string
        The name of a graph object that is a copy of the input graph with the addition that every vertex of the graph
        has its :term:`degree` stored in a user-specified property.

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
