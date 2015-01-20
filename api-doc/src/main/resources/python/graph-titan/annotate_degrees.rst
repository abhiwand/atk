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
        input_edge_labels : list of str


    Returns
    -------
    graph : graph
        a graph object that is a copy of the input graph with the addition that every vertex of the graph
        has its :term:`degree` stored in a user-specified property.

    Examples
    --------
    Quit your whining. It's obvious.

    .. versionadded:: 1.0
