Annotate Degrees.

Extended Summary
----------------
Computes the :term `global clustering coefficient` of the graph and optionally
creates a new graph which is the same as the input graph, with the addition that every vertex of the graph
has its :term:`local clustering coefficient` stored in a user-specified property.

Parameters
----------
output_graph_name : str (optional)
    The name of the new graph to which the results are written.
     If this option is not specified, no output graph will be produced and only the global clustering coefficient will
    be returned.
output_property_name : str (optional)
    The name of the new property to which each vertex's local clustering coefficient will be written.
    If this option is not specified, no output graph will be produced and only the global clustering coefficient will
    be returned.
input_edge_labels : list of str (optional)
    If this list is provided, only edges whose labels are included in the given set will be considered in the clustering
    coefficient calculation. In the default situation (when no list is provided), all edges will be used in the
    calculation, regardless of label.
    It is required that all edges that enter into the clustering coefficient analysis  be undirected.

NOTE: It is an error to specify only one of the output_graph_name and output_property_name.

Returns
-------
global_clustering_coefficient : Double
    The global clustering coefficient of the graph.

graph : graph (returned only if both output_graph_name and output_property_name are provided)
    a graph object that is a copy of the input graph with the addition that every vertex of the graph
    has its :term:`local clustering coefficient` stored in a user-specified property.

Examples
--------
Coming soon...

    .. versionadded:: 1.0
