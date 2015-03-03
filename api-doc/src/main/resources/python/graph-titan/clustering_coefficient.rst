Calculates global clustering coefficient.

Computes the :term `global clustering coefficient` of the graph and optionally
creates a new graph which is the same as the input graph, with the addition
that every vertex of the graph has its :term:`local clustering coefficient`
stored in a user-specified property.

THIS FUNCTION IS FOR UNDIRECTED GRAPHS.
If it is called on a directed graph, its output is NOT guaranteed to calculate
the local directed clustering coefficients.

Parameters
----------
output_graph_name : str (optional)
    The name of the new graph to which the results are written.
    If this option is not specified, no output graph will be produced and only
    the global clustering coefficient will be returned.

output_property_name : str (optional)
    The name of the new property to which each vertex's local clustering
    coefficient will be written.
    If this option is not specified, no output graph will be produced and only
    the global clustering coefficient will be returned.

input_edge_labels : list of str (optional)
    If this list is provided, only edges whose labels are included in the given
    set will be considered in the clustering coefficient calculation.
    In the default situation (when no list is provided), all edges will be used
    in the calculation, regardless of label.
    It is required that all edges that enter into the clustering coefficient
    analysis  be undirected.

NOTE: It is an error to specify only one of the ``output_graph_name`` and
``output_property_name``.

Returns
-------
global_clustering_coefficient : Double
    The global clustering coefficient of the graph.

Graph : graph
    A Graph is returned only if both ``output_graph_name`` and
    ``output_property_name`` are provided.
    A Graph object that is a copy of the input graph with the addition that
    every vertex of the graph has its :term:`local clustering coefficient`
    stored in a user-specified property.

Example
-------
::

    results = g.clustering_coefficient('ccgraph', 'local_clustering_coefficient')

    results
        Out[8]:
        ClusteringCoefficient:
        global_clustering_coefficient: 0.0853107962708,
        graph: TitanGraph "ccgraph"

    h = results.graph

    h.query.gremlin('g.V [ 0 .. 2]')

        Out[10]:
        {u'results': [{u'_id': 23040,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 615039,
        u'local_clustering_coefficient': 1,
        u'source': 17349,
        u'titanPhysicalId': 135912},
        {u'_id': 39424,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 400303,
        u'local_clustering_coefficient': 0,
        u'source': 27550,
        u'titanPhysicalId': 1187184},
        {u'_id': 55808,
        u'_label': u'vertex',
        u'_type': u'vertex',
        u'_vid': 673676,
        u'local_clustering_coefficient': 0,
        u'source': 29958,
        u'titanPhysicalId': 449424}],
        u'run_time_seconds': 1.756}

