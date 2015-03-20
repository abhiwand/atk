New graph with weight added to vertices.

Creates a new graph which is the same as the input graph, with the addition
that every vertex of the graph has its weighted :term:`degree` stored in a
user-specified property.
This is defined as follows: Each edge has a weight (that comes from the
user-specified property).
The weighted degree of a vertex is the sum of the weight of the edges incident
to that vertex.

Parameters
----------
output_graph_name : str
    The name of the new graph.
    Results are written to this graph.

output_property_name : str
    The name of the new property.
    The weighted degree is stored in this property.

degree_option : str (optional)
    Indicator for the definition of degree to be used for the calculation.
    Permitted values:

    *   "out" : Degree is calculated as the out-degree.
    *   "in" : Degree is calculated as the in-degree.
    *   "undirected" : Degree is calculated as the undirected degree.
        (Assumes that the edges are all undirected.)

    Any string with a prefix of the strings "out", "in", "undirected" will
    select the corresponding option.
    Default is "out".

input_edge_labels : list of str (optional)
    If this list is provided, only edges whose labels are included in the given
    set will be considered in the degree calculation.
    In the default situation (when no list is provided), all edges will be used
    in the degree calculation, regardless of label.

edge_weight_property : str (optional)
    The name of the edge property that contains the weights of edges.
    Default is 1.0D.

edge_weight_default : double (optional)
    Default weight to use for an edge if the edge does not possess a property
    of key edge_weight_property.
    Default is 1.0D.

Returns
-------
Graph : Weighted degree graph
    A graph object that is a copy of the input graph with the addition that
    every vertex of the graph has its weighted :term:`degree` stored in a
    user-specified property.

Examples
--------
Given a directed graph with three nodes and two edges like this:

.. only:: html

    .. code::

        >>> g.query.gremlin('g.V')
            Out[23]:
            {u'results': [{u'_id': 28304,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 4,
             u'source': 2},
            {u'_id': 21152,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 1,
             u'source': 1},
            {u'_id': 28064,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 3,
             u'source': 3}],
             u'run_time_seconds': 1.245}
 
        >>> g.query.gremlin('g.E')
            Out[24]:
            {u'results': [{u'_eid': 3,
             u'_id': u'34k-gbk-bth-lnk',
             u'_inV': 28064,
             u'_label': u'edge',
             u'_outV': 21152,
             u'_type': u'edge',
             u'weight': 0.01},
            {u'_eid': 4,
             u'_id': u'1xw-gbk-bth-lu8',
             u'_inV': 28304,
             u'_label': u'edge',
             u'_outV': 21152,
             u'_type': u'edge',
             u'weight': 0.1}],
             u'run_time_seconds': 1.359}
 
        >>> h = g.annotate_weighted_degrees('new_graph', 'weight',  edge_weight_property = 'weight')
 
        >>> h.query.gremlin('g.V')
            Out[26]:
            {u'results': [{u'_id': 24112,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 4,
             u'source': 2,
             u'titanPhysicalId': 28304,
             u'weight': 0},
            {u'_id': 17648,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 1,
             u'source': 1,
             u'titanPhysicalId': 21152,
             u'weight': 0.11},
            {u'_id': 30568,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 3,
             u'source': 3,
             u'titanPhysicalId': 28064,
             u'weight': 0}],
             u'run_time_seconds': 1.326}

.. only:: latex

    .. code::

        >>> g.query.gremlin('g.V')
            Out[23]:
            {u'results': [{u'_id': 28304,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 4,
             u'source': 2},
            {u'_id': 21152,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 1,
             u'source': 1},
            {u'_id': 28064,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 3,
             u'source': 3}],
             u'run_time_seconds': 1.245}
 
        >>> g.query.gremlin('g.E')
            Out[24]:
            {u'results': [{u'_eid': 3,
             u'_id': u'34k-gbk-bth-lnk',
             u'_inV': 28064,
             u'_label': u'edge',
             u'_outV': 21152,
             u'_type': u'edge',
             u'weight': 0.01},
            {u'_eid': 4,
             u'_id': u'1xw-gbk-bth-lu8',
             u'_inV': 28304,
             u'_label': u'edge',
             u'_outV': 21152,
             u'_type': u'edge',
             u'weight': 0.1}],
             u'run_time_seconds': 1.359}
 
        >>> h = g.annotate_weighted_degrees(
        ...        'new_graph',
        ...        'weight',
        ...        edge_weight_property = 'weight')
 
        >>> h.query.gremlin('g.V')
            Out[26]:
            {u'results': [{u'_id': 24112,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 4,
             u'source': 2,
             u'titanPhysicalId': 28304,
             u'weight': 0},
            {u'_id': 17648,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 1,
             u'source': 1,
             u'titanPhysicalId': 21152,
             u'weight': 0.11},
            {u'_id': 30568,
             u'_label': u'vertex',
             u'_type': u'vertex',
             u'_vid': 3,
             u'source': 3,
             u'titanPhysicalId': 28064,
             u'weight': 0}],
             u'run_time_seconds': 1.326}

