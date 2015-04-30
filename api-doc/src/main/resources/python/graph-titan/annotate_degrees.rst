Make new graph with degrees.

Creates a new graph which is the same as the input graph, with the addition
that every vertex of the graph has its :term:`degree` stored in a
user-specified property.


Parameters
----------
output_property_name : str
    The name of the new property.
    The degree is stored in this property.
degree_option : str (optional)
    Indicator for the definition of degree to be used for the calculation.
    Permitted values:

    *   "out" (default value) : Degree is calculated as the out-degree.
    *   "in" : Degree is calculated as the in-degree.
    *   "undirected" : Degree is calculated as the undirected degree.
        (Assumes that the edges are all undirected.)

    Any prefix of the strings "out", "in", "undirected" will select the
    corresponding option.
input_edge_labels : list of str (optional)
    If this list is provided, only edges whose labels are included in the given
    set will be considered in the degree calculation.
    In the default situation (when no list is provided), all edges will be used
    in the degree calculation, regardless of label.


Returns
-------
dict
    Dictionary containing the vertex type as the key and the corresponding
    vertex's frame with a column storing the annotated degree for the vertex
    in a user specified property.
    Call dictionary_name['label'] to get the handle to frame whose vertex type
    is label.


Examples
--------
Given a graph:

.. code::

    >>> g.query.gremlin('g.V [ 0 .. 1]')

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

    >>> h = g.annotate_degrees('degree')

