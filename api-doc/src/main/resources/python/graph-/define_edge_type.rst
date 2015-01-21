Define edge type.

Define an edge type.

Parameters
----------
label : str
    label of the edge type

source_vertex_label : str
    label of the source vertex type

destination_vertex_label : str
    label of the destination vertex type

directed : bool
    is the edge directed

Examples
--------
::

    graph = ia.Graph()
    graph.define_vertex_type('users')
    graph.define_vertex_type('movie')
    graph.define_edge_type('ratings', 'users', 'movie', directed=True)


