Add edges to a graph.

Includes appending to a list of existing edges.

Parameters
----------
source_frame : Frame
    frame that will be the source of the edge data

column_name_for_src_vertex_id : str
    column name for a unique id for each source vertex (this is not the system
    defined _vid)

column_name_for_dest_vertex_id : str
    column name for a unique id for each destination vertex (this is not the
    system defined _vid)

column_names : list of str
    column names that will be turned into properties for each edge

create_missing_vertices : Boolean (optional)
    True to create missing vertices for edge (slightly slower), False to drop
    edges pointing to missing vertices.
    Defaults to False.

Examples
--------
Create a frame and add edges:

.. only:: html

    .. code::

        >>> graph = ia.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.define_vertex_type('movie')
        >>> graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        >>> graph.add_edges(frame, 'user_id', 'movie_id', ['rating'], create_missing_vertices=True)

.. only:: latex

    .. code::

        >>> graph = ia.Graph()
        >>> graph.define_vertex_type('users')
        >>> graph.define_vertex_type('movie')
        >>> graph.define_edge_type('ratings', 'users', 'movies', directed=True)
        >>> graph.add_edges(frame, 'user_id', 'movie_id', ['rating'], \
        ...     create_missing_vertices=True)


