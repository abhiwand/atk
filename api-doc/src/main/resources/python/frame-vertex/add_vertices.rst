Add vertices to a graph.

Includes appending to a list of existing vertices.

Parameters
----------
source_frame : Frame
    frame that will be the source of the vertex data
id_column_name : str
    column name for a unique id for each vertex
column_names : list of str
    column names that will be turned into properties for each vertex

Examples
--------

.. code::

    >>> graph = ia.Graph()
    >>> graph.define_vertex_type('users')
    >>> graph.vertices['users'].add_vertices(frame, 'user_id', ['user_name', 'age'])


