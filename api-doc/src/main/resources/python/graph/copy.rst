Copy graph.

Makes a copy of the graph.

Parameters
----------
name : str (optional)
    The name for the copy of the graph.

Returns
-------
graph copy

Notes
-----
Cannot delete all columns from a frame. At least one column needs to remain.
If you want to delete all columns, then please delete the frame

Examples
--------
For this example, graph object *my_graph* accesses a graph ::

    copied_graph = my_graph.copy('my_graph2')



