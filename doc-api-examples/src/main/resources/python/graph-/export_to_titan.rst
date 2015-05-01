Convert current graph to TitanGraph.

Convert this Graph into a TitanGraph object.
This will be a new graph backed by Titan with all of the data found in this
graph.


Parameters
----------
new_graph_name : str (optional)
    The name of the new graph.
    Default is None.

Returns
-------
Graph
    A new TitanGraph.

Examples
--------
.. code::

    >>> graph = ia.get_graph("my_graph")
    >>> titan_graph = graph.export_to_titan("titan_graph")

