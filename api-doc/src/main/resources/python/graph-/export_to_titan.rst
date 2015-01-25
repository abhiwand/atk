Convert to TitanGraph.

Convert this Graph into a TitanGraph object.
This will be a new graph backed by Titan with all of the data found in this
graph.

Parameters
----------
new_graph_name : str
    The name of the new graph.
    This is optional.
    If omitted a name will be generated.

Examples
--------
::

    graph = ia.get_graph("my_graph")
    titan_graph = graph.export_to_titan("titan_graph")

