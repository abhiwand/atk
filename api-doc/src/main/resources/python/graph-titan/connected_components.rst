Make sub-graph of interconnected but isolated vertices.

Label vertices by their connected component in the graph induced by a given
edge label.


Parameters
----------
input_edge_label : str
    The name of edge label used to for performing the connected components
    calculation.

output_vertex_property : str
    The vertex property which will contain the connected component id for
    each vertex.

convergence_output_interval : int (optional)
    The convergence progress output interval.
    Since convergence is a tricky notion for
    The valid value range is [1, max_supersteps]
    Default is 1 (output every superstep).

Returns
-------
str : Multiple line str
    The configuration and convergence report for Connected Components.

Notes
-----
It is prerequisite that the edge label in the property graph must be
bidirectional.

Examples
--------

.. code::

    >>> g.connected_components(input_edge_label = "edge", output_vertex_property = "component_id")



