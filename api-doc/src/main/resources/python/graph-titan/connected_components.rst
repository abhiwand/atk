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
    The valid value range is [1, max_supersteps].
    Default is 1 (output every superstep).


Returns
-------
str
    The configuration and convergence report for Connected Components in the
    format of a multiple-line string.


Notes
-----
It is prerequisite that the edge label in the property graph must be
bidirectional.


Examples
--------
.. only:: html

    .. code::

        >>> g.connected_components(input_edge_label = "edge", output_vertex_property = "component_id")

.. only:: latex

    .. code::

        >>> g.connected_components(input_edge_label = "edge",
        ... output_vertex_property = "component_id")

