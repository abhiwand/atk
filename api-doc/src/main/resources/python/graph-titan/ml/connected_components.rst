Label vertices by their connected component in the graph induced by a given edge label.

Prerequisites::

    Edge label in the property graph must be bidirectional.

Parameters
----------
input_edge_label : string
    The name of edge label used to for performing the connected components
    calculation.

output_vertex_property : string
    The vertex property which will contain the connected component id for
    each vertex.

convergence_output_interval : integer (optional)
    The convergence progress output interval.
    Since convergence is a tricky notion for
    The valid value range is [1, max_supersteps]
    The default value is 1, which means output every super step.

Returns
-------
Multiple line string
    The configuration and convergence report for Connected Components.

Examples
--------
::

    g.ml.conncted_components(input_edge_label = "edge", output_vertex_property = "component_id")



