Calculates global clustering coefficient.

Computes the :term `global clustering coefficient` of the graph and optionally
creates a new frame with data from every vertex of the graph and
the :term:`local clustering coefficient` stored in a user-specified property.

THIS FUNCTION IS FOR UNDIRECTED GRAPHS.
If it is called on a directed graph, its output is NOT guaranteed to calculate
the local directed clustering coefficients.

Parameters
----------
output_property_name : str (optional)
    The name of the new property to which each vertex's local clustering
    coefficient will be written.
    If this option is not specified, no output frame will be produced and only
    the global clustering coefficient will be returned.
input_edge_labels : list of str (optional)
    If this list is provided, only edges whose labels are included in the given
    set will be considered in the clustering coefficient calculation.
    In the default situation (when no list is provided), all edges will be used
    in the calculation, regardless of label.
    It is required that all edges that enter into the clustering coefficient
    analysis be undirected.

Returns
-------
global_clustering_coefficient : double
    The global clustering coefficient of the graph.

frame : Frame
    A Frame is only returned if ``output_property_name`` is provided.
    The frame contains data from every vertex of the graph with its
    :term:`local clustering coefficient` stored in the user-specified property.

Examples
--------
.. code::

    >>> results = g.clustering_coefficient('ccgraph', 'local_clustering_coefficient')

    >>> results
        Out[8]:
        ClusteringCoefficient:
        global_clustering_coefficient: 0.0853107962708,
        frame: Frame

    >>> results.frame.inspect()
