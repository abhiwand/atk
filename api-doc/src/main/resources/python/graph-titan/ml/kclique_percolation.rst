Find groups of vertices with similar attributes.

Parameters
----------
clique_size : int
    The sizes of the cliques used to form communities.
    Larger values of clique size result in fewer, smaller communities that are
    more connected.
    Must be at least 2.

community_property_label : str
    Name of the community property of vertex that will be updated/created in
    the graph.
    This property will contain for each vertex the set of communities that
    contain that vertex.

Notes
-----
K-clique percolation spawns a number of Spark jobs that cannot be
calculated before execution (it is bounded by the diameter of the clique
graph derived from the input graph).
For this reason, the initial loading, clique enumeration and clique-graph
construction steps are tracked with a single progress bar (this is most of
the time), and then successive iterations of analysis of the clique graph
are tracked with many short-lived progress bars, and then finally the
result is written out.


Examples
--------
::

    graph.ml.kclique_percolation(4, 'community')


