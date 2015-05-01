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
.. code::

    >>> import intelanalytics as ia
    >>> ia.connect()
    >>> dataset = r"datasets/kclique_edges.csv"
    >>> schema = [("source", int64), ("target", int64)]
    >>> csvfile = ia.CsvFile(dataset, schema)
    >>> my_frame = ia.Frame(csvfile)

    >>> my_graph = ia.Graph())
    >>> my_graph.name = "mygraph"
    >>> source_vertex_type = my_graph.define_vertex_type("source")
    >>> target_vertex_type = my_graph.define_vertex_type("target")
    >>> direction_edge_type = my_graph.define_edge_type("direction",
    ... "source", "target", directed=True)

    >>> my_graph.vertices['source'].add_vertices(my_frame, 'source')
    >>> my_graph.vertices['target'].add_vertices(my_frame, 'target')
    >>> my_graph.edges['direction'].add_edges(my_frame, 'source', 'target',
    ... is_directed=True)
    >>> my_titan_graph = my_graph.export_to_titan("mytitangraph"))
    >>> my_titan_graph.ml.kclique_percolation(cliqueSize = 3,
    ... communityPropertyDefaultLabel = "Community")


