Determining which vertices are the most important.

** Experimental Feature **
The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

Parameters
----------
output_property : str
    The name of output property to be added to vertex/edge upon completion.
output_graph_name : str
    The name of output graph to be created (original graph will be left
    unmodified).
input_edge_labels : list of str (optional)
    The name of edge labels to be considered for pagerank.
    Default is all edges are considered.
max_iterations : int (optional)
    The maximum number of iterations that the algorithm will execute.
    The valid range is all positive int.
    Invalid value will terminate with vertex page rank set to reset_probability.
    Default is 20.
convergence_tolerance : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If this parameter is specified, max_iterations is not
    considered as a stopping condition.
    If the change is less than this threshold, the algorithm exits earlier.
    The valid value range is all float and zero.
    Default is 0.001.
reset_probability : float (optional)
    The probability that the random walk of a page is reset.
    Default is 0.15.

Returns
-------
str : Name of graph
    Name of the output graph.
    Call get_graph(graph) to get the handle to the new graph.

Examples
--------
.. code::

    >>> g.graphx_pagerank(output_property = "pr_result", output_graph_name = "pr_graph")

The expected output is like this:

.. code::

    {u'graph': u'pr_graph'}

To query:
    
.. only:: html

    .. code::

        >>> pr_graph = get_graph('pr_graph')
        >>> pr_graph.query.gremlin("g.V [0..4]")

        {u'results':[{u'_id':4,u'_type':u'vertex',u'pr_result':0.787226,u'titanPhysicalId':133200148,u'user_id':7665,u'vertex_type':u'L'},{u'_id':8,u'_type':u'vertex',u'pr_result':1.284043,u'movie_id':7080,u'titanPhysicalId':85200356,u'vertex_type':u'R'},{u'_id':12,u'_type':u'vertex',u'pr_result':0.186911,u'movie_id':8904,u'titanPhysicalId':15600404,u'vertex_type':u'R'},{u'_id':16,u'_type':u'vertex',u'pr_result':0.384138,u'movie_id':6836,u'titanPhysicalId':105600396,u'vertex_type': u'R'},{u'_id':20,u'_type':u'vertex',u'pr_result':0.822977,u'titanPhysicalId':68400136,u'user_id':3223,u'vertex_type':u'L'}],u'run_time_seconds':1.489}

.. only:: latex

    .. code::

        >>> pr_graph = get_graph('pr_graph')
        >>> pr_graph.query.gremlin("g.V [0..4]")

        {u'results':[
           {u'_id':4,
            u'_type':u'vertex',
            u'pr_result':0.787226,
            u'titanPhysicalId':133200148,
            u'user_id':7665,
            u'vertex_type':u'L'},
           {u'_id':8,
            u'_type':u'vertex',
            u'pr_result':1.284043,
            u'movie_id':7080,
            u'titanPhysicalId':85200356,
            u'vertex_type':u'R'},
           {u'_id':12,
            u'_type':u'vertex',
            u'pr_result':0.186911,
            u'movie_id':8904,
            u'titanPhysicalId':15600404,
            u'vertex_type':u'R'},
           {u'_id':16,
            u'_type':u'vertex',
            u'pr_result':0.384138,
            u'movie_id':6836,
            u'titanPhysicalId':105600396,
            u'vertex_type': u'R'},
           {u'_id':20,
            u'_type':u'vertex',
            u'pr_result':0.822977,
            u'titanPhysicalId':68400136,
            u'user_id':3223,
            u'vertex_type':u'L'}],
           u'run_time_seconds':1.489}
