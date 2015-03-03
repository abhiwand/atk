Number of triangles among vertices of current graph.

** Experimental Feature **
Triangle Count.
Counts the number of triangles among vertices in an undirected graph.
If an edge is marked bidirectional, the implementation opts for canonical
orientation of edges hence counting it only once (similar to an
undirected graph).

Parameters
----------
output_property : str
    The name of output property to be added to vertex/edge upon completion.

output_graph_name : str
    The name of output graph to be created (original graph will be left
    unmodified).

input_edge_labels : list of str (optional)
    The name of edge labels to be considered for triangle count.
    Default is all edges are considered.

Returns
-------
str : graph name
    Name of the output graph.
    Call get_graph(graph) to get the handle to the new graph.

Examples
--------
::
    g.graphx_triangle_count(output_property = "triangle_count",
                               output_graph_name = "tc_graph")

The expected output is like this::

    {u'graph': u'tc_graph'}

.. only:: html

    To query::

        tc_graph = get_graph('tc_graph')
        tc_graph.query.gremlin("g.V [0..4]")

        {u'results':[{u'_id':4,u'_type':u'vertex',u'b':3603376,u'pr':0.967054,u'titanPhysicalId':363016,u'triangle_count':1},{u'_id':8,u'_type':u'vertex',u'a':3118124,u'pr':0.967054,u'titanPhysicalId':343201000,u'triangle_count':1},{u'_id':12,u'_type':u'vertex',u'a':3063711,u'pr':0.967054,u'titanPhysicalId':43068,u'triangle_count':1},{u'_id':16,u'_type':u'vertex',u'c':899225,u'pr':0.967054,u'titanPhysicalId':202800088,u'triangle_count':1},{u'_id':20,u'_type':u'vertex',u'c':1493990,u'pr':0.967054,u'titanPhysicalId':268188,u'triangle_count':1}],u'run_time_seconds':0.271}

.. only:: latex

    To query::

        tc_graph = get_graph('tc_graph')
        tc_graph.query.gremlin("g.V [0..4]")

        {u'results':
            [{u'_id':4,
              u'_type':u'vertex',
              u'b':3603376,
              u'pr':0.967054,
              u'titanPhysicalId':363016,
              u'triangle_count':1},
             {u'_id':8,
              u'_type':u'vertex',
              u'a':3118124,
              u'pr':0.967054,
              u'titanPhysicalId':343201000,
              u'triangle_count':1},
             {u'_id':12,
              u'_type':u'vertex',
              u'a':3063711,
              u'pr':0.967054,
              u'titanPhysicalId':43068,
              u'triangle_count':1},
             {u'_id':16,
              u'_type':u'vertex',
              u'c':899225,
              u'pr':0.967054,
              u'titanPhysicalId':202800088,
              u'triangle_count':1},
             {u'_id':20,
              u'_type':u'vertex',
              u'c':1493990,
              u'pr':0.967054,
              u'titanPhysicalId':268188,
              u'triangle_count':1}],
              u'run_time_seconds':0.271}

