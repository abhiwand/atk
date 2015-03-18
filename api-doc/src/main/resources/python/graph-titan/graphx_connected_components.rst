Make sub-graph of interconnected but isolated vertices.

** Experimental Feature **
Connected components.

Parameters
----------
output_property : str
    The name of output property to be added to vertex/edge upon completion.

output_graph_name : str
    The name of output graph to be created (original graph will be left
    unmodified).

Returns
-------
str : graph name
    Name of the output graph.
    Call get_graph(graph) to get the handle to the new graph.

Examples
--------
.. only:: html

    .. code::

        >>> g.graphx_connected_components(output_property = "ccId", output_graph_name = "cc_graph")

    The expected output is like this:
    
    .. code::

        {u'graph': u'cc_graph'}

    To query:
    
    .. code::

        >>> cc_graph = get_graph('cc_graph')
        >>> cc_graph.query.gremlin("g.V [0..4]")

        {u'results':[{u'_id':4,u'_type':u'vertex',u'b':2335244,u'ccId':278456,u'pr':0.967054,u'titanPhysicalId':363016,u'triangle_count':1},{u'_id':8,u'_type':u'vertex',u'a':4877684,u'ccId':413036,u'pr':0.967054,u'titanPhysicalId':66001228,u'triangle_count':1},{u'_id':12,u'_type':u'vertex',u'b':1530344,u'ccId':34280,u'pr':0.967054,u'titanPhysicalId':9912,u'triangle_count':1},{u'_id':16,u'_type':u'vertex',u'b':3664209,u'ccId':206980,u'pr':0.967054,u'titanPhysicalId':229200900,u'triangle_count':1},{u'_id':20,u'_type':u'vertex',u'b':663159,u'ccId':268188,u'pr':0.967054,u'titanPhysicalId':268188,u'triangle_count':1}],u'run_time_seconds':0.254}

.. only:: latex

    .. code::

        >>> g.graphx_connected_components(output_property = "ccId", \
        ...     output_graph_name = "cc_graph")

    The expected output is like this:
    
    .. code::

        {u'graph': u'cc_graph'}

    To query:
    
    .. code::

        >>> cc_graph = get_graph('cc_graph')
        >>> cc_graph.query.gremlin("g.V [0..4]")

        {u'results':[
        {u'_id':4,
         u'_type':
         u'vertex',
         u'b':2335244,
         u'ccId':278456,
         u'pr':0.967054,
         u'titanPhysicalId':363016,
         u'triangle_count':1},
        {u'_id':8,
         u'_type':
         u'vertex',
         u'a':4877684,
         u'ccId':413036,
         u'pr':0.967054,
         u'titanPhysicalId':66001228,
         u'triangle_count':1},
        {u'_id':12,
         u'_type':
         u'vertex',
         u'b':1530344,
         u'ccId':34280,
         u'pr':0.967054,
         u'titanPhysicalId':9912,
         u'triangle_count':1},
        {u'_id':16,
         u'_type':
         u'vertex',
         u'b':3664209,
         u'ccId':206980,
         u'pr':0.967054,
         u'titanPhysicalId':229200900,
         u'triangle_count':1},
        {u'_id':20,
         u'_type':
         u'vertex',
         u'b':663159,
         u'ccId':268188,
         u'pr':0.967054,
         u'titanPhysicalId':268188,
         u'triangle_count':1}],
         u'run_time_seconds':0.254}



