Build hierarchical clustering over an inital titan graph.


Parameters
----------
none

Returns
-------
a set of titan vertices and edges representing the internal clustering of the graph.

Notes
-----
The internal vertices and edges are for graph navigation and are added to the intial graph.

Examples
--------

::

sample_graph.txt is a file in the following format: src, dest, distance

1, 2, 1.5f
2, 1, 1.5f
2, 3, 1.5f
3, 2, 1.5f
1, 3, 1.5f
3, 1, 1.5f

the edge should have a property called "dist" holding the float value

the script:

    import intelanalytics as ia
    ia.connect()
    graph = ia.TitanGraph([src, dest, dist], "sample_graph")
    graph.hierarchical_clustering()

The expected output (new vertices) can be queried ::

    graph.query.gremlin('g.V.map(\'id\', \'vertex\', \'_label\', \'name\',\'count\')')

Snippet output for the above query will look like this:

{u'results': [u'{id=18432, count=null, _label=null, vertex=29, name=null}',
  u'{id=24576, count=null, _label=null, vertex=22, name=null}',
  u'{id=27136, count=null, _label=2, vertex=null, name=21944_25304}'

where:
     24576 - represents a initial ode
     27136 - represents a meta-node of 2 nodes (as per _label value)





