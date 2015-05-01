Make subgraph from vertex sampling.

Create a vertex induced subgraph obtained by vertex sampling.
Three types of vertex sampling are provided: 'uniform', 'degree', and
'degreedist'.
A 'uniform' vertex sample is obtained by sampling vertices uniformly at random.
For 'degree' vertex sampling, each vertex is weighted by its out-degree.
For 'degreedist' vertex sampling, each vertex is weighted by the total
number of vertices that have the same out-degree as it.
That is, the weight applied to each vertex for 'degreedist' vertex sampling
is given by the out-degree histogram bin size.


Parameters
----------
size : int
    The number of vertices to sample from the graph.
sample_type : str
    The type of vertex sample among: ['uniform', 'degree', 'degreedist'].
seed : int (optional)
    Random seed value.


Returns
-------
Graph
    A new Graph object representing the vertex induced subgraph.


Examples
--------
Assume a set of rules created on a Frame that specifies 'user' and 'product'
vertices as well as an edge rule.
The Graph created from this data can be vertex sampled to obtain a vertex
induced subgraph:

.. code::

    >>> my_graph = ia.TitanGraph([user_vertex_rule, product_vertex_rule, edge_rule])
    >>> my_subgraph = my_graph.sampling.vertex_sample(1000, 'uniform')
