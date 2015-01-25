Subgraph from vertex sampling.

Create a vertex induced subgraph obtained by vertex sampling.
Three types of vertex sampling are provided: 'uniform', 'degree', and
'degreedist'.
A 'uniform' vertex sample is obtained by sampling vertices uniformly at
random.
For 'degree' vertex sampling, each vertex is weighted by its out-degree.
For 'degreedist' vertex sampling, each vertex is weighted by the total
number of vertices that have the same out-degree as it.
That is, the weight applied to each vertex for 'degreedist' vertex sampling
is given by the out-degree histogram bin size.

Parameters
----------
size : int
    the number of vertices to sample from the graph

sample_type : str
    the type of vertex sample among: ['uniform', 'degree', 'degreedist']

seed : int (optional)
    random seed value

Returns
-------
BigGraph
    a new BigGraph object representing the vertex induced subgraph

Examples
--------
Assume a set of rules created on a BigFrame that specifies 'user' and
'product' vertices as well as an edge rule.
The BigGraph created from this data can be vertex sampled to obtain a vertex
induced subgraph::

    graph = BigGraph([user_vertex_rule, product_vertex_rule, edge_rule])
    subgraph = graph.sampling.vertex_sample(1000, 'uniform')




