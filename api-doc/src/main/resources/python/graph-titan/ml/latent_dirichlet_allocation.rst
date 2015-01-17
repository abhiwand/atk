Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`_",

Parameters
----------
edge_value_property_list : list of string
    The edge properties which contain the input edge values.
    We expect comma-separated list of property names  if you use
    more than one edge property.

input_edge_label_list : list of string
    The name of edge label

output_vertex_property_list : list of string
    The list of vertex properties to store output vertex values

vertex_type : string
    The name of vertex property which contains vertex type

vector_value : boolean
    True means a vector as vertex value is supported,
    False means a vector as vertex value is not supported

max_supersteps : integer (optional)
    The maximum number of super steps (iterations) that the algorithm
    will execute.
    The valid value range is all positive integer.
    The default value is 20.

alpha : float (optional)
    The hyper-parameter for document-specific distribution over topics.
    It's mainly used as a smoothing parameter in Bayesian inference.
    Larger value implies that documents are assumed to cover all topics
    more uniformly; smaller value implies that documents are more
    concentrated on a small subset of topics.
    Valid value range is all positive float.
    The default value is 0.1.

beta : float (optional)
    The hyper-parameter for word-specific distribution over topics.
    It's mainly used as a smoothing parameter in Bayesian inference.
    Larger value implies that topics contain all words more uniformly and
    smaller value implies that topics are more concentrated on a small
    subset of words.
    Valid value range is all positive float.
    The default value is 0.1.

convergence_threshold : float (optional)
    The amount of change in LDA model parameters that will be tolerated
    at convergence. If the change is less than this threshold, the algorithm
    exists earlier before it reaches the maximum number of super steps.
    Valid value range is all positive float and zero.
    The default value is 0.001.

evaluate_cost : string (optional)
    "True" means turn on cost evaluation and "False" means turn off
    cost evaluation.
    It's relatively expensive for LDA to evaluate cost function.
    For time-critical applications, this option allows user to turn off cost
    function evaluation.
    The default value is False.

max_val : float (optional)
    The maximum edge weight value. If an edge weight is larger than this
    value, the algorithm will throw an exception and terminate. This option
    is mainly for graph integrity check.
    Valid value range is all float.
    The default value is "Infinity".

min_val : float (optional)
    The minimum edge weight value. If an edge weight is smaller than this
    value, the algorithm will throw an exception and terminate. This option
    is mainly for graph integrity check.
    Valid value range is all float.
    The default value is "-Infinity".

validate_graph_structure : boolean (optional)
    Checks if the graph meets certain structural requirements before starting
    the algorithm.

    At present, this checks that at every vertex, the in-degree equals the
    out-degree.
    Because this algorithm is for undirected graphs, this is a necessary
    but not sufficient, check for valid input.

num_topics : integer (optional)
    The number of topics to identify in the LDA model. Using fewer
    topics will speed up the computation, but the extracted topics
    might be more abstract or less specific; using more topics will
    result in more computation but lead to more specific topics.
    Valid value range is all positive integers.
    The default value is 10.

Returns
-------
Multiple line string
    The configuration and learning curve report for Latent Dirichlet
    Allocation.

Examples
--------
.. only:: html

    ::

        g.ml.latent_dirichlet_allocation(edge_value_property_list = "word_count", vertex_type_property_key = "vertex_type", input_edge_label_list = "contains", output_vertex_property_list = "lda_result ", vector_value = "true", num_topics = 3)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 12 (doc: 6, word: 6)\\nNumber of edges: 12\\n\\n======LDA Configuration======\\nnumTopics: 3\\nalpha: 0.100000\\nbeta: 0.100000\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: false\\nmaxSupersteps: 20\\nmaxVal: Infinity\\nminVal: -Infinity\\nevaluateCost: false\\n\\n======Learning Progress======\\nsuperstep = 1\\tmaxDelta = 0.333682\\nsuperstep = 2\\tmaxDelta = 0.117571\\nsuperstep = 3\\tmaxDelta = 0.073708\\nsuperstep = 4\\tmaxDelta = 0.053260\\nsuperstep = 5\\tmaxDelta = 0.038495\\nsuperstep = 6\\tmaxDelta = 0.028494\\nsuperstep = 7\\tmaxDelta = 0.020819\\nsuperstep = 8\\tmaxDelta = 0.015374\\nsuperstep = 9\\tmaxDelta = 0.011267\\nsuperstep = 10\\tmaxDelta = 0.008305\\nsuperstep = 11\\tmaxDelta = 0.006096\\nsuperstep = 12\\tmaxDelta = 0.004488\\nsuperstep = 13\\tmaxDelta = 0.003297\\nsuperstep = 14\\tmaxDelta = 0.002426\\nsuperstep = 15\\tmaxDelta = 0.001783\\nsuperstep = 16\\tmaxDelta = 0.001311\\nsuperstep = 17\\tmaxDelta = 0.000964\\nsuperstep = 18\\tmaxDelta = 0.000709\\nsuperstep = 19\\tmaxDelta = 0.000521\\nsuperstep = 20\\tmaxDelta = 0.000383'}

.. only:: latex

    ::

        g.ml.latent_dirichlet_allocation( \\
            edge_value_property_list = "word_count", \\
            vertex_type_property_key = "vertex_type", \\
            input_edge_label_list = "contains", \\
            output_vertex_property_list = "lda_result ", \\
            vector_value = "true", \\
            num_topics = 3)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\n
        Number of vertices: 12 (doc: 6, word: 6)\\n
        Number of edges: 12\\n
        \\n
        ======LDA Configuration======\\n
        numTopics: 3\\n
        alpha: 0.100000\\n
        beta: 0.100000\\n
        convergenceThreshold: 0.000000\\n
        bidirectionalCheck: false\\n
        maxSupersteps: 20\\n
        maxVal: Infinity\\n
        minVal: -Infinity\\n
        evaluateCost: false\\n
        \\n
        ======Learning Progress======\\n
        superstep = 1\\tmaxDelta = 0.333682\\n
        superstep = 2\\tmaxDelta = 0.117571\\n
        superstep = 3\\tmaxDelta = 0.073708\\n
        superstep = 4\\tmaxDelta = 0.053260\\n
        superstep = 5\\tmaxDelta = 0.038495\\n
        superstep = 6\\tmaxDelta = 0.028494\\n
        superstep = 7\\tmaxDelta = 0.020819\\n
        superstep = 8\\tmaxDelta = 0.015374\\n
        superstep = 9\\tmaxDelta = 0.011267\\n
        superstep = 10\\tmaxDelta = 0.008305\\n
        superstep = 11\\tmaxDelta = 0.006096\\n
        superstep = 12\\tmaxDelta = 0.004488\\n
        superstep = 13\\tmaxDelta = 0.003297\\n
        superstep = 14\\tmaxDelta = 0.002426\\n
        superstep = 15\\tmaxDelta = 0.001783\\n
        superstep = 16\\tmaxDelta = 0.001311\\n
        superstep = 17\\tmaxDelta = 0.000964\\n
        superstep = 18\\tmaxDelta = 0.000709\\n
        superstep = 19\\tmaxDelta = 0.000521\\n
        superstep = 20\\tmaxDelta = 0.000383'}

