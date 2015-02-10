Loopy belief propagation on Markov Random Fields(MRF).

This algorithm was originally designed for acyclic graphical models,
then it was found that the Belief Propagation algorithm can be used
in general graphs.
The algorithm is then sometimes called "loopy" belief propagation,
because graphs typically contain cycles, or loops.

In Giraph, we run the algorithm in iterations until it converges.

Parameters
----------
vertex_value_property_list : list of string
    The vertex properties which contain prior vertex values if you
    use more than one vertex property.

edge_value_property_list : list of string
    The edge properties which contain the input edge values.
    We expect comma-separated list of property names  if you use
    more than one edge property.

input_edge_label_list : list of string
    The name of edge label.

output_vertex_property_list : list of string
    The list of vertex properties to store output vertex values.

vertex_type : string
    The name of vertex property which contains vertex type.
    The default value is "vertex_type".

vector_value : boolean
    True means a vector as vertex value is supported,
    False means a vector as vertex value is not supported.

max_supersteps : integer (optional)
    The maximum number of super steps that the algorithm will execute.
    The valid value range is all positive integer.
    The default value is 20.

convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of super steps.
    The valid value range is all float and zero.
    The default value is 0.001.

anchor_threshold : float (optional)
    The parameter that determines if a node's posterior will be updated or
    not.
    If a node's maximum prior value is greater than this threshold, the node
    will be treated as anchor node, whose posterior will inherit from prior
    without update.
    This is for the case where we have confident prior estimation for some
    nodes and don't want the algorithm updates these nodes.
    The valid value range is in [0, 1].
    The default value is 1.0.

smoothing : float (optional)
    The Ising smoothing parameter.
    This parameter adjusts the relative strength of closeness encoded edge
    weights, similar to the width of Gussian distribution.
    Larger value implies smoother decay and the edge weight beomes less
    important.
    The default value is 2.0.

validate_graph_structure : boolean (optional)
    Checks if the graph meets certain structural requirements before starting
    the algorithm.

    At present, this checks that at every vertex, the in-degree equals the
    out-degree.
    Because this algorithm is for undirected graphs, this is a necessary
    but not sufficient, check for valid input.

ignore_vertex_type : boolean (optional)
    If true, all vertex will be treated as training data.
    The default value is False.

max_product : boolean (optional)
    Should LBP use max_product or not.
    The default value is False.

power : float (optional)
    Power coefficient for power edge potential.
    The default value is 0.

Returns
-------
Multiple line string
    The configuration and learning curve report for Loopy Belief Propagation

Examples
--------
.. only:: html

    ::

        g.ml.loopy_belief_propagation(vertex_value_property_list = "value", edge_value_property_list  = "weight", input_edge_label_list = "edge",   output_vertex_property_list = "lbp_posterior",   vertex_type_property_key = "vertex_type",  vector_value = "true",    max_supersteps = 10,   convergence_threshold = 0.0, anchor_threshold = 0.9, smoothing = 2.0, bidirectional_check = False,  ignore_vertex_type = False, max_product= False, power = 0)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 80000 (train: 56123, validate: 15930, test: 7947)\\nNumber of edges: 318400\\n\\n======LBP Configuration======\\nmaxSupersteps: 10\\nconvergenceThreshold: 0.000000\\nanchorThreshold: 0.900000\\nsmoothing: 2.000000\\nbidirectionalCheck: false\\nignoreVertexType: false\\nmaxProduct: false\\npower: 0.000000\\n\\n======Learning Progress======\\nsuperstep = 1\\tavgTrainDelta = 0.594534\\tavgValidateDelta = 0.542366\\tavgTestDelta = 0.542801\\nsuperstep = 2\\tavgTrainDelta = 0.322596\\tavgValidateDelta = 0.373647\\tavgTestDelta = 0.371556\\nsuperstep = 3\\tavgTrainDelta = 0.180468\\tavgValidateDelta = 0.194503\\tavgTestDelta = 0.198478\\nsuperstep = 4\\tavgTrainDelta = 0.113280\\tavgValidateDelta = 0.117436\\tavgTestDelta = 0.122555\\nsuperstep = 5\\tavgTrainDelta = 0.076510\\tavgValidateDelta = 0.074419\\tavgTestDelta = 0.077451\\nsuperstep = 6\\tavgTrainDelta = 0.051452\\tavgValidateDelta = 0.051683\\tavgTestDelta = 0.052538\\nsuperstep = 7\\tavgTrainDelta = 0.038257\\tavgValidateDelta = 0.033629\\tavgTestDelta = 0.034017\\nsuperstep = 8\\tavgTrainDelta = 0.027924\\tavgValidateDelta = 0.026722\\tavgTestDelta = 0.025877\\nsuperstep = 9\\tavgTrainDelta = 0.022886\\tavgValidateDelta = 0.019267\\tavgTestDelta = 0.018190\\nsuperstep = 10\\tavgTrainDelta = 0.018271\\tavgValidateDelta = 0.015924\\tavgTestDelta = 0.015377'}
.. only:: latex

    ::

        g.ml.loopy_belief_propagation( \\
            vertex_value_property_list = "value", \\
            edge_value_property_list  = "weight", \\
            input_edge_label_list = "edge", \\
            output_vertex_property_list = "lbp_posterior",   \\
            vertex_type_property_key = "vertex_type",  \\
            vector_value = "true", \\
            max_supersteps = 10, \\
            convergence_threshold = 0.0, \\
            chor_threshold = 0.9, \\
            oothing = 2.0, \\
            directional_check = False, \\
            gnore_vertex_type = False, \\
            x_product= False, \\
            wer = 0)

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\n
        Number of vertices: 80000 (train: 56123, validate: 15930, test: 7947)\\n
        Number of edges: 318400\\n
        \\n
        ======LBP Configuration======\\n
        maxSupersteps: 10\\n
        convergenceThreshold: 0.000000\\n
        anchorThreshold: 0.900000\\n
        smoothing: 2.000000\\n
        bidirectionalCheck: false\\n
        ignoreVertexType: false\\n
        maxProduct: false\\n
        power: 0.000000\\n
        \\n
        ======Learning Progress======\\n
        superstep = 1\\t
            avgTrainDelta = 0.594534\\t
            avgValidateDelta = 0.542366\\t
            avgTestDelta = 0.542801\\n
        superstep = 2\\t
            avgTrainDelta = 0.322596\\t
            avgValidateDelta = 0.373647\\t
            avgTestDelta = 0.371556\\n
        superstep = 3\\t
            avgTrainDelta = 0.180468\\t
            avgValidateDelta = 0.194503\\t
            avgTestDelta = 0.198478\\n
        superstep = 4\\t
            avgTrainDelta = 0.113280\\t
            avgValidateDelta = 0.117436\\t
            avgTestDelta = 0.122555\\n
        superstep = 5\\t
            avgTrainDelta = 0.076510\\t
            avgValidateDelta = 0.074419\\t
            avgTestDelta = 0.077451\\n
        superstep = 6\\t
            avgTrainDelta = 0.051452\\t
            avgValidateDelta = 0.051683\\t
            avgTestDelta = 0.052538\\n
        superstep = 7\\t
            avgTrainDelta = 0.038257\\t
            avgValidateDelta = 0.033629\\t
            avgTestDelta = 0.034017\\n
        superstep = 8\\t
            avgTrainDelta = 0.027924\\t
            avgValidateDelta = 0.026722\\t
            avgTestDelta = 0.025877\\n
        superstep = 9\\t
            avgTrainDelta = 0.022886\\t
            avgValidateDelta = 0.019267\\t
            avgTestDelta = 0.018190\\n
        superstep = 10\\t
            avgTrainDelta = 0.018271\\t
            avgValidateDelta = 0.015924\\t
            avgTestDelta = 0.015377'}

