Classification on sparse data using belief propagation.

Belief propagation by the sum-product algorithm.
This algorithm analyzes a graphical model with prior beliefs using sum
product message passing.
The priors are read from a property in the graph, the posteriors are written
to another property in the graph.

This is the GraphX-based implementation of belief propagation.


Parameters
----------
prior_property : str
    Name of the vertex property which contains the prior belief for the
    vertex.
posterior_property : str
    Name of the vertex property which will contain the posterior belief for
    each vertex.
edge_weight_property : str (optional)
    The edge property that contains the edge weight for each edge.
    Default is 1.
convergence_threshold : double (optional)
    Minimum average change in posterior beliefs between supersteps.
    Belief propagation will terminate when the average change in posterior
    beliefs between supersteps is less than or equal to this threshold.
    Default is 0.
max_iterations : int (optional)
    The maximum number of supersteps that the algorithm will execute.
    The valid range is all positive int.
    Default is 20.


Returns
-------
str
    Progress report for belief propagation in the format of a multiple-line
    string.


Examples
--------
.. only:: html

    .. code::

        >>> graph.ml.belief_propagation("value", "lbp_output", string_output = True, state_space_size = 5, max_iterations = 6)

        {u'log': u'Vertex Count: 80000\\nEdge Count: 318398\\nIATPregel engine has completed iteration 1  The average delta is 0.6853413553663811\\nIATPregel engine has completed iteration 2  The average delta is 0.38626944467366386\\nIATPregel engine has completed iteration 3  The average delta is 0.2365329376479823\\nIATPregel engine has completed iteration 4  The average delta is 0.14170840479478952\\nIATPregel engine has completed iteration 5  The average delta is 0.08676093923623975\\n', u'time': 70.248999999999995}

        >>> graph.query.gremlin("g.V [0..4]")

        {u'results': [{u'vertex_type': u'VA', u'target': 12779523, u'lbp_output': u'0.9485759073302487, 0.001314151524421738, 0.040916996746627056, 0.001397331576080859, 0.0077956128226217315', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'titanPhysicalId': 4, u'_id': 4}, {u'vertex_type': u'VA', u'titanPhysicalId': 8, u'lbp_output': u'0.7476996339617544, 0.0021769696832380173, 0.24559940461433935, 0.0023272253558738786, 0.002196766384794168', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'source': 7798852, u'_id': 8}, {u'vertex_type': u'TR', u'target': 13041863, u'lbp_output': u'0.7288360734608738, 0.07162637515155296, 0.15391773902131053, 0.022620779563724287, 0.02299903280253846', u'_type': u'vertex', u'value': u'0.5 0.125 0.125 0.125 0.125', u'titanPhysicalId': 12, u'_id': 12}, {u'vertex_type': u'TR', u'titanPhysicalId': 16, u'lbp_output': u'0.9996400056392905, 9.382190989071985E-5, 8.879762476576982E-5, 8.867586165695348E-5, 8.869896439624652E-5', u'_type': u'vertex', u'value': u'0.5 0.125 0.125 0.125 0.125', u'source': 11731127, u'_id': 16}, {u'vertex_type': u'TE', u'titanPhysicalId': 20, u'lbp_output': u'0.004051247779081896, 0.2257641948616088, 0.01794622866204068, 0.7481547408142287, 0.004083587883039745', u'_type': u'vertex', u'value': u'0.125 0.125 0.5 0.125 0.125', u'source': 3408035, u'_id': 20}], u'run_time_seconds': 1.042}

.. only:: latex

    .. code::

        >>> graph.ml.belief_propagation("value", "lbp_output", string_output = True,
        ...    state_space_size = 5, max_iterations = 6)

        {u'log': u'Vertex Count: 80000\\n
        Edge Count: 318398\\n
        IATPregel engine has completed iteration 1  The average delta is 0.6853413553663811\\n
        IATPregel engine has completed iteration 2  The average delta is 0.38626944467366386\\n
        IATPregel engine has completed iteration 3  The average delta is 0.2365329376479823\\n
        IATPregel engine has completed iteration 4  The average delta is 0.14170840479478952\\n
        IATPregel engine has completed iteration 5  The average delta is 0.08676093923623975\\n
        ', u'time': 70.248999999999995}

        >>> graph.query.gremlin("g.V [0..4]")

        {u'results': [{u'vertex_type':
         u'VA',
         u'target': 12779523,
         u'lbp_output':
         u'0.9485759073302487, 0.001314151524421738,
            0.040916996746627056, 0.001397331576080859, 0.0077956128226217315',
         u'_type':
         u'vertex',
         u'value':
         u'0.125 0.125 0.5 0.125 0.125',
         u'titanPhysicalId': 4,
         u'_id': 4},
        {u'vertex_type':
         u'VA',
         u'titanPhysicalId': 8,
         u'lbp_output':
         u'0.7476996339617544,
            0.0021769696832380173, 0.24559940461433935, 0.0023272253558738786,
            0.002196766384794168',
         u'_type':
         u'vertex',
         u'value':
         u'0.125 0.125 0.5 0.125 0.125',
         u'source': 7798852,
         u'_id': 8},
        {u'vertex_type':
         u'TR',
         u'target': 13041863,
         u'lbp_output':
         u'0.7288360734608738, 0.07162637515155296,
            0.15391773902131053, 0.022620779563724287, 0.02299903280253846',
         u'_type':
         u'vertex',
         u'value':
         u'0.5 0.125 0.125 0.125 0.125',
         u'titanPhysicalId': 12,
         u'_id': 12},
        {u'vertex_type':
         u'TR',
         u'titanPhysicalId': 16,
         u'lbp_output':
         u'0.9996400056392905,
            9.382190989071985E-5, 8.879762476576982E-5, 8.867586165695348E-5,
            8.869896439624652E-5',
         u'_type':
         u'vertex',
         u'value':
         u'0.5 0.125 0.125 0.125 0.125',
         u'source': 11731127,
         u'_id': 16},
        {u'vertex_type':
         u'TE',
         u'titanPhysicalId': 20,
         u'lbp_output':
         u'0.004051247779081896, 0.2257641948616088,
            0.01794622866204068, 0.7481547408142287, 0.004083587883039745',
         u'_type':
         u'vertex',
         u'value':
         u'0.125 0.125 0.5 0.125 0.125',
         u'source': 3408035,
         u'_id': 20}],
         u'run_time_seconds': 1.045}


