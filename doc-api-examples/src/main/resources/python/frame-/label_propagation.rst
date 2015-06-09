Label Propagation on Gaussian Random Fields.

This algorithm is presented in X. Zhu and Z. Ghahramani.
Learning from labeled and unlabeled data with label propagation.
Technical Report CMU-CALD-02-107, CMU, 2002.


Parameters
----------
src_col_name: str
    The column name for the source vertex id.
dest_col_name: str
    The column name for the destination vertex id.
weight_col_name: str
    The column name for the edge weight.
src_label_col_name: str
    The column name for the label properties for the source vertex.
result_col_name : str (optional)
    column name for the results (holding the post labels for the vertices)
max_iterations : int (optional)
    The maximum number of supersteps that the algorithm will execute.
    The valid value range is all positive int.
    The default value is 10.
convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of supersteps.
    The valid value range is all float and zero.
    The default value is 0.00000001f.
alpha : float (optional)
    The tradeoff parameter that controls how much influence an external
    classifier's prediction contributes to the final prediction.
    This is for the case where an external classifier is available that can
    produce initial probabilistic classification on unlabeled examples, and
    the option allows incorporating external classifier's prediction into
    the LP training process.
    The valid value range is [0.0,1.0].
    The default value is 0.


Returns
-------
a 2-column frame:

vertex: int
    A vertex id.
result : Vector (long)
    label vector for the results (for the node id in column 1)

Examples
--------
.. only:: html

    .. code::

    input frame (lp.csv)
    "a"        "b"        "c"        "d"
    1,         2,         0.5,       "0.5,0.5"
    2,         3,         0.4,       "-1,-1"
    3,         1,         0.1,       "0.8,0.2"

    script

    ia.connect()
    s = [("a", ia.int32), ("b", ia.int32), ("c", ia.float32), ("d", ia.vector(2))]
    d = "lp.csv"
    c = ia.CsvFile(d,s)
    f = ia.Frame(c)
    r = f.label_propagation("a", "b", "c", "d", "results")
    r['frame'].inspect()
    r['report']

.. only:: latex

    .. code::

        >>> r = f.label_propagation(
        ... srcColName = "a",
        ... destColName  = "b",
        ... weightColName = "c",
        ... srcLabelColName = "d",
        ... resultColName = "resultLabels")
        ... r['frame'].inspect()
        ... r['report']

The expected output is like this:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\nNumber of vertices: 600\nNumber of edges: 15716\n\n======LP Configuration======\nlambda: 0.000000\nanchorThreshold: 0.900000\nconvergenceThreshold: 0.000000\nmaxSupersteps: 10\nbidirectionalCheck: false\n\n======Learning Progress======\nsuperstep = 1\tcost = 0.008692\nsuperstep = 2\tcost = 0.008155\nsuperstep = 3\tcost = 0.007809\nsuperstep = 4\tcost = 0.007544\nsuperstep = 5\tcost = 0.007328\nsuperstep = 6\tcost = 0.007142\nsuperstep = 7\tcost = 0.006979\nsuperstep = 8\tcost = 0.006833\nsuperstep = 9\tcost = 0.006701\nsuperstep = 10\tcost = 0.006580'}

.. only:: latex

    .. code::

        {u'value': u'======Graph Statistics======\n
        Number of vertices: 600\n
        Number of edges: 15716\n
        \n
        ======LP Configuration======\n
        lambda: 0.000000\n
        anchorThreshold: 0.900000\n
        convergenceThreshold: 0.000000\n
        maxSupersteps: 10\n
        bidirectionalCheck: false\n
        \n
        ======Learning Progress======\n
        superstep = 1\tcost = 0.008692\n
        superstep = 2\tcost = 0.008155\n
        superstep = 3\tcost = 0.007809\n
        superstep = 4\tcost = 0.007544\n
        superstep = 5\tcost = 0.007328\n
        superstep = 6\tcost = 0.007142\n
        superstep = 7\tcost = 0.006979\n
        superstep = 8\tcost = 0.006833\n
        superstep = 9\tcost = 0.006701\n
        superstep = 10\tcost = 0.006580'}

