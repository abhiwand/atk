PageRank.

The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

Parameters
----------
input_edge_label : list of string
    The name of edge label

output_vertex_property_list : list of string
    The list of vertex properties to store output vertex values

max_supersteps : integer (optional)
    The maximum number of super steps that the algorithm will execute.
    The valid value range is all positive integer.
    The default value is 20.

convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of super steps.
    The valid value range is all Float and zero.
    The default value is 0.001.

reset_probability : float (optional)
    The probability that the random walk of a page is reset.

convergence_output_interval : integer (optional)
    The convergence progress output interval
    The valid value range is [1, max_supersteps]
    The default value is 1, which means output every super step.

Returns
-------
Multiple line string
    The configuration and convergence report for Page Rank

Examples
--------
.. only:: html

    ::

        g.ml.page_rank(input_edge_label_list = "edge", output_vertex_property_list = "pr_result")

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 20140\\nNumber of edges: 604016\\n\\n======PageRank Configuration======\\nmaxSupersteps: 20\\nconvergenceThreshold: 0.001000\\nresetProbability: 0.150000\\nconvergenceProgressOutputInterval: 1\\n\\n======Learning Progress======\\nsuperstep = 1\\tsumDelta = 34080.702123\\nsuperstep = 2\\tsumDelta = 28520.485452\\nsuperstep = 3\\tsumDelta = 24241.118854\\nsuperstep = 4\\tsumDelta = 20605.006026\\nsuperstep = 5\\tsumDelta = 17514.126263\\nsuperstep = 6\\tsumDelta = 14887.007741\\nsuperstep = 7\\tsumDelta = 12653.956935\\nsuperstep = 8\\tsumDelta = 10755.863697\\nsuperstep = 9\\tsumDelta = 9142.484399\\nsuperstep = 10\\tsumDelta = 7771.111957\\nsuperstep = 11\\tsumDelta = 6605.445348\\nsuperstep = 12\\tsumDelta = 5614.628704\\nsuperstep = 13\\tsumDelta = 4772.434532\\nsuperstep = 14\\tsumDelta = 4056.569466\\nsuperstep = 15\\tsumDelta = 3448.084143\\nsuperstep = 16\\tsumDelta = 2930.871604\\nsuperstep = 17\\tsumDelta = 2491.240933\\nsuperstep = 18\\tsumDelta = 2117.554852\\nsuperstep = 19\\tsumDelta = 1799.921675\\nsuperstep = 20\\tsumDelta = 1529.933467\\nsuperstep = 21\\tsumDelta = 1300.443483\\nsuperstep = 22\\tsumDelta = 1105.376992\\nsuperstep = 23\\tsumDelta = 939.570469\\nsuperstep = 24\\tsumDelta = 798.634921\\nsuperstep = 25\\tsumDelta = 678.839702\\nsuperstep = 26\\tsumDelta = 577.013763\\nsuperstep = 27\\tsumDelta = 489.603196\\nsuperstep = 28\\tsumDelta = 415.350199\\nsuperstep = 29\\tsumDelta = 352.477966\\nsuperstep = 30\\tsumDelta = 299.025706'}

.. only:: latex

    ::

        g.ml.page_rank(input_edge_label_list = "edge", \\
            output_vertex_property_list = "pr_result")

    The expected output is like this::

        {u'value': u'======Graph Statistics======\\n
        Number of vertices: 20140\\n
        Number of edges: 604016\\n
        \\n
        ======PageRank Configuration======\\n
        maxSupersteps: 20\\n
        convergenceThreshold: 0.001000\\n
        resetProbability: 0.150000\\n
        convergenceProgressOutputInterval: 1\\n
        \\n
        ======Learning Progress======\\n
        superstep = 1\\tsumDelta = 34080.702123\\n
        superstep = 2\\tsumDelta = 28520.485452\\n
        superstep = 3\\tsumDelta = 24241.118854\\n
        superstep = 4\\tsumDelta = 20605.006026\\n
        superstep = 5\\tsumDelta = 17514.126263\\n
        superstep = 6\\tsumDelta = 14887.007741\\n
        superstep = 7\\tsumDelta = 12653.956935\\n
        superstep = 8\\tsumDelta = 10755.863697\\n
        superstep = 9\\tsumDelta = 9142.484399\\n
        superstep = 10\\tsumDelta = 7771.111957\\n
        superstep = 11\\tsumDelta = 6605.445348\\n
        superstep = 12\\tsumDelta = 5614.628704\\n
        superstep = 13\\tsumDelta = 4772.434532\\n
        superstep = 14\\tsumDelta = 4056.569466\\n
        superstep = 15\\tsumDelta = 3448.084143\\n
        superstep = 16\\tsumDelta = 2930.871604\\n
        superstep = 17\\tsumDelta = 2491.240933\\n
        superstep = 18\\tsumDelta = 2117.554852\\n
        superstep = 19\\tsumDelta = 1799.921675\\n
        superstep = 20\\tsumDelta = 1529.933467\\n
        superstep = 21\\tsumDelta = 1300.443483\\n
        superstep = 22\\tsumDelta = 1105.376992\\n
        superstep = 23\\tsumDelta = 939.570469\\n
        superstep = 24\\tsumDelta = 798.634921\\n
        superstep = 25\\tsumDelta = 678.839702\\n
        superstep = 26\\tsumDelta = 577.013763\\n
        superstep = 27\\tsumDelta = 489.603196\\n
        superstep = 28\\tsumDelta = 415.350199\\n
        superstep = 29\\tsumDelta = 352.477966\\n
        superstep = 30\\tsumDelta = 299.025706'}


