The `Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`

Parameters
----------
frame : Frame
    Input frame data.
doc : str
    Column Name for documents.
    Column should contain a str value.
word : str
    Column name for words.
    Column should contain a str value.
word_count : str
    Column name for word count.
    Column should contain an int64 value.
max_interations : int (optional)
    The maximum number of iterations that the algorithm will execute.
    The valid value range is all positive int.
    Default is 20.
alpha : float (optional)
    The hyper-parameter for document-specific distribution over topics.
    Mainly used as a smoothing parameter in Bayesian inference.
    Larger value implies that documents are assumed to cover all topics
    more uniformly; smaller value implies that documents are more
    concentrated on a small subset of topics.
    Valid value range is all positive float.
    Default is 0.1.
beta : float (optional)
    The hyper-parameter for word-specific distribution over topics.
    Mainly used as a smoothing parameter in Bayesian inference.
    Larger value implies that topics contain all words more uniformly and
    smaller value implies that topics are more concentrated on a small
    subset of words.
    Valid value range is all positive float.
    Default is 0.1.
convergence_threshold : float (optional)
    The amount of change in LDA model parameters that will be tolerated
    at convergence.
    If the change is less than this threshold, the algorithm exits
    before it reaches the maximum number of supersteps.
    Valid value range is all positive float and 0.0.
    Default is 0.001.
evaluate_cost : bool (optional)
    "True" means turn on cost evaluation and "False" means turn off
    cost evaluation.
    It's relatively expensive for LDA to evaluate cost function.
    For time-critical applications, this option allows user to turn off cost
    function evaluation.
    Default is "False".
num_topics : int (optional)
    The number of topics to identify in the LDA model.
    Using fewer topics will speed up the computation, but the extracted topics
    might be more abstract or less specific; using more topics will
    result in more computation but lead to more specific topics.
    Valid value range is all positive int.
    Default is 10.

Returns
-------
dict : results dictionary
    Contains three keys |EM|

    Frame : doc_results
        Frame with LDA results.
    Frame : word_results
        Frame with LDA results.
    str : report
       The configuration and learning curve report for Latent Dirichlet
       Allocation as a multiple line str.

Examples
--------

.. code::

    >>> my_model = LdaModel()
    >>> results = my_model.train(frame, 'doc_column_name', 'word_column_name', 'word_count_column_name', num_topics = 3)

The variable *results* is a dictionary with three keys:

.. code::

    >>> doc_results = results['doc_results']
    >>> word_results = results['word_results']
    >>> report = results['report']

Inspect the results:

.. code::

    >>> doc_results.inspect()
    >>> word_results.inspect()

View the report:

.. code::

    >>> print report

The expected output of ``results['report']`` is similar to:

.. only:: html

    .. code::

        {u'value': u'======Graph Statistics======\\nNumber of vertices: 12 (doc: 6, word: 6)\\nNumber of edges: 12\\n\\n======LDA Configuration======\\nnumTopics: 3\\nalpha: 0.100000\\nbeta: 0.100000\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: false\\nmaxIterations: 20\\nmaxVal: Infinity\\nminVal: -Infinity\\nevaluateCost: false\\n\\n======Learning Progress======\\niteration = 1\\tmaxDelta = 0.333682\\niteration = 2\\tmaxDelta = 0.117571\\niteration = 3\\tmaxDelta = 0.073708\\niteration = 4\\tmaxDelta = 0.053260\\niteration = 5\\tmaxDelta = 0.038495\\niteration = 6\\tmaxDelta = 0.028494\\niteration = 7\\tmaxDelta = 0.020819\\niteration = 8\\tmaxDelta = 0.015374\\niteration = 9\\tmaxDelta = 0.011267\\niteration = 10\\tmaxDelta = 0.008305\\niteration = 11\\tmaxDelta = 0.006096\\niteration = 12\\tmaxDelta = 0.004488\\niteration = 13\\tmaxDelta = 0.003297\\niteration = 14\\tmaxDelta = 0.002426\\niteration = 15\\tmaxDelta = 0.001783\\niteration = 16\\tmaxDelta = 0.001311\\niteration = 17\\tmaxDelta = 0.000964\\niteration = 18\\tmaxDelta = 0.000709\\niteration = 19\\tmaxDelta = 0.000521\\niteration = 20\\tmaxDelta = 0.000383'}

.. only:: latex

    .. code::

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
        maxIterations: 20\\n
        maxVal: Infinity\\n
        minVal: -Infinity\\n
        evaluateCost: false\\n
        \\n
        ======Learning Progress======\\n
        iteration = 1\\tmaxDelta = 0.333682\\n
        iteration = 2\\tmaxDelta = 0.117571\\n
        iteration = 3\\tmaxDelta = 0.073708\\n
        iteration = 4\\tmaxDelta = 0.053260\\n
        iteration = 5\\tmaxDelta = 0.038495\\n
        iteration = 6\\tmaxDelta = 0.028494\\n
        iteration = 7\\tmaxDelta = 0.020819\\n
        iteration = 8\\tmaxDelta = 0.015374\\n
        iteration = 9\\tmaxDelta = 0.011267\\n
        iteration = 10\\tmaxDelta = 0.008305\\n
        iteration = 11\\tmaxDelta = 0.006096\\n
        iteration = 12\\tmaxDelta = 0.004488\\n
        iteration = 13\\tmaxDelta = 0.003297\\n
        iteration = 14\\tmaxDelta = 0.002426\\n
        iteration = 15\\tmaxDelta = 0.001783\\n
        iteration = 16\\tmaxDelta = 0.001311\\n
        iteration = 17\\tmaxDelta = 0.000964\\n
        iteration = 18\\tmaxDelta = 0.000709\\n
        iteration = 19\\tmaxDelta = 0.000521\\n
        iteration = 20\\tmaxDelta = 0.000383'}

