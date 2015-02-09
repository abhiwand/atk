The `Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`

Parameters
----------
frame: Frame
    Input frame data

doc: str
    Column Name for documents.  Column should contain a string value.

word: str
    Column name for words.  Column should contain a string value.

word_count: str
    Column name for word count.  Column should contain an int64 value.

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

num_topics : integer (optional)
    The number of topics to identify in the LDA model. Using fewer
    topics will speed up the computation, but the extracted topics
    might be more abstract or less specific; using more topics will
    result in more computation but lead to more specific topics.
    Valid value range is all positive integers.
    The default value is 10.

Returns
-------
results: dict
    Contains three keys

    doc_results : Frame
        Frame with LDA results
    word_results : Frame
        Frame with LDA results
    report : str
       The configuration and learning curve report for Latent Dirichlet
       Allocation as a multiple line string

Examples
--------
::
    model = LdaModel()
    results = model.train(frame, 'doc_column_name', 'word_column_name', 'word_count_column_name', num_topics = 3)

    # results is a dictionary with three keys
    doc_results = results['doc_results']
    word_results = results['word_results']
    report = results['report']

    # inspect the results
    doc_results.inspect()
    word_results.inspect()

    # view the report
    print report

The expected output of results['report'] is like this::

    {u'value': u'======Graph Statistics======\\nNumber of vertices: 12 (doc: 6, word: 6)\\nNumber of edges: 12\\n\\n======LDA Configuration======\\nnumTopics: 3\\nalpha: 0.100000\\nbeta: 0.100000\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: false\\nmaxSupersteps: 20\\nmaxVal: Infinity\\nminVal: -Infinity\\nevaluateCost: false\\n\\n======Learning Progress======\\nsuperstep = 1\\tmaxDelta = 0.333682\\nsuperstep = 2\\tmaxDelta = 0.117571\\nsuperstep = 3\\tmaxDelta = 0.073708\\nsuperstep = 4\\tmaxDelta = 0.053260\\nsuperstep = 5\\tmaxDelta = 0.038495\\nsuperstep = 6\\tmaxDelta = 0.028494\\nsuperstep = 7\\tmaxDelta = 0.020819\\nsuperstep = 8\\tmaxDelta = 0.015374\\nsuperstep = 9\\tmaxDelta = 0.011267\\nsuperstep = 10\\tmaxDelta = 0.008305\\nsuperstep = 11\\tmaxDelta = 0.006096\\nsuperstep = 12\\tmaxDelta = 0.004488\\nsuperstep = 13\\tmaxDelta = 0.003297\\nsuperstep = 14\\tmaxDelta = 0.002426\\nsuperstep = 15\\tmaxDelta = 0.001783\\nsuperstep = 16\\tmaxDelta = 0.001311\\nsuperstep = 17\\tmaxDelta = 0.000964\\nsuperstep = 18\\tmaxDelta = 0.000709\\nsuperstep = 19\\tmaxDelta = 0.000521\\nsuperstep = 20\\tmaxDelta = 0.000383'}
