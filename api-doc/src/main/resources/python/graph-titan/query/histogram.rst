Generate histograms.

Generate histograms of prior and posterior probabilities.
The prerequisite is that either LBP, ALS or CGD has been run before this query.

Parameters
----------
prior_property_list : str
    Name of the property containing the vector of prior probabilities.
    The prior probabilities are represented in the graph as a delimited list
    of real values between [0,1], one for each feature dimension.

posterior_property_list : str (optional)
    Name of the property containing the vector of posterior probabilities.
    The posterior probabilities are represented in the graph as a delimited
    list of real values between [0,1], one for each feature dimension.

property_type : str (optional)
    The type of property for the prior and posterior values.
    Valid values are either "VERTEX_PROPERTY" or "EDGE_PROPERTY".
    The default value is "VERTEX_PROPERTY".

vertex_type_property_key : str (optional)
    The property name for vertex type.
    The default value "vertex_type".
    This property indicates whether the data is in the train, validation, or
    test splits.

split_types : list of str (optional)
    The list of split types to include in the report.
    The default value is ["TR", "VA", "TE"] for train (TR), validation (VA),
    and test (TE) splits.

histogram_buckets : int32 (optional)
    The number of buckets to plot in histograms.
    The default value is 30.

Returns
-------
dictionary
    Dictionary containing prior histograms, and, optionally, the posterior
    histograms.
    The dictionary entries are:

    *   prior_histograms : An array of histograms of prior probabilities
        for each feature dimension.
        The histogram comprises of an array of buckets and corresponding counts.
        The buckets are all open to the left except for the last which is
        closed, for example, for the array [1,5,10] the buckets are
        [1, 5] [5, 10].
        The size of the counts array is smaller than the buckets array by 1.
    *   posterior_histograms : An array of histograms of posterior
        probabilities for each feature dimension.


Examples
--------
.. only:: html

    Generate the prior and posterior histograms for LBP::

        graph = BigGraph(...)
        graph.ml.loopy_belief_propagation(...)
        results = graph.query.histogram(prior_property_list ="value", posterior_property_list = "lbp_posterior",  property_type = "VERTEX_PROPERTY", vertex_type_property_key="vertex_type",  split_types=["TR", "VA", "TE"], histogram_buckets=30)

        results["prior_histograms"]
        results["posterior_histograms"]

.. only:: latex

    Generate the prior and posterior histograms for LBP::

        graph = BigGraph(...)
        graph.ml.loopy_belief_propagation(...)
        results = graph.query.histogram( \\
            prior_property_list ="value", \\
            posterior_property_list = "lbp_posterior", \\
            property_type = "VERTEX_PROPERTY", \\
            vertex_type_property_key="vertex_type", \\
            split_types=["TR", "VA", "TE"], \\
            histogram_buckets=30)

        results["prior_histograms"]
        results["posterior_histograms"]

If you want compute only the prior histograms use::

    results = graph.query.histogram(prior_property_list ="value")


