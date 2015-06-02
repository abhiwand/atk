Examples
--------
Generate the prior and posterior histograms for LBP:

.. only:: html

    .. code::

        >>> my_graph = ia.TitanGraph(...)
        >>> my_graph.ml.loopy_belief_propagation(...)
        >>> results = my_graph.query.histogram(prior_property_list ="value", posterior_property_list = "lbp_posterior",  property_type = "VERTEX_PROPERTY", vertex_type_property_key="vertex_type",  split_types=["TR", "VA", "TE"], histogram_buckets=30)

        >>> results["prior_histograms"]
        >>> results["posterior_histograms"]

.. only:: latex

    .. code::

        >>> my_graph = ia.TitanGraph(...)
        >>> my_graph.ml.loopy_belief_propagation(...)
        >>> results = my_graph.query.histogram(
        ...     prior_property_list ="value",
        ...     posterior_property_list = "lbp_posterior",
        ...     property_type = "VERTEX_PROPERTY",
        ...     vertex_type_property_key="vertex_type",
        ...     split_types=["TR", "VA", "TE"],
        ...     histogram_buckets=30)

        >>> results["prior_histograms"]
        >>> results["posterior_histograms"]

If you want to compute only the prior histograms, use:

.. code::

    >>> results = my_graph.query.histogram(prior_property_list ="value")


