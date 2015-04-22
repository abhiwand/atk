Determining which vertices are the most important.

** Experimental Feature **
The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

Parameters
----------
output_property : str
    The name of output property to be added to the frame upon completion.
input_edge_labels : list of str (optional)
    The name of edge labels to be considered for pagerank.
    Default is all edges are considered.
max_iterations : int (optional)
    The maximum number of iterations that the algorithm will execute.
    The valid range is all positive int.
    Invalid value will terminate with vertex page rank set to
    reset_probability.
    Default is 20.
convergence_tolerance : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If this parameter is specified, max_iterations is not
    considered as a stopping condition.
    If the change is less than this threshold, the algorithm exits earlier.
    The valid value range is all float and zero.
    Default is 0.001.
reset_probability : float (optional)
    The probability that the random walk of a page is reset.
    Default is 0.15.

Returns
-------
dict(dict) : dict((vertex_dictionary, (label, Frame)), (edge_dictionary,(label,Frame)))
    Dictionary containing a dictionary of labeled vertices and labeled edges.
    For the vertex_dictionary the vertex type is the key and the corresponding
    vertex's frame with a new column storing the page rank value for the vertex
    Call vertex_dictionary['label'] to get the handle to frame whose vertex type
    is label.
    For the edge_dictionary the edge type is the key and the corresponding
    edge's frame with a new column storing the page rank value for the edge
    Call edge_dictionary['label'] to get the handle to frame whose edge type
    is label.

Examples
--------
.. code::

    >>> a = ia.VertexRule("node",frame.followed,{"_label":"a"})
    >>> b = ia.VertexRule("node",frame.follows,{"_label":"b"})
    >>> e1 = ia.EdgeRule("e1",b,a,bidirectional=False)
    >>> e2 = ia.EdgeRule("e2",a,b,bidirectional=False)
    >>> graph = ia.TitanGraph([b,a,e1,a,b,e2],"GraphName")
    >>> output = graph.graphx_pagerank(output_property="PR", max_iterations = 1, convergence_tolerance = 0.001)

The expected output is like this:

.. code::

    {'vertex_dictionary': {u'a': Frame "None"
    row_count = 29
    schema =
      _vid:int64
      _label:unicode
      node:int32
      PR:float64, u'b': Frame "None"
    row_count = 17437
    schema =
      _vid:int64
      _label:unicode
      node:int32
      PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
    row_count = 19265
    schema =
      _eid:int64
      _src_vid:int64
      _dest_vid:int64
      _label:unicode
      PR:float64, u'e2': Frame "None"
    row_count = 19265
    schema =
      _eid:int64
      _src_vid:int64
      _dest_vid:int64
      _label:unicode
      PR:float64}}

To query:

.. only:: html

    .. code::

        >>> a = ia.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ia.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ia.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ia.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ia.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR", max_iterations = 1, convergence_tolerance = 0.001)

        {'vertex_dictionary': {u'a': Frame "None"
        row_count = 29
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64, u'b': Frame "None"
        row_count = 17437
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64, u'e2': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64}}

.. only:: latex

    .. code::

        >>> a = ia.VertexRule("node",frame.followed,{"_label":"a"})
        >>> b = ia.VertexRule("node",frame.follows,{"_label":"b"})
        >>> e1 = ia.EdgeRule("e1",b,a,bidirectional=False)
        >>> e2 = ia.EdgeRule("e2",a,b,bidirectional=False)
        >>> graph = ia.TitanGraph([b,a,e1,a,b,e2],"GraphName")
        >>> output = graph.graphx_pagerank(output_property="PR", max_iterations = 1, convergence_tolerance = 0.001)


        {'vertex_dictionary': {u'a': Frame "None"
        row_count = 29
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64, u'b': Frame "None"
        row_count = 17437
        schema =
          _vid:int64
          _label:unicode
          node:int32
          PR:float64}, 'edge_dictionary': {u'e1': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64, u'e2': Frame "None"
        row_count = 19265
        schema =
          _eid:int64
          _src_vid:int64
          _dest_vid:int64
          _label:unicode
          PR:float64}}
