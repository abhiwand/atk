Make sub-graph of interconnected but isolated vertices.

** Experimental Feature **
Connected components.


Parameters
----------
output_property : str
    The name of the column containing the connected component value.


Returns
-------
dict
    Dictionary containing the vertex type as the key and the corresponding
    vertex's frame with a connected component column.
    Call dictionary_name['label'] to get the handle to frame whose vertex type
    is label.


Examples
--------
.. code::

    >>> f= g.graphx_connected_components(output_property = "ccId")

The expected output is like this:

.. code::

    {u'movie': Frame "None"
     row_count = 597
     schema =
       _vid:int64
       _label:unicode
       movie:int32
       Con_Com:int64, u'user': Frame "None"
     row_count = 597
     schema =
       _vid:int64
       _label:unicode
       vertexType:unicode
       user:int32
       Con_Com:int64}

To query:

.. code::

    >>> movie_frame = f['movie']
    >>> user_frame = f['user']

