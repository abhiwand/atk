Executes a Gremlin query.

Executes a Gremlin query on an existing graph.
The query returns a list of results in GraphSON format(for vertices or edges)
or JSON (for other results like counts).
GraphSON is a JSON-based format for property graphs which uses reserved keys
that begin with underscores to encode vertex and edge metadata.


Parameters
----------
gremlin : str
    The Gremlin script to execute.


Returns
-------
dict
    Query results and runtime in seconds.


Notes
-----
The query does not support pagination so the results of query should be limited
using the Gremlin range filter [i..j], for example, g.V[0..9] to return the
first 10 vertices.


Examples
--------
Get the first two outgoing edges of the vertex whose source equals 5767244:

.. code::

    >>> mygraph = ia.TitanGraph(...)
    >>> results = mygraph.query.gremlin("g.V('source', 5767244).outE[0..1]")
    >>> print results["results"]

The expected output is a list of edges in GraphSON format:

.. only:: html

    .. code::

        [{u'_label': u'edge', u'_type': u'edge', u'_inV': 1381202500, u'weight': 1, u'_outV': 1346400004, u'_id': u'fDEQC9-1t7m96-1U'},{u'_label': u'edge', u'_type': u'edge', u'_inV': 1365600772, u'weight': 1, u'_outV': 1346400004, u'_id': u'frtzv9-1t7m96-1U'}]

.. only:: latex

    .. code::

        [{u'_label': u'edge',
          u'_type': u'edge',
          u'_inV': 1381202500,
          u'weight': 1,
          u'_outV': 1346400004,
          u'_id': u'fDEQC9-1t7m96-1U'},
         {u'_label': u'edge',
          u'_type': u'edge',
          u'_inV': 1365600772,
          u'weight': 1,
          u'_outV': 1346400004,
          u'_id': u'frtzv9-1t7m96-1U'}]

Get the count of incoming edges for a vertex:

.. code::

    >>> results = mygraph.query.gremlin("g.V('target', 5767243).inE.count()")
    >>> print results["results"]

The expected output is:

.. code::

    [4]

Get the count of name and age properties from vertices:

.. code::

    >>> results = mygraph.query.gremlin("g.V.transform{[it.name, it.age]}[0..10])")
    >>> print results["results"]

The expected output is:

.. code::

    [u'["alice", 29]', u'[ "bob", 45 ]', u'["cathy", 34 ]']

