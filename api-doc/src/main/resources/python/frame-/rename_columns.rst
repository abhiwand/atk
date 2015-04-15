Rename one or more columns in current frame.

Parameters
----------
column_names : dictionary of str pairs
    The name pair (existing name, new name).

Notes
-----
Unicode in column names is not supported and will likely cause the
drop_frames() method (and others) to fail!

Examples
--------
Start with a frame with columns *Wrong* and *Wong*.

.. code::

    >>> print my_frame.schema
    [('Wrong', str), ('Wong', str)]

Rename the columns to *Right* and *Wite*:

.. code::

    >>> my_frame.rename_columns({"Wrong": "Right, "Wong": "Wite"})

Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

.. code::

    >>> print my_frame.schema
    [('Right', str), ('Wite', str)]
