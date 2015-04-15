Remove columns from the frame.

The data from the columns is lost.

Parameters
----------
columns: [ str | list of str ]
    Column name OR list of column names to be removed from the frame.

Notes
-----
It is not possible to delete all columns from a frame.
At least one column needs to remain.
If it is necessary to delete all columns, then delete the frame.

Examples
--------
For this example, Frame object *my_frame* accesses a frame with
columns *column_a*, *column_b*, *column_c* and *column_d*.

.. code::

    >>> print my_frame.schema
    [("column_a", str), ("column_b", numpy.int32), ("column_c", str), ("column_d", numpy.int32)]

Eliminate columns *column_b* and *column_d*:

.. code::

    >>> my_frame.drop_columns([column_b, column_d])
    >>> print my_frame.schema
    [("column_a", str), ("column_c", str)]


Now the frame only has the columns *column_a* and *column_c*.
For further examples, see: ref:`example_frame.drop_columns`.


