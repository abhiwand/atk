Calculate correlation for two columns of current frame.

Parameters
----------
columns : [ str | list of str ]
    The names of 2 columns from which to compute the correlation

Returns
-------
correlation of the two columns

Notes
-----
This method applies only to columns containing numerical data.

Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

    >>> cov = my_frame.correlation(['col_0', 'col_1'])

    >>> print(cov)

