Calculate correlation matrix for two or more columns.


Parameters
----------
columns : [ str | list of str ]
    The names of the column from which to compute the matrix.

Returns
-------
matrix
    A matrix with the correlation values for the columns.


Notes
-----
This method applies only to columns containing numerical data.


Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

    >>> cor_matirx = my_frame.correlation_matrix(['col_0', 'col_1', 'col_2'])
    >>> cor_matrix.inspect()

