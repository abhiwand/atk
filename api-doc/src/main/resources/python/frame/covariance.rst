Calculate covariance for exactly two columns.


Parameters
----------
columns : [ str | list of str ]
    The names 2 columns from which to compute the covariance.


Returns
-------
?
    Covariance of the two columns.


Notes
-----
This method applies only to columns containing numerical data.


Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*:

.. code::

    >>> cov = my_frame.covariance(['col_0', 'col_1'])
    >>> print(cov)

