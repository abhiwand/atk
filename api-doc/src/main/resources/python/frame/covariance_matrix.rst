Calculate covariance matrix for two or more columns.

Compute the covariance matrix for two or more columns.

Parameters
----------
columns : [ str | list of str ]
    The names of the column from which to compute the matrix

Returns
-------
A matrix with the covariance values for the columns

Notes
-----
This function applies only to columns containing numerical data.

Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*::

    cov_matirx = my_frame.covariance_matrix(['col_0', 'col_1', 'col_2'])

    cov_matrix.inspect()
