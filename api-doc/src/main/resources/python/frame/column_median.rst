Calculate the (weighted) median of a column.

The median is the least value X in the range of the distribution so that
the cumulative weight of values strictly below X is strictly less than half
of the total weight and the cumulative weight of values up to and including X
is greater than or equal to one-half of the total weight.

All data elements of weight less than or equal to 0 are excluded from the
calculation, as are all data elements whose weight is NaN or infinite.
If a weight column is provided and no weights are finite numbers greater
than 0, None is returned.

Parameters
----------
data_column : str
    The column whose median is to be calculated.

weights_column : str (optional)
    The column that provides weights (frequencies) for the median
    calculation.
    Must contain numerical data.
    Uniform weights of 1 for all items will be used for the calculation
    if this parameter is not provided.

Returns
-------
median : The median of the values
    If a weight column is provided and no weights are finite numbers greater
    than 0, None is returned.
    The type of the median returned is the same as the contents of the data
    column, so a column of Longs will result in a Long median and a column of
    Floats will result in a Float median.

Examples
--------
::

    median = frame.column_median('middling column')


