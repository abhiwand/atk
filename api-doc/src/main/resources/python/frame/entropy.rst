Column Shannon entropy.

Calculate the Shannon entropy of a column.
The column can be weighted.
All data elements of weight <= 0 are excluded from the calculation, as are
all data elements whose weight is NaN or infinite.
If there are no data elements with a finite weight greater than 0,
the entropy is zero.

Parameters
----------
data_column : str
    The column whose entropy is to be calculated

weights_column : str (optional)
    The column that provides weights (frequencies) for the entropy
    calculation.
    Must contain numerical data.
    Uniform weights of 1 for all items will be used for the calculation if
    this parameter is not provided.

Returns
-------
float64
    entropy

Examples
--------
::

    entropy = frame.entropy('data column')
    weighted_entropy = frame.entropy('data column', 'weight column')


