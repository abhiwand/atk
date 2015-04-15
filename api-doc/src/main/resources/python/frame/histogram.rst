Compute the histogram for a column in a frame.

Extended Summary
----------------
Compute the histogram of the data in a column.
The returned value is a Histogram object containing 3 lists one each for:
the cutoff points of the bins, size of each bin, and density of each bin.

Parameters
----------
column_name: str
    Name of column to be evaluated.
num_bins: int (optional)
    Number of bins in histogram.
    If omitted the system will use the Square-root choice
    `math.floor(math.sqrt(frame.row_count))`.
weight_column_name: str (optional)
    Name of column containing weights.
    If omitted the system will weigh all observations equally.
bin_type : str (optional)
    The binning algorithm to use ['equalwidth' | 'equaldepth'].
    If omitted defaults to equalwidth.

Notes
-----
The num_bins parameter is considered to be the maximum permissible number
of bins because the data may dictate fewer bins.
With equal depth binning, for example, if the column to be binned has 10
elements with only 2 distinct values and the *num_bins* parameter is
greater than 2, then the number of actual number of bins will only be 2.
This is due to a restriction that elements with an identical value must
belong to the same bin.

Returns
-------
histogram : Histogram
    A Histogram object containing the result set.
    It contains three attributes:

array of type float : cutoffs
    A list containing the edges of each bin.
array of type float : hist
    A list containing count of the weighted observations found in each bin.
array of type float : density
    A list containing a decimal containing the percentage of
    observations found in the total set per bin.

Examples
--------
Consider the following sample data set:

.. code::

    >>> frame.inspect()

      a:unicode  b:int32
    /--------------------/
        a          2
        b          7
        c          3
        d          9
        e          1

    >>> hist = frame.histogram("b")
    >>> print hist

    Histogram:
        cutoffs: [1, 3, 6, 9],
        hist: [2, 1, 2],
        density: [0.4, 0.2, 0.4]


Plot hist as a bar chart using matplotlib:

.. code::

    >>> import matplotlib.pyplot as plt

    >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - hist.cutoffs[0])

