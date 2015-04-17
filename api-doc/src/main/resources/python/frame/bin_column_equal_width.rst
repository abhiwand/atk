Classify a particular column into same-width groups.

Group rows of data based on the value in a single column and add a label
to identify grouping.

Equal width binning places column values into groups such that the values
in each group fall within the same interval and the interval width for each
group is equal.

Parameters
----------
column_name : str
    The column whose values are to be binned.
num_bins : int (optional)
    The maximum number of bins.
    Default is the Square-root choice
    :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
bin_column_name : str (optional)
    The name for the new column holding the grouping labels.
    Default is ``<column_name>_binned``.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
2)  The num_bins parameter is considered to be the maximum permissible number
    of bins because the data may dictate fewer bins.
    For example, if the column to be binned has 10
    elements with only 2 distinct values and the *num_bins* parameter is
    greater than 2, then the number of actual number of bins will only be 2.
    This is due to a restriction that elements with an identical value must
    belong to the same bin.

Returns
-------
array of floats : cutoffs
   A list of the edges of each bin.

Examples
--------
Given a frame with column *a* accessed by a Frame object *my_frame*:

.. code::

    >>> my_frame.inspect( n=11 )

      a:int32
    /---------/
        1
        1
        2
        3
        5
        8
       13
       21
       34
       55
       89

Modify the frame, adding a column showing what bin the data is in.
The data should be separated into a maximum of five bins and the bin cutoffs
should be evenly spaced.
Note that there may be bins with no members:

.. code::

    >>> cutoffs = my_frame.bin_column_equal_width('a', 5, 'aEWBinned')
    >>> my_frame.inspect( n=11 )

      a:int32     aEWBinned:int32
    /-----------------------------/
        1                 0
        1                 0
        2                 0
        3                 0
        5                 0
        8                 0
       13                 0
       21                 1
       34                 1
       55                 3
       89                 4

The method returns a list of 6 cutoff values that define the edges of each
bin.
Note that difference between the cutoff values is constant:

.. code::

    >>> print cutoffs
    [1.0, 18.6, 36.2, 53.8, 71.4, 89.0]
