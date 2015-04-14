Classify column into discrete groups with the same frequency.

Group rows of data based on the value in a single column and add a label
to identify grouping.

*   Equal depth binning attempts to label rows such that each bin contains the
    same number of elements.
    For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin
    number is determined by:

    .. math::

        ceiling \left( n * \frac {f(C)}{m} \right)

    where :math:`f` is a tie-adjusted ranking function over values of
    :math:`C`.
    If there are multiples of the same value in :math:`C`, then their
    tie-adjusted rank is the average of their ordered rank values.

Parameters
----------
column_name : str
    The column whose values are to be binned.
num_bins : int (optional)
    The maximum number of bins.
    Default is the Square-root choice:
    :math:`\lfloor\sqrt{m}\rfloor`.
bin_column_name : str (optional)
    The name for the new binned column.
    Default is '<column_name>_binned'.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
2)  The num_bins parameter is considered to be the maximum permissible number
    of bins because the data may dictate fewer bins.
    For example, if the column to be binned has a quantity of :math"`X`
    elements with only 2 distinct values and the *num_bins* parameter is
    greater than 2, then the actual number of bins will only be 2.
    This is due to a restriction that elements with an identical value must
    belong to the same bin.

Returns
-------
array of floats | cutoffs
    A list containing the edges of each bin.

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
The data should be grouped into a maximum of five bins.
Note that each bin will have the same quantity of members (as much as
possible):

.. code::

    >>> cutoffs = my_frame.bin_column_equal_depth('a', 5, 'aEDBinned')
    >>> my_frame.inspect( n=11 )

      a:int32     aEDBinned:int32
    /-----------------------------/
          1                   0
          1                   0
          2                   1
          3                   1
          5                   2
          8                   2
         13                   3
         21                   3
         34                   4
         55                   4
         89                   4

    >>> print cutoffs
    [1.0, 2.0, 5.0, 13.0, 34.0, 89.0]
