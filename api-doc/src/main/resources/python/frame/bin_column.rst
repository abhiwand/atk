Bin a particular columnNone.

    Summarize rows of data based on the value in a single column.
    Two types of binning are provided: `equalwidth` and `equaldepth`.

    *   Equal width binning places column values into bins such that the values in
        each bin fall within the same interval and the interval width for each bin
        is equal.
    *   Equal depth binning attempts to place column values into bins such that
        each bin contains the same number of elements.
        For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin
        number is determined by:

        .. math::

            ceiling \\left( n * \\frac {f(C)}{m} \\right)

        where :math:`f` is a tie-adjusted ranking function over values of
        :math:`C`.
        If there are multiples of the same value in :math:`C`, then their
        tie-adjusted rank is the average of their ordered rank values.

    Parameters
    ----------
    column_name : str
        The column whose values are to be binned.

    num_bins : int
        The maximum number of bins.

    bin_type : str (optional)
        The binning algorithm to use ['equalwidth' | 'equaldepth'].

    bin_column_name : str (optional)
        The name for the new binned column.
        If unassigned, bin_column_name defaults to '<column_name>_binned'.

    Notes
    -----
    1)  Unicode in column names is not supported and will likely cause the
        drop_frames() function (and others) to fail!
    #)  The num_bins parameter is considered to be the maximum permissible number
        of bins because the data may dictate fewer bins.
        With equal depth binning, for example, if the column to be binned has 10
        elements with only 2 distinct values and the *num_bins* parameter is
        greater than 2, then the number of actual number of bins will only be 2.
        This is due to a restriction that elements with an identical value must
        belong to the same bin.

    Examples
    --------
    For this example, we will use a frame with column *a* accessed by a Frame object *my_frame*::

        my_frame.inspect( n=11 )

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

    Modify the frame with a column showing what bin the data is in.
    The data should be separated into a maximum of five bins and the bins should
    be *equalwidth*::

        my_frame.bin_column('a', 5, 'equalwidth', 'aEWBinned')
        my_frame.inspect( n=11 )

          a:int32     aEWBinned:int32
        /-----------------------------/
           1                   1
           1                   1
           2                   1
           3                   1
           5                   1
           8                   1
          13                   1
          21                   2
          34                   2
          55                   4
          89                   5

    Modify the frame with a column showing what bin the data is in.
    The data should be separated into a maximum of five bins and the bins should
    be *equaldepth*::


        my_frame.bin_column('a', 5, 'equaldepth', 'aEDBinned')
        my_frame.inspect( n=11 )

          a:int32     aEDBinned:int32
        /-----------------------------/
           1                   1
           1                   1
           2                   1
           3                   2
           5                   2
           8                   3
          13                   3
          21                   4
          34                   4
          55                   5
          89                   5



