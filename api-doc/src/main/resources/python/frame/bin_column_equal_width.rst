Bin a particular column into discrete bins which are of the same width.

    Summarize rows of data based on the value in a single column using equal width binnin.

    *   Equal width binning places column values into bins such that the values in
        each bin fall within the same interval and the interval width for each bin
        is equal.

    Parameters
    ----------
    column_name : str
        The column whose values are to be binned.

    num_bins : int (optional)
        The maximum number of bins.
        if omitted the system will use the Square-root choice `math.floor(math.sqrt(frame.row_count))`

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

    Returns
    -------
    cutoffs: array of type float
       a list containing the edges of each bin.

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

        cutoffs = my_frame.bin_column_equal_width('a', 5, 'aEWBinned')
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

        print cutoffs
        [1.0 18.0, 35.0, 52.0, 69.0, 86.0]
