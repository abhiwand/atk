Classify data into user-defined groups.

Summarize rows of data based on the value in a single column by sorting them into bins
based off of a list of bin cutoff points.

Parameters
----------
column_name : str
    The column whose values are to be binned.

cutoffs : array of float values
   Array of values containing bin cutoff points. It must be a single value and monotonically increasing.

include_lowest : bool (optional)
    Indicating whether the intervals should be lower or upper inclusive. True indicates
    that the lower bound of the bin is inclusive. False indicates that the upper bound is inclusive.
    Default is True.

strict_binning : bool (optional)
    Indicates if values outside of the cutoffs array should be binned. if True each value lesser than cutoffs[0]
    or greater than cutoffs[-1] will be assigned a bin value of -1. if False values lesser than cutoffs[0]
    will be included in the first bin while values greater than cutoffs[-1] will be included in the final bin.
    Default is False

bin_column_name : str (optional)
    The name for the new binned column.
    If unassigned, bin_column_name defaults to '<column_name>_binned'.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() function (and others) to fail!

2)  Bins IDs are 0-index: the lowest bin number is 0.

3)  The first and last cutoffs are always included in the bins: when include_lowest=True, the last bin includes
    both cutoffs; when include_lowest=False the first bin (bin 0) includes both cutoffs.


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

Modify the frame with a column showing what bin the data is in
The data values should use strict_binning::

    my_frame.bin_column('a', [5,12,25,60], include_lowest=True, strict_binning=True, bin_column_name='binned')
    my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
       1                   -1
       1                   -1
       2                   -1
       3                   -1
       5                   0
       8                   0
      13                   1
      21                   1
      34                   2
      55                   2
      89                   -1

Modify the frame with a column showing what bin the data is in.
The data value should not use strict_binning::

    my_frame.bin_column('a', [5,12,25,60], include_lowest=True, strict_binning=False, bin_column_name='binned')
    my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
       1                   0
       1                   0
       2                   0
       3                   0
       5                   0
       8                   0
      13                   1
      21                   1
      34                   2
      55                   2
      89                   2


Modify the frame with a column showing what bin the data is in.
The bins should be lower inclusive::

    my_frame.bin_column('a', [1,5,34,55,89], include_lowest=True, strict_binning=False, bin_column_name='binned')
    my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
       1                   0
       1                   0
       2                   0
       3                   0
       5                   1
       8                   1
      13                   1
      21                   1
      34                   2
      55                   3
      89                   3

Modify the frame with a column showing what bin the data is in.
The bins should be upper inclusive::

    my_frame.bin_column('a', [1,5,34,55,89], include_lowest=False, strict_binning=True, bin_column_name='binned')
    my_frame.inspect( n=11 )

      a:int32     binned:int32
    /---------------------------/
       1                   0
       1                   0
       2                   0
       3                   0
       5                   0
       8                   1
      13                   1
      21                   1
      34                   1
      55                   2
      89                   3
