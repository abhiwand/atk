Cumulative percent count.

Compute a cumulative percent count.

A cumulative percent count is computed by sequentially stepping through
the column values and keeping track of the current percentage of the
total number of times the specified *count_value* has been seen up to
the current value.

Parameters
----------
sample_col : str
    The name of the column from which to compute the cumulative sum.

count_value : str
    The column value to be used for the counts.

Returns
-------
None

Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column named *obs*::

    my_frame.inspect()

      obs:int32
    /-----------/
         0
         1
         2
         0
         1
         2

The cumulative percent count for column *obs* is obtained by::

    my_frame.tally_percent('obs', 1)

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativePercentCount* that contains the cumulative percent count::

    my_frame.inspect()

      obs:int32    obs_tally_percent:float64
    /----------------------------------------/
         0                         0.0
         1                         0.5
         2                         0.5
         0                         0.5
         1                         1.0
         2                         1.0

