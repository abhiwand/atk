Computes a cumulative sum.

Compute a cumulative sum.

A cumulative sum is computed by sequentially stepping through the column
values and keeping track of the current cumulative sum for each value.

Parameters
----------
sample_col : string
    The name of the column from which to compute the cumulative sum

Returns
-------
None

Notes
-----
This function applies only to columns containing numerical data.

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

The cumulative sum for column *obs* is obtained by::

    my_frame.cumulative_sum('obs')

The Frame *my_frame* accesses the original frame that now contains two
columns, *obs* that contains the original column values, and
*obsCumulativeSum* that contains the cumulative percent count::

    my_frame.inspect()

      obs:int32   obs_cumulative_sum:int32
    /--------------------------------------/
         0                          0
         1                          1
         2                          3
         0                          3
         1                          4
         2                          6

