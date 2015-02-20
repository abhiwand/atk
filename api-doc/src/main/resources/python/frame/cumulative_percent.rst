Builds new column in current frame with a cumulative percent sum.

A cumulative percent sum is computed by sequentially stepping through the
column values and keeping track of the current percentage of the total sum
accounted for at the current value.

Parameters
----------
sample_col : str
    The name of the column from which to compute the cumulative percent sum.

Returns
-------
None

Notes
-----
This function applies only to columns containing numerical data.
Although this function will execute for columns containing negative
values, the interpretation of the result will change (for example,
negative percentages).

Examples
--------
Consider Frame *my_frame* accessing a frame that contains a single
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

The cumulative percent sum for column *obs* is obtained by::

    my_frame.cumulative_percent('obs')

The Frame *my_frame* now contains two columns *obs* and
*obsCumulativePercentSum*.
They contain the original data and the cumulative percent sum,
respectively::

    my_frame.inspect()

      obs:int32   obs_cumulative_percent:float64
    /--------------------------------------------/
         0                             0.0
         1                             0.16666666
         2                             0.5
         0                             0.5
         1                             0.66666666
         2                             1.0

