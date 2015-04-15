Count number of times a value is seen.

A cumulative count is computed by sequentially stepping through the column
values and keeping track of the the number of times the specified
*count_value* has been seen up to the current value.

Parameters
----------
sample_col : str
    The name of the column from which to compute the cumulative count.
count_value : str
    The column value to be used for the counts.

Examples
--------
Consider Frame *my_frame*, which accesses a frame that contains a single
column *obs*:

.. code::

    >>> my_frame.inspect()

      obs:int32
    /-----------/
        0
        1
        2
        0
        1
        2

The cumulative count for column *obs* using *count_value = 1* is obtained by:

.. code::

    >>> my_frame.tally('obs', '1')

The Frame *my_frame* accesses a frame which now contains two columns *obs*
and *obsCumulativeCount*.
Column *obs* still has the same data and *obsCumulativeCount* contains the
cumulative counts:

.. code::

    >>> my_frame.inspect()

      obs:int32        obs_tally:int32
    /----------------------------------/
         0                      1
         1                      1
         2                      1
         0                      2
         1                      2
         2                      2

