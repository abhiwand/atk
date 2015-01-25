Remove columns.

Remove columns from the frame.
The data from the columns is lost.

Parameters
----------
columns: [ str | list of str ]
    column name OR list of column names to be removed from the frame

Returns
-------
None

Notes
-----
Cannot delete all columns from a frame. At least one column needs to remain.
If you want to delete all columns, then please delete the frame

Examples
--------
For this example, Frame object *my_frame* accesses a frame with
columns *column_a*, *column_b*, *column_c* and *column_d*.
Eliminate columns *column_b* and *column_d*::

    my_frame.drop_columns([column_b, column_d])

Now the frame only has the columns *column_a* and *column_c*.
For further examples, see: ref: `example_frame.drop_columns`.


