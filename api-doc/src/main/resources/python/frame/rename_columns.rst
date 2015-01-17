Rename one or more columns.

Renames columns in a frame.

Parameters
----------
column_names : dictionary of str pairs
    The name pair (existing name, new name).

Notes
-----
Unicode in column names is not supported and will likely cause the
drop_frames() function (and others) to fail!

Examples
--------
Start with a frame with columns *Wrong* and *Wong*.
Rename the columns to *Right* and *Wite*::

    my_frame.rename_columns({"Wrong": "Right, "Wong": "Wite"})

Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

