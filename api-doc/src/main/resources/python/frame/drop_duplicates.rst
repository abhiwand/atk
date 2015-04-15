Modify the current frame, removing duplicate rows.

Remove data rows which are the same as other rows.
The entire row can be checked for duplication, or the search for duplicates
can be limited to one or more columns.
This modifies the current frame.

Parameters
----------
columns : [str | list of str] (optional)
    Column name(s) to identify duplicates.
    Default is the entire row is compared.

Examples
--------
Given a Frame *my_frame* with data:

.. code::

    >>> my_frame.inspect(4)

      a:int32  b:int32 c:int32
    /--------------------------/
       200      4       25
       200      5       25
       200      4       25
       200      5       35

Remove any rows that have the same data in column *b* as a previously
checked row:

.. code::

    >>> my_frame.drop_duplicates("b")

The result is a frame with unique values in column *b*.

.. code::

    >>> my_frame.inspect(4)

      a:int32  b:int32 c:int32
    /--------------------------/
       200      4       25
       200      5       25
       200      6       25
       200      8       35

Instead of that, remove any rows that have the same data in columns *a* and 
*c* as a previously checked row:

.. code::

   >>> my_frame.drop_duplicates([ "a", "c"])

The result is a frame with unique values for the combination of columns *a*
and *c*.

.. code::

    >>> my_frame.inspect(4)

      a:int32  b:int32 c:int32
    /--------------------------/
       200      4       25
       201      5       25
       200      8       35
       200      4       45

Or, remove any rows that have the whole row identical:

.. code::

    >>> my_frame.drop_duplicates()

The result is a frame where something is different in every row from every
other row.
Each row is unique.

.. code::

    >>> my_frame.inspect(4)

      a:int32  b:int32 c:int32
    /--------------------------/
       200      4       25
       200      5       25
       200      5       35
       201      4       25

