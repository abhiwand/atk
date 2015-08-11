Examples
--------
Given a Frame *my_frame* with data:

.. code::

    >>> my_frame.inspect(11)
      a:int32   b:int32   c:int32
    /-------------------------------/
          200         4        25
          200         5        25
          200         4        25
          200         5        35
          200         6        25
          200         8        35
          200         4        45
          200         4        25
          200         5        25
          200         5        35
          201         4        25

Remove any rows that are identical to a previous row.
The result is a frame of unique rows.
Note that row order may change.

.. code::

    >>> my_frame.drop_duplicates()
    >>> my_frame.inspect(11)
      a:int32   b:int32   c:int32
    /-------------------------------/
          200         4        25
          200         6        25
          200         4        45
          201         4        25
          200         5        35
          200         5        25
          200         8        35

Instead of that, remove any rows that have the same data in columns *a* and 
*c* as a previously checked row:

.. code::

   >>> my_frame.drop_duplicates([ "a", "c"])

The result is a frame with unique values for the combination of columns *a*
and *c*.

.. code::

    >>>my_frame.inspect(11)
      a:int32   b:int32   c:int32
    /-------------------------------/
          200         4        45
          200         4        25
          200         8        35
          201         4        25

Remove any rows that have the same data in column *b* as a previously
checked row:

.. code::

    >>> my_frame.drop_duplicates("b")

The result is a frame with unique values in column *b*.

.. code::

      a:int32   b:int32   c:int32
    /-------------------------------/
          200         8        35  
          200         4        45  
