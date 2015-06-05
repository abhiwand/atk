Compacts data from multiple rows based on cell data.

Groups together cells in all columns (less the composite key) using "," as string delimiter.
The original rows are deleted. Thr grouping takes place based on a composite key passed as arguments.


Parameters
----------
column : List<str>
    The column list representing the composite key.
delimiter : str (optional)
    The delimiter string.
    Default is comma (,).


Examples
--------
Given a data file::

    user1 1/1/2015 1 70
    user1 1/1/2015 2 60
    user2 1/1/2015 1 65

The commands to bring the data into a frame, where it can be worked on:

.. only:: html

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', str), ('b', str),('c', int32) ,('d', int32]))
        >>> my_frame = Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = CsvFile("unflatten_column.csv", schema=[('a', str), ('b', str),('c', int32) ,('d', int32)])
        >>> my_frame = Frame(source=my_csv)

Looking at it:

.. code::

    >>> my_frame.inspect()

      a:str        b:str       c:int32       d:int32
    /----------------------n-------------------------/
       user1       1/1/12015   1             70
       user1       1/1/12015   2             60
       user2       1/1/2015    1             65

Unflatten the data using columns a & b:

.. code::

    >>> my_frame.unflatten_column({'a','b'})

Check again:

.. code::

    >>> my_frame.inspect()

      a:str        b:str       c:int32       d:int32
    /-----------------------------------------------/
       user1       1/1/12015   1,2           70,60
       user2       1/1/2015    1             65

