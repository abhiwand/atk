Spread data to multiple rows based on cell data.

Splits cells in the specified column into multiple rows according to a string
delimiter.
New rows are a full copy of the original row, but the specified column only
contains one value.
The original row is deleted.

Parameters
----------
column : str
    The column to be flattened.
delimiter : str (optional)
    The delimiter string.
    Default is comma (,).

Examples
--------
Given a data file::

    1-"solo,mono,single"
    2-"duo,double"

    The commands to bring the data into a frame, where it can be worked on:

.. only:: html

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

.. only:: latex

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32),
        ...    ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

Looking at it:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /-------------------------------/
        1       solo, mono, single
        2       duo, double

Now, spread out those sub-strings in column *b*:

.. code::

    >>> my_frame.flatten_column('b')

Check again:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /------------------/
        1       solo
        1       mono
        1       single
        2       duo
        2       double

