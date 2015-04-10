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
Given that I have a frame accessed by Frame *my_frame* and the frame has two
columns *a* and *b*.
The "original_data"::

    1-"solo,mono,single"
    2-"duo,double"

.. only:: html

    I run my commands to bring the data in where I can work on it:

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

.. only:: latex

    I run my commands to bring the data in where I can work on it:

    .. code::

        >>> my_csv = CsvFile("original_data.csv", schema=[('a', int32),
        ...    ('b', str)], delimiter='-')
        >>> my_frame = Frame(source=my_csv)

I look at it and see:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /------------------------------/
        1       solo, mono, single
        2       duo, double

Now, I want to spread out those sub-strings in column *b*:

.. code::

    >>> my_frame.flatten_column('b')

Now I check again and my result is:

.. code::

    >>> my_frame.inspect()

      a:int32   b:str
    /------------------/
        1       solo
        1       mono
        1       single
        2       duo
        2       double

