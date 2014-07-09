=====================
Process Flow Examples
=====================

To load the toolkit::

    from intelanalytics import *

.. _ds_apio_data_types:

----------
Data Types
----------

The following data types are supported:

    bool, bytearray, dict, float32, float64, int32, int64, list, str, string, unicode

where "str" is ASCII per Python, "string" is UTF-8
 
------------
Data Sources
------------

1. CSV Files - Comma separated values
=====================================

Describe a CSV file with its name and schema at minimum::

    schema_ab = [('a', int32), ('b', string)]
    csv1 = CsvFile("data.txt", schema_ab)
    csv2 = CsvFile("more_data.txt", schema_ab)
    csv3 = CsvFile("different_data.txt", [('x', float32), ('', ignore), ('y', int64)])

    raw_csv_data_file = "my_data.csv"
    column_schema_list = [("x", float32), ("y", float32), ("z", bool)]
    csv4 = CsvFile(raw_csv_data_file,
                   column_schema_list,
                   delimiter='|',
                   skip_header_lines=2)

A "schema" is a computer definition of the data in the CSV file.
It is built from tuples, each representing a field -- its name and type (See :ref:`ds_apio_data_types`.)
The schema is used to tell the BigFrame parser what the structure of the data file should be.
When defining schemas, if the parser should ignore the field, the type can be assigned "ignore" and the name can be assigned an empty string ''.

Optionally, the delimiter could be declared using the key word ``delimiter``.
This would be a benefit if the delimiter is something other than a comma.

Another option is to use the key word ``skip_header_lines`` and skip the first *n* lines of the file, so it will ignore lines like headers.

.. TODO::

    2. JSON Files
    =  ==========

    >>> json1 = JsonFile("json_records.json") # schema TBD

    3. XML Files
    =  =========

    >>> xml1 - XmlFile("xml_records.xml") # schema TBD

--------
BigFrame
--------

A BigFrame is a table structure of rows and columns, capable of holding "big data".
 
1. Create
=========
A new frame is created as empty, defined by CSV schema (possibly multiple schemas), or copying (all or a part of) another frame.
A list of columns can be specified using a list::

           f = BigFrame()               # create an empty frame
    my_frame = BigFrame(csv1, csv2)     # create a frame from multiple CSV files
          f2 = BigFrame(my_frame)       # create a new frame, identical to the original
          f3 = BigFrame(f2[['a', 'c']]) # create a new frame with only columns *a* and *c* from the original

What gets returned is not all the data, but a proxy (descriptive pointer) for the data.
Commands such as ``f4 = my_frame`` will only give you a copy of the proxy, pointing to the same frame.

The "append" function can add more rows of data to a frame, typically from a different data source.
If columns are the same in both name and data type, the appended data will go into the existing column.
If the column of data in the new source is not in the original structure, it will be added to the structure and all existing rows will have ``None``
assigned to the new column and the new data will be added to the bottom with ``None`` in all of the previously existing, non-identical columns.
::

    my_frame.append(CsvFile("bonus_ab_data.txt", schema_ab))

2. Inspect
==========
::

    my_frame.count()               # row count
    len(my_frame)                  # column count
    my_frame.inspect(5)            # pretty-print first 5 rows
    my_frame.take(10, offset=200)  # retrieve a list of 10 rows, starting at row 200
 
3. Clean
========

| See :doc:`ds_apir`
| See :doc:`ds_lambda`

.. warning::

    Unless stated otherwise, all of the following functions use the proxy to operate on the data in the given frame,
    so it changes the frame's content rather than return a new frame with the changed data.

Drop Rows
---------
    The function ``drop`` takes a predicate function and removes all rows for which the predicate evaluates to ``True``.

        Drop all rows where column *b* contains a negative number::

            my_frame.drop(lambda row: row['b'] < 0)

        Drop all rows where column *a* is empty::

            my_frame.drop(lambda row: row['a'] is None)

        Drop all rows where any column is empty::

            my_frame.drop(lambda row: any([cell is None for cell in row]))

    The functon ``filter`` is like ``drop`` except it removes all the rows for which the predicate evaluates False.

        Keep only those rows where field *b* is in the range 0 to 10::

            my_frame.filter(lambda row: 0 >= row['b'] >= 10)

    The function ``drop_duplicates`` performs row uniqueness comparisons across the whole table.

        Drop any rows where the data in column *a* and column *b* are duplicates of some previously evaluated row::

            my_frame.drop_duplicates(['a', 'b'])

        Drop any rows where the data matches some previously evaluated row in all columns::

            my_frame.drop_duplicates()
     
.. TODO:: There is no way to fill in the data
    Fill Cells
    - --------

    >>> f['a'].fill(lambda cell: 800001 if cell is None else 800002 if cell < 0 else cell)
    >>> def filler(cell):
    ...     if cell is None:
    ...         return 800001
    ...     if cell < 0:
    ...         return 800002
    ...     if cell > 255:
    ...         return 800003
    ...     return cell
    >>> f['a'].fill(filler)
    
Remove Columns
--------------

    Columns can be removed either with a string matching the column name or a list of strings::

        my_frame.remove_columns('b')
        my_frame.remove_columns(['a', 'c'])

Rename Columns
--------------

    Columns can be renamed by giving the column name and setting it equal to the new name, or by specifying a dictionary entry with the key
    being the existing column name and the value being the new column name::

        my_frame.rename_columns(a='id')
        my_frame.rename_columns(b='author', c='publisher')
        my_frame.rename_columns({'col-with-dashes': 'no_dashes'})

.. TODO:: Cast columns
    Cast Columns
    ~~~~~~~~~~~~

    ***WIP*** Thinking something explicit like this instead of allowing schema to be edited directly

    >>> f['a'].cast(int32)

4. Transform
============

Add Columns
-----------

    Columns can be added to the frame using values (usually manipulated) from other columns as their value.

    Add a column *column3* as an int32 and fill it with the contents of *column1* and *column2* multiplied together::

        my_frame.add_columns(lambda row: row.column1*row.column2, ('column3', int32))

    Add a new column *all_ones* and fill the entire column with the value 1::

        my_frame.add_columns(lambda row: 1, ('all_ones', int32))

    Add a new column *a_plus_b* and fill the entire column with the value of column *a* plus column *b*::

        my_frame.add_columns(lambda row: row.a + row.b, ('a_plus_b', int32))

    Add a new column *a_lpt* and fill the value according to this table:

    +-------------------------------------------+-------------------------------------------+
    | value in column *a*                       | value for column *a_lpt*                  |
    +===========================================+===========================================+
    | None                                      | None                                      |
    +-------------------------------------------+-------------------------------------------+
    | Between 30 and 127 (inclusive)            | column *a* times 0.0046 plus 0.4168       |
    +-------------------------------------------+-------------------------------------------+
    | Between 15 and 29 (inclusive)             | column *a* times 0.0071 plus 0.3429       |
    +-------------------------------------------+-------------------------------------------+
    | Between -127 and 14 (inclusive)           | column *a* times 0.0032 plus 0.4025       |
    +-------------------------------------------+-------------------------------------------+
    | None of the above                         | None                                      |
    +-------------------------------------------+-------------------------------------------+

    An example of a Piecewise Linear Transformation::

        def transform_a(row):
            x = row['a']
            if x is None:
                return None
            if 30 <= x <= 127:
                m, c = 0.0046, 0.4168
            elif 15 <= x <= 29:
                m, c = 0.0071, 0.3429
            elif -127 <= x <= 14:
                m, c = 0.0032, 0.4025
            else:
                return None
            return m * x + c

        my_frame.add_columns(transform_a, float32, 'a_lpt')

    Create multiple columns at once by making a function return a tuple of cell values for the new frame columns, and then providing a tuple of
    types and a tuple of names::

        my_frame.add_columns(lambda row: (abs(row.a), abs(row.b)), (int32, int32), ('a_abs', 'b_abs'))

.. TODO:: There is no map command

    Map (WIP)

    The function ``map()`` produces a new BigFrame by applying a function to each row of a frame or each cell of a column.
    It has the same functionality as ``add_column``, but the results go to a new frame instead of being added to the current frame.

    >>> f2 = f1['a'].map(lambda cell: abs(cell))
    >>> f3 = f1.map_many(lambda row: (abs(row.a), abs(row.b)), ('a_abs', 'b_abs'))
    >>> f4 = f1.map_many(lambda row: (abs(row.a), abs(row.b)), (('a_abs', float32), ('b_abs', float32)))

.. TODO:: Note: Better name than ``map_many``?
 
.. TODO:: There is no reduce command

    Reduce (WIP)

    Apply a reducer function to each row in a Frame, or each cell in a column.
    The reducer has two parameters, the *accumulator* value and the row or cell *update* value.

    >>> f.reduce(lambda acc, row_upd: acc + row_upd['a'] - row_upd['b'])
    >>> f['a'].reduce(lambda acc, cell_upd: acc + cell_upd)

    There are also a bunch of built-in reducers:  count, sum, avg, stdev, etc.
     

Groupby (and Aggregate)
-----------------------

    Group rows together based on matching column values and then apply aggregation
    functions on each group, producing a **new** BigFrame object.
    Two parameters:

        (1) the column(s) to group on
        (2) aggregation function(s)

    Aggregation based on columns:

        | Given a frame with columns *a*, *b*, *c*, and *d*, minimum:
        | Group by unique values in columns *a* and *b*;
        | Average the grouped values in column *c* and save it in a new column *c_avg*;
        | Add up the grouped values in column *c* and save it in a new column *c_sum*;
        | Get the standard deviation of the grouped values in column *c* and save it in a new column *c_stdev*;
        | Average the grouped values in column *d* and save it in a new column *d_avg*;
        | Add up the grouped values in column *d* and save it in a new column *d_sum*::

            my_frame.groupby(['a', 'b'], { 'c': [agg.avg, agg.sum, agg.stdev], 'd': [agg.avg, agg.sum]})

        Note:
            The only columns in the new frame will be the grouping columns and the generated columns. In this case, regardless of the original frame size,
            you will get seven columns::

                *a*
                *b*
                *c_avg*
                *c_sum*
                *c_stdev*
                *d_avg*
                *d_sum*

    Aggregation based on full row:

        | Given a frame with columns *a*, and *b*, minimum:
        | Group by unique values in columns *a* and *b*;
        | Count the number of rows in each group and put that value in column *count*::

            my_frame.groupby(['a', 'b'], agg.count)

        Note:
            agg.count is the only one supported at this time

    Aggregation based on both column and row together:

        | Given a frame with columns *a*, *b*, *c*, and *d*, minimum:
        | Group by unique values in columns *a* and *b*;
        | Count the number of rows in each group and put that value in column *count*:
        | Average the grouped values in column *c* and save it in a new column *c_avg*;
        | Add up the grouped values in column *c* and save it in a new column *c_sum*;
        | Get the standard deviation of the grouped values in column *c* and save it in a new column *c_stdev*;
        | Average the grouped values in column *d* and save it in a new column *d_avg*;
        | Add up the grouped values in column *d* and save it in a new column *d_sum*::

            my_frame.groupby(['a', 'b'], [agg.count, { 'c': [agg.avg, agg.sum, agg.stdev], 'd': [agg.avg, agg.sum]}])

        Supported aggregation functions:

            avg
            count
            max
            mean
            min
            quantile
            stdev
            sum
            variance
            distinct


.. ifconfig:: internal_docs

    (Follows GraphLab's SFrame:
    http://graphlab.com/products/create/docs/graphlab.data_structures.html#module-graphlab.aggregate)

    And then from IAT Product Defn:  (any must-haves for 0.8?)

    Mean, Median, Mode, Sum, Geom Mean
    Skewness, Kurtosis, Cumulative Sum, Cumulative Count, Sum, Count
    Minimum, Maximum, Range, Variance, Standard Deviation, Mean Standard Error, Mean Confidence Interval, Outliers
    Count Distinct, Distribution
    Possibly others I missed


.. TODO:: Stuff to consider for >= 1.0

    . Use a 'stats' builtin to get all the basic statistical calculations:

    >>> f.groupby(['a', 'b'], { 'c': stats, 'd': stats })
    >>> f.groupby(['a', 'b'], stats)  # on all columns besides the groupby columns

    . Use lambdas for custom groupby operations --i.e. first parameter can be a lambda

    . Customer reducers:

    >>> f.groupby(['a', 'b'], ReducerByRow('my_row_lambda_col', lambda acc, row_upd: acc + row_upd.c - row_upd.d))

    Produces a frame with 3 columns: ``"a", "b", "my_row_lambda_col"``

    . Mixed-combo:
    >>> f.groupby(['a', 'b'],
    >>>           stats,
    >>>           ReducerByRow('my_row_lambda_col', lambda acc, row_upd: acc + row_upd.c - row_upd.d))
    >>>           { 'c': ReducerByCell('c_fuzz', lambda acc, cell_upd: acc * cell_upd / 2),
    >>>             'd': ReducerByCell('d_fuzz', lambda acc, cell_upd: acc * cell_upd / 3.14)})

    Produces a frame with several columns:
    ``"a", "b", "c_avg", "c_stdev", "c_ ..., "d_avg", "d_stdev", "d_ ..., "my_row_lambda_col", "c_fuzz", "d_fuzz"``


.. TODO:: Functions do not work well except in .py files

Join
----

    Create a **new** BigFrame from a JOIN operation with another BigFrame

    Given two frames *my_frame* (columns *a*, *b*, *c*) and *your_frame* (columns *b*, *c*, *d*);
    Column *b* in both frames is a unique identifier used to tie the two frame together;
    Join the *your_frame* to *my_frame*;
    Include all data from *my_frame* and only that data in *your_frame* which has a value in *b* that matches a value in *my_frame* *b*::

        our_frame = my_frame.join(your_frame, 'b', how='left')

    Result is *our_frame* with columns *a*, *b*, *c_L*, *c_R*, and *d*.
 
    Include only data from *my_frame* and *your_frame* which have matching values in *b*::

        our_frame = my_frame.join(your_frame, 'b')

    Result is *our_frame* with columns *a*, *b*, *c_L*, *c_R*, and *d*.

    Include any data from *my_frame* and *your_frame* which do not have matching values in *b*::

        our_frame = my_frame.join(your_frame, 'b', how='outer')

    Result is *our_frame* with columns *a*, *b*, *c_L*, *c_R*, and *d*.

    Given that column *b* in *my_frame* and column *c* in *your_frame* are the tie:
    Include all data from *your_frame* and only that data in *my_frame* which has a value in *b* that matches a value in *your_frame* *c*::

        our_frame = my_frame.join(your_frame, left_on='b', right_on='c', how='right')

    Result is *our_frame* with columns *a*, *b_L*, *b_R*, *c_L*, *c_R*, and *d*.

Flatten
-------

    The function ``flatten_column`` creates a **new** BigFrame by copying all the rows of a given frame and splitting a particular cell to produce
    possibly many new rows.

    Example::

        my_frame.inspect()

        a:int32   b:str
        -------   ------------------------
          1       "solo", "mono", "single"
          2       "duo", "double"

        your_frame = my_frame.flatten_column('b')
        your_frame.inspect()

        a:int32   b:str
        -------   --------
          1       "solo"
          1       "mono"
          1       "single"
          2       "duo"
          2       "double"


.. TODO:: future flatter?

    The ``flatten_column`` function requires a single column name as its first parameter.
    There is a second optional function parameter which defines how the splitting should be done::

        frame2 = frame1.flatten('b', lambda cell: [item.strip() for item in cell.split(',')])  # could make this the default behavior for string data type

.. TODO:: Miscellaneous Notes
    Misc Notes

    . uh, this was a thought once --something about not cancelling the job on an
    error, but just marking row/cell as None and reporting
    ``raise FillNone("col value out of range")``
    map or whatever will catch this, log it, add to a count in the report, and fill
    the entry with a None
