..  role:: strikeraw
    
..  role:: strike
        
===================
Python API Overview
===================

>>> from intelanalytics import *

\ 
\ 
Data Types
==========

The following data types are supported:

>>> supported_types:
    bool, bytearray, dict, float32, float64, int32, int64, list, str, string

where ``str`` is ASCII per Python, ``string`` is UTF-8

 
 
Data Sources
============

1. CSV Files - Comma separated values
-------------------------------------

Describe a CSV file with its name and schema at minimum:

>>> schema_ab = [('a', int32), ('b', string)]
>>> csv1 = CsvFile("data.txt", schema_ab)
>>> csv2 = CsvFile("more_data.txt", schema_ab)
>>> csv3 = CsvFile("different_data.txt", [('x', float32), ('', ignore), ('y', int64)])

The schema is the structure of a single line in the CSV File.  It is built from
tuples, each representing a field --its name and type.  (See supported Data Types)

For File schemas, the ``ignore`` type may be specified if the parser should ignore
the field.  When used as a BigFrame data source, the CSV file's schema becomes
the schema of the BigFrame, where the name and type become column descriptions

Optionally choose a delimiter other than ',' and skip the first n lines
of the file, which may be header:

>>> csv4 = CsvFile("bar_data.txt",
                   [('m', int32), ('n', int32), ('o', string)],
                   delimiter='|',
                   skip_header_lines==2)

2. JSON Files
-------------

>>> json1 = JsonFile("json_records.json") # schema TBD
 
 
 
 
BigFrame
========

A BigFrame is a table structure of rows and columns, capable of holding many,
many, ..., many rows.

 
 
1. Create
---------

>>> f = BigFrame()            # empty frame
>>> g = BigFrame(csv1, csv2)  # initialize from a couple CSV File data sources
>>> h = BigFrame(json1)       # initialize from a JSON File data source
>>> f2 = BigFrame(f)          # copy another frame

``append`` will add more rows to a frame from given data sources.  The frame schema
will merge with those of the new data sources.

>>> f.append(CsvFile("bonus_ab_data.txt", schema_ab))
>>> # frame f will get more rows and a new column 'c'
>>> f.append(CsvFile("bonus_abc_data.txt", [('a', int32), ('b', string), ('c', string)]))

 
 
2. Inspect
----------

>>> f.count()               # row count
>>> len(f)                  # column count
>>> f.inspect(5)            # pretty-print first 5 rows
>>> f.take(10, offset=200)  # retrieve a list of 10 rows, starting at row 200

 
 
3. Clean
--------

Drop Rows
~~~~~~~~~

>>> # drop all rows where column 'b' contains a negative number
>>> f.drop(lambda row: row['b'] < 0)

>>> # drop all rows where column 'a' is empty
>>> f.drop(lambda row: row['a'] is None)

>>> # drop all rows where any column is empty
>>> f.drop(lambda row: any([cell is None for cell in row]))

``drop`` takes a predicate function and removes all rows for which the predicate
evaluates True.  It operates in place on the given frame, so it mutates the
frame's content.

``filter`` is like ``drop`` except it removes all the rows for which the
predicate evaluates False.

>>> # keep only those rows where field 'b' is in the range 0 to 10
>>> f2.filter(lambda row: 0 >= row['b'] >= 10)

If we want to hang on to the dropped rows, we can pass in a BigFrame to collect
them.  All of the dropped rows will be appended to that frame. **Not implemented
for 0.8**

>>> r = BigFrame()
>>> f.filter(lambda row: 0 >= row['b'] >= 10, rejected_store=r)

That effectively splits frame ``f`` in two.

See :doc:`rowfunc`

``drop_duplicates`` performs row uniqueness comparisons across the whole table.

>>> f.drop_duplicates(['a', 'b'])  # only columns 'a' and 'b' considered for uniqueness
>>> f.drop_duplicates()            # all columns considered for uniqueness

 
 
Fill Cells
~~~~~~~~~~

>>> f['a'].fillna(800001)
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

 
 
Copy Columns
~~~~~~~~~~~~

>>> f2 = BigFrame(f[['a', 'c']])  # projects columns 'a' and 'c' to new frame f2

A list of columns can be specified using a list to index the frame.

 
 
Remove Columns
~~~~~~~~~~~~~~

>>> f2.remove_column('b')
>>> f2.remove_column(['a', 'c'])

 
 
Rename Columns
~~~~~~~~~~~~~~

>>> f.rename_column(a='id')
>>> f.rename_column(b='author', c='publisher')
>>> f.rename_column({'col-with-dashes': 'no_dashes'})

 
 
Cast Columns
~~~~~~~~~~~~

***WIP*** Thinking something explicit like this instead of allowing schema to be edited directly

>>> f['a'].cast(int32)

 
 
4. Engineer
-----------

Add Column
~~~~~~~~~~

Map a function to each row in the frame, producing a new column

>>> f.add_column(lambda row: 1, 'all_ones') # add new column of all ones
>>> f.add_column(lambda row: row.a + row.b, 'a_plus_b', int32)


>>> # Piecewise Linear Transformation
>>> def transform_a(row):
...     x = row['a']
...     if x is None:
...         return None
...     if 30 <= x <= 127:
...         m, c = 0.0046, 0.4168
...     elif 15 <= x <= 29:
...         m, c = 0.0071, 0.3429
...     elif -127 <= x <= 14:
...         m, c = 0.0032, 0.4025
...     else:
...         return None
...     return m * x + c

>>> f.add_column(transform_a, 'a_lpt')

Create multiple columns at once with ``add_columns``, which requires the function
to return a tuple of cell values for the new frame columns

>>> f.add_columns(lambda row: (abs(row.a), abs(row.b)), ('a_abs', 'b_abs'))  # adds 2 columns

 
 
Map
~~~

``map()`` produces a new BigFrame by applying a function to each row of
a frame or each cell of a column.  It is the functionality as ``add_column`` but
the results go to a new frame instead of being added to the current frame.

>>> f2 = f1['a'].map(lambda cell: abs(cell))
>>> f3 = f1.map_many(lambda row: (abs(row.a), abs(row.b)), ('a_abs', 'b_abs'))
>>> f4 = f1.map_many(lambda row: (abs(row.a), abs(row.b)), (('a_abs', float32), ('b_abs', float32)))

Note: Better name than ``map_many``?
 
 
Reduce
~~~~~~

Apply a reducer function to each row in a Frame, or each cell in a column.  The
reducer has two parameters, the *accumulator* value and the row or cell *update* value.

>>> f.reduce(lambda acc, row_upd: acc + row_upd['a'] - row_upd['b'])

>>> f['a'].reduce(lambda acc, cell_upd: acc + cell_upd)

There are also a bunch of built-in reducers:  count, sum, avg, stdev, etc.
 
 
Groupby (and Aggregate)
~~~~~~~~~~~~~~~~~~~~~~~

(Follows GraphLab's SFrame:
http://graphlab.com/products/create/docs/graphlab.data_structures.html#module-graphlab.aggregate
)

Group rows together based on matching column values and then apply aggregation
functions on each group, producing a new BigFrame object.  Two parameters:
(1) the column(s) to group on and (2) aggregation function(s)

Aggregation on individual columns:
>>> f.groupby(['a', 'b'], { 'c': [agg.avg, agg.sum, agg.stdev], 'd': [agg.avg, agg.sum]})

The name of the new columns are implied.  The previous example would be a new
BigFrame with 7 columns:

Aggregation based on full row:  (*agg.count is the only one supported)
>>> f.groupby(['a', 'b'], agg.count)

Both by column and row together:
>>> f.groupby(['a', 'b'], [agg.count, { 'c': [agg.avg, agg.sum, agg.stdev], 'd': [agg.avg, agg.sum]}])



def groupby(self, column, aggregation):
    """
    Groups rows together based on matching column values and applies aggregation
    functions on each group, producing a new BigFrame object.

    Parameters
    ----------
    column : str, list of string
        The name(s) of the column(s) to be grouped by

    aggregation : row aggregator, dict of cell aggregators, or list of row aggregator and dict of cell aggregators
        The aggregation functions (reducers) to apply to each group.  ByRow aggregators
        use the entire row, reducing all the columns in a group (all columns) to a single value
        `count` is the only supported ByRow aggregator.
        ByCell aggregators just use a specific cell, reducing a column in a group to a single value

    Returns
    -------
    frame : BigFrame
        A new BigFrame containing the aggregation results


A big question is what aggregators must we support for 0.8?

To match GraphLab's SFrame groupby:


avg
count
max
mean
min
quantile
stdev
sum
variance

I would add `distinct` to that list for 0.8


And then from IAT Product Defn:  (any must-haves for 0.8?)

Mean, Median, Mode, Sum, Geom Mean
Skewness, Kurtosis, Cumulative Sum, Cumulative Count, Sum, Count
Minimum, Maximum, Range, Variance, Standard Deviation, Mean Standard Error, Mean Confidence Interval, Outliers
Count Distinct, Distribution
Possibly others I missed


Stuff to consider for >= 1.0

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




Join
~~~~

def join(self, right, left_on, right_on=None, how='left', suffixes=None):
    """
    Create a new BigFrame from a JOIN operation with another BigFrame

    Parameters
    ----------
    right : BigFrame
        Another frame to join with

    left_on : str
        Name of the column for the join for this (left) frame

    right_on : Str, optional
        Name of the column for the join for the right frame, if not
        provided, then the value of left_on is used.

    how : str, optional
        {'left', 'right', 'outer', 'inner'}

    suffixes : 2-ary tuple of str, optional
        Suffixes to apply to overlapping column names on the output frame.
        Default suffixes are ('_L', '_R')


    Returns
    -------
    frame : BigFrame
        The new joined frame

    Examples
    --------
    >>> joined_frame = frame1.join(frame2, 'a')  # left join on column 'a'
    >>> joined_frame = frame1.join(frame2, left_on='b', right_on='book', how='outer')
    """


Flatten
~~~~~~~

``flatten_column`` creates a new BigFrame by copying all the rows of a given
Frame and flattening a particular cell to produce possibly many new rows.

Example:

>>> frame1.inspect()

a:int32    b:str
-------   --------
  1        "solo", "mono", "single"
  2        "duo", "double"

>>> frame2 = frame1.flatten_column('b')
>>> frame2.inspect()

 a:int32    b:str
-------   --------
  1        "solo"
  1        "mono"
  1        "single"
  2        "duo"
  2        "double"


``flatten_column`` requires a single column name as its first parameter.  There
is a second optional function parameter which defines how the splitting should
be done.

>>> frame2 = frame1.flatten('b')  # if column 'a' is natively a list (we don't really support that data type yet)

>>> frame2 = frame1.flatten('b', lambda cell: [item.strip() for item in cell.split(',')])  # could make this the default behavior for string data type


Misc Notes
==========

. uh, this was a thought once --something about not cancelling the job on an
error, but just marking row/cell as None and reporting
``raise FillNone("col value out of range")``
map or whatever will catch this, log it, add to a count in the report, and fill
the entry with a None
