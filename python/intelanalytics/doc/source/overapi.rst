..  role:: strikeraw

..  role:: strike

===================
Python API Overview
===================

>>> from intelanalytics import *

Data Types
==========

The following data types are supported:

>>> supported_types
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

*Better name than ``map_many``?

Reduce
~~~~~~

Apply a reducer function to each row in a Frame, or each cell in a column.  The
reducer has two parameters, the *accumulator* value and the row or cell *update* value.

>>> f.reduce(lambda acc, row_upd: acc + row_upd['a'] - row_upd['b'])

>>> f['a'].reduce(lambda acc, cell_upd: acc + cell_upd)

There are also a bunch of built-in reducers:  count, sum, avg, stdev, etc.

Groupby (and Aggregate)
~~~~~~~~~~~~~~~~~~~~~~~

***WIP***  current idea:  (follows GraphLab's SFrame)

Group rows together based on matching column values and then apply aggregation
functions on each group, producing a new BigFrame object.  Two parameters:
(1) the column(s) to group on and (2) aggregation function(s)

Aggregation on individual columns:
>>> f.groupby(['a', 'b'], { 'c': [avg, sum, stdev], 'd': [avg, sum]})

The name of the new columns are implied.  The previous example would a new
BigFrame with 7 columns:
  ``"a", "b", "c_avg", "c_sum", "c_stdev", "d_avg", "d_sum"``


Aggregation based on full row:
>>> f.groupby(['a', 'b'], count)

Both by column and row together:
>>> f.groupby(['a', 'b'], count, { 'c': [avg, sum, stdev], 'd': [avg, sum]})


Use 'stats' to get all the basic statistical calculations:
>>> f.groupby(['a', 'b'], { 'c': stats, 'd': stats })
>>> f.groupby(['a', 'b'], stats)  # on all columns besides the groupby columns

Custom reducers:
>>> f.groupby(['a', 'b'], ReducerByRow('my_row_lambda_col', lambda acc, row_upd: acc + row_upd.c - row_upd.d))

Produces a frame with 3 columns: ``"a", "b", "my_row_lambda_col"``

Mixed-combo:   (a little much? this is pretty much exactly what GraphLab is supporting,
except I'm adding custom reducers)
>>> f.groupby(['a', 'b'],
              stats,
              ReducerByRow('my_row_lambda_col', lambda acc, row_upd: acc + row_upd.c - row_upd.d))
              { 'c': ReducerByCell('c_fuzz', lambda acc, cell_upd: acc * cell_upd / 2),
                'd': ReducerByCell('d_fuzz', lambda acc, cell_upd: acc * cell_upd / 3.14)})

Produces a frame with several columns:
``"a", "b", "c_avg", "c_stdev", "c_ ..., "d_avg", "d_stdev", "d_ ..., "my_row_lambda_col", "c_fuzz", "d_fuzz"``


Join
~~~~

***WIP***

``join`` produces a new BigFrame

Legacy Tribeca does this:  (which is pretty much the way pandas does it.  NB: GraphLab's website says "Coming soon" for SFrame joins):
>>> joined_frame = frame1.join(frame2, left_on='a', right_on='a', how='left')

It also supports lists for the "right" side to join several tables in one "shot".  In legacy this amounted to multiple calls to JOIN in the same PIG job.
>>> joined_frame = frame1.join([frame2, frame3], left_on='a', right_on=['a', 'b'], how=['left', 'left'])


We could add...

1. Custom join conditions
>>> joined_frame = frame1.join(frame2, on=lambda f1_row, f2_row: f1_row['rating'] >= f2['rating'] and f1_row['movie'] == f2_row['film'], how='left')

2. Explicit select
>>> joined_frame = frame1.join(frame2, on=lambda f1_row, f2_row: f1_row['rating'] >= f2['rating'] and f1_row['movie'] == f2_row['film'], how='left', select=[frame1['movie'], frame1['rating'], frame2['oscars']])

3. Other?



BTW, Legacy reference:  (NB: last 2 parameters not needed)

def join(self,
         right=None,
         how='left',
         left_on=None,
         right_on=None,
         suffixes=None,
         join_frame_name='',
         overwrite=False):

        """
        Perform SQL JOIN on BigDataFrame

        Syntax is similar to pandas DataFrame.join.

        Parameters
        ----------
        right   : BigDataFrame or list/tuple of BigDataFrame
            Frames to be joined with
        how     : Str
            {'left', 'right', 'outer', 'inner'}, default 'inner'
        left_on : Str
            Columns selected to bed joined on from left frame
        right_on : Str or list/tuple of Str
            Columns selected to bed joined on from right frame(s)
        suffixes : tuple of Str
            Suffixes to apply to columns on the output frame
        join_frame_name : Str
            The name of the BigDataFrame that holds the result of join
        overwrite : Boolean
            True will overwrite the output table if it already exists

        Returns
        -------
        joined : BigDataFrame
        """


>>> f4 = f1.join([f2, f3], left_on='a', right_on=['a', 'x'], how=['left', 'left'])


#Pandas does this (only difference is ``on`` vs. ``left_on``, ``right_on``)
#>>> f5 = f1.join(f2, on='a', how='left')
#>>> f6 = f1.join(f2, on=['a', 'b'], how='left')

Or could try something like this, making the join implicit with the "on" tuples, and adding "select"
>> f7 = f1.join([f2, f3], on=(f1['a'], f2['a'], f3['x']), how='left', select=(f1[['a', 'b', 'c']], f2[['a', 'd'], f3['y']))
#>>> f8 = join((f1['a'], f2['a'], f3['x']), how='left', select=(f1[['a', 'b', 'c']], f2[['a', 'd'], f3['y']))


f1.join([f2, f3], on=lambda f1, f2, f3: f1['a'] == f2['b'] and f1['a'] > f3[x],


   def join(self,
             right=None,
             how='left',
             left_on=None,
             right_on=None,
             suffixes=None,
             join_frame_name='',
             overwrite=False):

        """
        Perform SQL JOIN on BigDataFrame

        Syntax is similar to pandas DataFrame.join.

        Parameters
        ----------
        right   : BigDataFrame or list/tuple of BigDataFrame
            Frames to be joined with
        how     : Str
            {'left', 'right', 'outer', 'inner'}, default 'inner'
        left_on : Str
            Columns selected to bed joined on from left frame
        right_on : Str or list/tuple of Str
            Columns selected to bed joined on from right frame(s)
        suffixes : tuple of Str
            Suffixes to apply to columns on the output frame
        join_frame_name : Str
            The name of the BigDataFrame that holds the result of join
        overwrite : Boolean
            True will overwrite the output table if it already exists

        Returns
        -------
        joined : BigDataFrame
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
