..  role:: strikeraw

..  role:: strike

Python API Overview
===================

>>> from intelanalytics import *

Data Sources
------------

1. CSV Files - Comma separated values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Describe a CSV file with its name and schema at minimum:

>>> schema_ab = [('a', int32), ('b', string)]
>>> csv1 = CsvFile("data.txt", schema_ab)
>>> csv2 = CsvFile("more_data.txt", schema_ab)
>>> csv3 = CsvFile("different_data.txt", [('x', float32), ('', ignore), ('y', int64)])

The schema is the structure of a single line in the CSV File.  It is built from
tuples, each representing a field --its name and type.  (See link:todo-DataTypes for
supported Data Types)

Optionally choose a delimiter other than ',' and skip the first n lines
of the file, which may be header:

>>> csv4 = CsvFile("bar_data.txt",
                   [('m', int32), ('n', int32), ('o', string)],
                   delimiter='|',
                   skip_header_lines==2)


2. JSON Files
~~~~~~~~~~~~~

>>> json1 = JsonFile("json_records.json") # schema TBD



BigFrame
--------

A BigFrame is a table structure of rows and columns, capable of holding many,
many, ..., many rows.

1. Create
~~~~~~~~~

>>> f = BigFrame()            # empty frame
>>> g = BigFrame(csv1, csv2)  # initialize from a couple CSV File data sources
>>> h = BigFrame(json1)       # initialize from a JSON File data source
>>> i = BigFrame(f)           # copy another frame

``append`` will add more rows to a frame from given data sources.  The frame schema
will merge with those of the new data sources.

>>> f.append(CsvFile("bonus_ab_data.txt", schema_ab))
>>> # frame f will get more rows and a new column 'c'
>>> f.append(CsvFile("bonus_abc_data.txt", [('a', int32), ('b', string), ('c', string)]))

2. Inspect
~~~~~~~~~~

>>> f.count()      # row count
>>> len(f)         # column count
>>> f.inspect(5)   # print 5 rows chosen randomly


3. Clean
~~~~~~~~

**Drop Rows**

>>> # drop all rows where column 'a' is empty
>>> f.drop(lambda row: row.is_empty('a'))

``drop`` takes a predicate function and removes all rows for which the predicate
evaluates True.  It operates in place, so it is destructive to the frame.  Be
advised to make a copy of the frame before cleaning.

>>> # drop all rows where any column is empty
>>> j = BigFrame(f)
#>>> j.drop(lambda row: any([is_empty(c) for c in row]))
>>> j.drop(lambda row: row.is_empty('a'))
>>> j.drop(lambda row: row.is_empty(any, ('a', 'b'), any))
>>> j.drop(lambda row: row.is_empty('', all))
>>> j.drop(lambda row: row.is_empty('', any))
>>> j.drop(lambda row: row.is_empty(('a', 'b'), all)
>>> j.drop(lambda row: row.is_empty(any, ('a', 'b')))

``filter`` is like drop except it removes all the rows for which the predicate
evaluates False.

>>> # drop all rows where field 'b' is out of range 0 to 10
>>> j.filter(lambda row: 0 >= row['b'] >= 10)

If we want to hang on to the dropped rows, we can pass in a BigFrame to collect
them.  All of the dropped rows will be appended to that frame.

>>> r = BigFrame()
>>> j.filter(lambda row: 0 >= row['b'] >= 10, rejected_store=r)

That effectively splits frame ``j`` in two.

See :doc:`rowfunc`

**Drop Duplicates**

(Probably solve with aggregation, add this only for sugar)

:strike:`Drop all rows which are duplicates.`

..  container:: strikeraw

    # >>> j.drop_duplicates(['a', 'b'])  # only columns 'a' and 'b' considered for uniqueness
    # >>> j.drop_duplicates()            # all columns considered for uniqueness

**Fill Cells**

Fill cells with map jobs

>>> j['a'].fillna(800001)

**Drop Columns**

>>> del g['b']  # in place

**Copy Columns**

>>> k = BigFrame(j[['a', 'c']])  # projects columns 'a' and 'b' to new frame k

**Rename Columns**

>>> j.rename_column(a='id')
>>> j.rename_column(b='author', c='publisher')


**Add Columns**

>>> # Add an empty column
>>> j.add_column('d', int32)
>>>
>>> # Add a column 'e' which is the absolute value of column 'a'
>>> j.add_column('e', int32, lambda row: abs(row['a']))

>>> # Add a column 'e' which is the absolute value of column 'a'
>>> j.add_column('e', int32, lambda row: abs(row['a']))

** Map **


Map a function to each row in the frame

>>> j.map(func)
>>> j.map(lambda row: 1).assign('all_ones')  # add new column of all ones

>>> Fill NA with 0
>>> j.map(lambda row: 0 if row.is_empty(a) else row.a).assign('a')
>>> # or with sugar
>>> j.fillna('a', 0)

def func(row):
    if row.a > 10:
        row

>>> j.apply(func)
vs.
>>> j.map(func).reduce(func)  # map and reduce do not have assignments in them

# do we want to let user have arbitrary powers at the row level?
# or consider the row immutable?
# map - row is immutable, map just produce a map object, requires an assign or reduce call to effectuate
# reduce
# assign

>>> j.map(func).assign('d', 'e')
>>> j.map(func).assign(**{a:int32, b:int32})

>>> def my_row_op(col):
        def row_op(row):
            if row[col] is None:
                return None
            if 30 <= row[col] <= 127:
                a, b = 0.0046, 0.4168
            elif 15 <= row[col] <= 29:
                a, b = 0.0071, 0.3429
            elif -127 <= row[col] <= 14:
                a, b = 0.0032, 0.4025
            else:
                return None

            return a*row[col] + b
        return row_op


>>> j.map(my_row_op('a')).assign('prior')


>>> j.map(func).reduce(avg)


            raise FillNone("col value out of range")
            # map or whatever will catch this, log it, add to a count in the report, and fill the entry with a None


>>> j['a'].reduce(lambda row_accum, row: row_accum + row)
>>> j['a'].avg()
>>> j.groupby('a', 'b').reduce(lambda row1, row2: )
>>> j.groupby('a', 'b').c.avg()
>>> j.groupby('a', 'b').map(lambda( row1c.avg()
>>> j.groupby('a', 'b').aggregate(c_avg=row.c.avg)
>>> j.groupby('a', 'b').aggregate([('c', avg),
                                   ('c', min),
                                   ('c', max, out='c_maximum'),
                                   ('*', lambda row_accum, row: row_accum + (1 if row.c > 10 else 0)), 'c_over_10')  # custom reducer
                                   ('c', sum)],
                                   exclude_groupby_columns=True),
>>> j.groupby('a', 'b').stats('c')

** Reduce **

>>> j.map(func1).reduce(func2)



