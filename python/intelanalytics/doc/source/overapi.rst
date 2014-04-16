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

**Copy Columns**

>>> k = BigFrame(j[['a', 'c']])  # projects columns 'a' and 'b' to new frame k

**Drop Columns**

>>> g.remove_column('b')  # in place

**Rename Columns**

>>> j.rename_column(a='id')
>>> j.rename_column(b='author', c='publisher')

** Cast Columns**

>>> j.cast_column(ia=int32)


4. Engineer
~~~~~~~~~~~

**Map**

Map a function to each row in the frame, produce new column

>>> j.map(lambda row: 1, out='all_ones')  # add new column of all ones
>>> j.map(lambda row: row.a + row.b, out='a_plus_b')

>>> # Fill NA with 0 (rather than sugared j.fillna('a', 0))
>>> j.map(lambda row: 0 if row.is_empty('a') else row.a, out='a')

            # uh, this was a thought once --something about not cancelling the job on an error, but just marking row/cell as None and reporting
            raise FillNone("col value out of range")
            # map or whatever will catch this, log it, add to a count in the report, and fill the entry with a None

>>> # Conditional Linear Transformation
>>> def transform_a(row):
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

>>> j.map(transform_a, out='prior')


**Reduce**

Apply a reduce function to each row in a Frame, or each cell in a column.  The
reducer has two parameters, the **accumulator** value and the **update** value.

>>> x = j.reduce(lambda acc, row_upd: acc + row_upd['a'] - row_upd['b'])

>>> j['a'].reduce(lambda acc, cell_upd: acc + cell_upd)


**Groupby** and **Aggregate**

Group rows together based on matching column values and then apply aggregation
functions on each group, producing a new Frame object

>>> j['a'].avg()

>>> j.groupby('a', 'b').reduce(lambda acc, row_upd: row_a)
>>> j.groupby('a', 'b').c.avg()
>>> j.groupby('a', 'b').map(func1).reduce(func2, out="custom_m1r2")
>>> # j.groupby('a', 'b').aggregate(c_avg=row.c.avg)
>>> j.groupby('a', 'b').aggregate([('c', avg),
                                   ('c', min),
                                   ('c', max, out='c_maximum'),
                                   (reduce, lambda row_accum, row: row_accum + (1 if row.c > 10 else 0)), 'c_over_10')  # custom reducer
                                   ('c', sum)],
                                   exclude_groupby_columns=True),
>>> j.groupby(...).map(...).map().reduce(  )
>>> j.groupby('a', 'b').stats('c')


>>> j.groupby('a', 'b').map(func1).reduce(func2, out="custom_m1r2")

>>> j.groupby('a', 'b').aggregate([('c', avg),
                                   ('c', min),
                                   ('c', max, 'c_maximum'),
                                   ('', (map, func1, reduce, func2), 'c_specialA'),
                                   ('', (reduce, func3), 'c_specialB'),
                                   ('c', sum)],
                                   exclude_groupby_columns=True)

>>> j.groupby('a', 'b').aggregate([( ('c', 'd'), (avg, min, max)),
                                   ('c', min),
                                   ('d', min),
                                   ('c', max, 'c_maximum'),

def my_agg(frame):
    return frame[c].avg(), frame[d].avg(), frame[e].avg()


    j.groupby('a', 'b').reduce(my_agg_reduce, out=('c_avg', 'd_avg', 'e'.avg))




>>> j.map(lambda row:  (row['a'], row['b'], abs(row['a']), abs(row['b']))
>>> k = BigFrame(MapSource(j, lambda row: (row['a'], row['b'], abs(row['a']), abs(row['b'])), out=('a', 'b', 'a_abs', 'b_abs'))



