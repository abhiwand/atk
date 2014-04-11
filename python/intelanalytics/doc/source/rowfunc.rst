Row Functions
=============

Frame methods like ``map`` or ``filter`` require a function to operate on each
row.  These "row functions" may be lambdas or formally defined functions which
accept a single row parameter.

Row Object
----------

The row function receives a Row argument.  Specific cells in the row are
accessed with square brackets or directly.

>>> # Equivalent statements:
>>> lambda row:  row['a'] > 0
>>> lambda row:  row.a > 0

The Row object is **iterable** on the cell values:

>>> lambda row: ", ".join([c for c in rows])


**is_empty** - determines if the cell is "empty" according to the definition of
its column's data type

>>> row.is_empty('a')
>>> row.is_empty(any)
>>> row.is_empty(any, ['a', 'b'])
>>> row.is_empty(all, ['a', 'b'])


The row is immutable in the context of the row function.  Assignment operations
are prohibited.

>>> # Prohibited!
>>> def my_row_func(row):
        row['a'] = r['b'] * 0.031 - 1
>>> frame.map(my_row_func)
Exception!


>>> frame = BigFrame(SimpleDataSource(schema={'a':int32, 'b':int32}, rows=[(1, 2), (1, 3), (1, 5)]))
>>> frame2= BigFrame(SimpleDataSource(schema={'a':int32, 'b':int32}, columns={'b':[2, 3, 5], 'a':[1, 1, 1]}))
