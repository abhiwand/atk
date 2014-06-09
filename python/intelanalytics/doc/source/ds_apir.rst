Row Functions
=============

Frame methods like ``filter`` or ``add_column`` require a function to operate on each row.
These "row functions" may be *lambdas* or formally defined functions which accept a single row parameter.

Row Object
----------

The ``row`` function receives a row argument.
Specific cells in the row are accessed with square brackets or directly.

>>>
Equivalent statements:
>>> lambda row:  row['a'] > 0
>>> lambda row:  row.a > 0

The Row object is iterable on the cells:

>>> lambda row: ", ".join(str(cell_value) for cell_value in row.values())


The function ``is_empty`` determines if the cell is "empty" according to the definition of its column's data type.

>>> row.is_empty('a')
>>> row.is_empty(any)
>>> row.is_empty(any, ['a', 'b'])
>>> row.is_empty(all, ['a', 'b'])

.. caution::

    The row is immutable in the context of the row function.
    Assignment operations are prohibited.

>>>
This is Prohibited!
>>> def my_row_func(row):
...     row['a'] = r['b'] * 0.031 - 1
>>> frame.map(my_row_func)
