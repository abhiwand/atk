Get a sorted subset of the data.

Take the first k (sorted) rows for the currently active Frame. Rows are
sorted by column values in either ascending or descending order.

Returning the first k (sorted) rows is more efficient than sorting the
entire frame when k is much smaller than the number of rows in the frame.



Parameters
----------
k : int
    The number of sorted rows to copy from the currently active Frame.
columns : list of tuples
    Each tuple is a column name, and a boolean value that indicates
    whether to sort the column in ascending or descending order.
reduce_tree_depth : int (optional)
    Advanced tuning parameter which determines the depth of the
    reduce-tree for the sorted_k plugin. This plugin uses Spark's treeReduce()
    for scalability. The default depth is 2.

Returns
-------
Frame : A new frame with the first k sorted rows from the original frame.

Notes
-----
The number of sorted rows (k) should be much smaller than the number of rows
in the original frame.

In particular:

1) The number of sorted rows (k) returned should fit in Spark driver memory.
  The maximum size of serialized results that can fit in the Spark driver is
  set by the Spark configuration parameter *spark.driver.maxResultSize*.

2) If you encounter a Kryo buffer overflow exception, increase the Spark
  configuration parameter *spark.kryoserializer.buffer.max.mb*

3) Use Frame.sort() instead if the number of sorted rows (k) is
  very large (i.e., cannot fit in Spark driver memory).


Examples
--------
For this example, we calculate the top 5 movie genres in a data frame:

This example returns the top-3 rows sorted by a single column: 'year' descending:

.. code::

    >>> topk_frame = frame.sorted_k(3, [ ('year', False) ])

      col1:str   col2:int   col3:str
    /-----------------------------------/
      Animation    2013     Frozen
      Animation    2010     Tangled
      Drama        2008     The Dark Knight


This example returns the top-5 rows sorted by multiple columns: 'genre' ascending and 'year' descending:

.. code::

    >>> topk_frame = frame.sorted_k(5, [ ('genre', True), ('year', False) ])

      col1:str   col2:int   col3:str
    /-----------------------------------/
      Animation    2013     Frozen
      Animation    2010     Tangled
      Animation    1994     The Lion King
      Drama        2008     The Dark Knight
      Drama        1972     The Godfather

This example returns the top-5 rows sorted by multiple columns: 'genre' ascending and 'year' ascending
using the optional tuning parameter for reduce-tree depth:

.. code::

    >>> topk_frame = frame.sorted_k(5, [ ('genre', True), ('year', True) ], reduce_tree_depth=1)

      col1:str   col2:int   col3:str
    /-----------------------------------/
      Animation    1994     The Lion King
      Animation    2010     Tangled
      Animation    2013     Frozen
      Drama        1972     The Godfather
      Drama        2008     The Dark Knight

