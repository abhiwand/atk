Build new frame with columns for data and distribution.

Generates the :term:`empirical cumulative distribution` for the input column.


Parameters
----------
sample_col : str
    The name of the column containing sample.


Returns
-------
Frame
    A frame containing each distinct value in the sample and its
    corresponding ecdf value.


Examples
--------
Consider the following sample data set in *frame* with actual data labels
specified in the *labels* column and the predicted labels in the
*predictions* column:

.. code::

    >>> my_frame.inspect()

      a:unicode   b:int32
    /---------------------/
       red         1
       blue        3
       blue        1
       green       0

    >>> result = my_frame.ecdf('b')
    >>> result.inspect()

      b:int32   b_ECDF:float64
    /--------------------------/
       1             0.2
       2             0.5
       3             0.8
       4             1.0

