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

