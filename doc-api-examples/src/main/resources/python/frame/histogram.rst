Examples
--------
Consider the following sample data set:

.. code::

    >>> frame.inspect()

      a:unicode  b:int32
    /--------------------/
        a          2
        b          7
        c          3
        d          9
        e          1

    >>> hist = frame.histogram("b")
    >>> print hist

    Histogram:
        cutoffs: [1, 3, 6, 9],
        hist: [2, 1, 2],
        density: [0.4, 0.2, 0.4]


Plot hist as a bar chart using matplotlib:

.. only:: html

    .. code::

        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - hist.cutoffs[0])

.. only:: latex

    .. code::

        >>> import matplotlib.pyplot as plt

        >>> plt.bar(hist.cutoffs[:1], hist.hist, width=hist.cutoffs[1] - 
        ... hist.cutoffs[0])

