Most or least frequent column values.

Calculate the top (or bottom) K distinct values by count of a column.
The column can be weighted.
All data elements of weight <= 0 are excluded from the calculation, as are
all data elements whose weight is NaN or infinite.
If there are no data elements of finite weight > 0, then topK is empty.


Parameters
----------
data_column : str
    The column whose top (or bottom) K distinct values are to be calculated.
k : int
    Number of entries to return (If k is negative, return bottom k).
weights_column : str (optional)
    The column that provides weights (frequencies) for the topK calculation.
    Must contain numerical data.
    Default is 1 for all items.


Returns
-------
Frame
    An object with access to the frame of data.


Examples
--------
For this example, we calculate the top 5 movie genres in a data frame:

.. code::

    >>> top5 = frame.top_k('genre', 5)
    >>> top5.inspect()

      genre:str   count:float64
    /---------------------------/
      Drama        738278
      Comedy       671398
      Short        455728
      Documentary  323150
      Talk-Show    265180

This example calculates the top 3 movies weighted by rating:

.. code::

    >>> top3 = frame.top_k('genre', 3, weights_column='rating')
    >>> top3.inspect()

      movie:str      count:float64
    /------------------------------/
      The Godfather         7689.0
      Shawshank Redemption  6358.0
      The Dark Knight       5426.0

This example calculates the bottom 3 movie genres in a data frame:

.. code::

    >>> bottom3 = frame.top_k('genre', -3)
    >>> bottom3.inspect()

      genre:str   count:float64
    /---------------------------/
      Musical       26
      War           47
      Film-Noir    595


