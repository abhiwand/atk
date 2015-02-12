Calculate the dot product for each row in a frame using values from two equal-length sequences of columns.

    Dot product is computed by the following formula:

    The dot product of two vectors A=[a_1, a_2, ..., a_n], and B =[b_1, b_2, ..., b_n] = a_1*b_1 + a_2*b_2 + ...+ a_n*b_n
    The dot product for each row is stored in a new column in the existing frame.

    Parameters
    ----------
    left_column_names : list of strings
        Names of columns used to create the left vector (A) for each row

    right_column_names : list of strings
        Names of columns used to create right vector (B) for each row

    dot_product_column_name : string
        Name of column used to store the dot product

    default_left_values : list of doubles (optional)
        Default values used to substitute null values in left vector

    default_right_values : list of doubles (optional)
        Default values used to substitute null values in right vector


    Notes
    -----
    1)  If default values are not specified, any null values in the data will be substituted by zeros.

    Examples
    --------
     For this example, we will calculate the dot product for a sequence of columns in Frame object *my_frame*::

         my_frame.inspect()

          col_0:int32  col_1:float64 col_2:int32 col3:int32
         /-------------------------------------------------/
          1            0.2           -2          5
          2            0.4           -1          6
          3            0.6            0          7
          4            0.8            1          8
          5            null           2          null

     Modify the frame by computing the dot product for a sequence of columns::

         my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product')
         my_frame.inspect()

          col_0:int32  col_1:float64 col_2:int32 col3:int32  dot_product:float64
         /----------------------------------------------------------------------/
          1            0.2           -2          5            -1.0
          2            0.4           -1          6             0.4
          3            0.6            0          7             4.2
          4            0.8            1          8            10.4
          5            null           2          null         10.0


     Modify the frame by computing the dot product with default values for nulls::

         my_frame.dot_product(['col_0','col_1'], ['col_2', 'col_3'], 'dot_product_2', [0.1, 0.2], [0.3, 0.4])
         my_frame.inspect()

         col_0:int32  col_1:float64 col_2:int32 col3:int32  dot_product:float64  dot_product_2:float64
         /--------------------------------------------------------------------------------------------/
          1            0.2           -2          5            -1.0               -1.0
          2            0.4           -1          6             0.4                0.4
          3            0.6            0          7             4.2                4.2
          4            0.8            1          8            10.4                10.4
          5            null           2          null         10.0                10.08
