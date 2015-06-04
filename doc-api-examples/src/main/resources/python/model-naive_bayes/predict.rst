Make new frame with column for label prediction.

Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted label's column.

Parameters
----------
predict_frame : Frame
    A frame whose labels are to be predicted.
    By default, predict is run on the same columns over which the model is
    trained.
    The user could specify column names too if needed.
observation_column : list of str (Optional)
    Column(s) containing the observations whose labels are to be predicted.
    By default, we predict the labels over columns the NaiveBayesModel
    was trained on.

Returns
-------
Frame
    Frame containing the original frame's columns and a column with the
    predicted label

Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'])
        >>> output = my_model.predict(predict_frame, ['name_of_observation_column(s)'])
        >>> output.inspect(5)

              Class:int32   Dim_1:int32   Dim_2:int32   Dim_3:int32   predicted_class:float64
            -----------------------------------------------------------------------------------
                        0             1             0             0                       0.0
                        1             0             1             0                       1.0
                        1             0             2             0                       1.0
                        2             0             0             1                       2.0
                        2             0             0             2                       2.0

.. only:: latex

    .. code::

        >>> my_model = ia.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column', ['name_of_observation_column(s)'])
        >>> output = my_model.predict(predict_frame, ['name_of_observation_column(s)'])
        >>> output.inspect(5)

              Class:int32   Dim_1:int32   Dim_2:int32   Dim_3:int32   predicted_class:float64
            -----------------------------------------------------------------------------------
                        0             1             0             0                       0.0
                        1             0             1             0                       1.0
                        1             0             2             0                       1.0
                        2             0             0             1                       2.0
                        2             0             0             2                       2.0

