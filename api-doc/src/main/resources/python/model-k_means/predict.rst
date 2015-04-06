Predict the cluster assignments for the data points. 

Parameters
----------
predict_frame : Frame
    A frame whose labels are to be predicted.
    By default, predict is run on the same columns over which the model is
    trained.
    The user could specify column names too if needed.
observation_columns : list of str (optional)
    Column(s) containing the observations whose clusters are to be predicted.
    By default, we predict the clusters over columns the KMeansModel was
    trained on.
    The columns are scaled using the same values used when training the model.

Returns
-------
Frame
    A new frame consisting of the existing columns of the frame along with the
    following new columns:
    'k' columns of type Double containing squared distance of each point to
    every cluster center predicted_cluster' column of type Int containing the
    cluster assignment


Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> new_frame = my_model.predict(my_frame)

.. only:: latex

    .. code::

        >>> my_model = ia.KMeansModel(name='MyKmeansModel')
        >>> my_model.train(my_frame, ['name_of_observation_column1',
        ... 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
        >>> new_frame = my_model.predict(my_frame)


