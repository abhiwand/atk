Predict frame labels.

Predict the labels for a test frame and create a new frame revision with existing columns and a new predicted label's column.

Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted.
    By default, predict is run on the same columns over which the model is trained.
    The user could specify column names too if needed.

observation_column : list of str (Optional)
    Column(s) containing the observations whose labels are to be predicted.
    By default, we predict the labels over columns the SvmModel was trained on.

Returns
-------
Frame
    Frame containing the original frame's columns and a column with the
    predicted label

Examples
--------
::

    model = ia.SvmModel(name='mySVM')
    model.train(train_frame, ['name_of_observation_column1'], 'name_of_label_column')
    predicted_frame = model.predict(predict_frame, ['predict_for_observation_column'])
