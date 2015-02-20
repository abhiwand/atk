Predict frame labels.

Predict the labels for a test frame and create a new frame revision with existing columns and a new predicted label's column.

Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted

observation_column : list of str (Optional)
    Columns containing the observations. Default is the same column names used to train the model

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
