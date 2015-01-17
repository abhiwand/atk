Predict frame labels.

Predict the labels for a test frame and return a new frame with existing
columns and a predicted label's column.

Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted

predict_for_observation_column : str
    Column containing the observations

Returns
-------
Frame
    Frame containing the original frame's columns and a column with the
    predicted label


Examples
--------
::

    model = ia.LogisticRegressionModel(name='LogReg')
    model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
    new_frame = model.predict(predict_frame, 'predict_for_observation_column')


