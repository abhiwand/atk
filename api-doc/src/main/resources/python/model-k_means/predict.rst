Predict frame labels.

Predict the cluster assignments for the data points. 

Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted. By default, predict is run on the same columns over which the model is trained. The user could specify column names too if needed.


Returns
-------
    It creates a new revision of the frame with the following new columns:
    - A column containing the predicted cluster assignment for each row


Examples
--------
::

    model = ia.KMeansModel(name='MyKmeansModel')
    model.train(frame, ['name_of_observation_column1', 'name_of_observation_column2'], 3, 10, 0.0002, "random")        
    model.predict(frame)


