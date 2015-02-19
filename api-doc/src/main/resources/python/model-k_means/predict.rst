Predict frame labels.

Predict the cluster assignments for the data points. 

Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted. By default, predict is run on the same columns over which the model is trained. The user could specify column names too if needed.

observation_columns : list of str (optional)
    Columns containing the observations whose clusters are to be predicted. By default, we predict the clusters over columns the KMeansModel was trained on. The columns are scaled using the same values used when training the model.

Returns
-------
    It creates a new revision of the frame consisting of the existing columns of the frame along with the following new columns:
        'k' columns of type Double containing squared distance of each point to every cluster center
        'predicted_cluster' column of type Int containing the cluster assignment


Examples
--------
    model = ia.KMeansModel(name='MyKmeansModel')
    model.train(frame, ['name_of_observation_column1', 'name_of_observation_column2'],[2.0, 5.0] 3, 10, 0.0002, "random")
    model.predict(frame)


