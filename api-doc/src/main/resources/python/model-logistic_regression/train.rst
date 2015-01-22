Train frame model.

Creating a LogisticRegression Model using the observation column and label column of the train frame",

Parameters
----------
frame : Frame
    frame to train the model on

observation_column : str
    Column containing the observations

label_column : str
    Column containing the label for each observation

Returns
-------
Trained LogisticRegression model object

Examples
--------
::

    model = ia.LogisticRegressionModel(name='LogReg')
    model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')

