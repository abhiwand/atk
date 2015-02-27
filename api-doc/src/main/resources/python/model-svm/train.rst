Train SVM model based on another frame.

Creating a SVM Model using the observation column and label column of the train
frame

Parameters
----------
frame : Frame
    frame to train the model on.

label_column : str
    Column name containing the label for each observation.

observation_column : list of str
    Column(s) containing the observations.

intercept : bool (Optional)
    intercept value. Default is true.

num_iterations: int (Optional)
    number of iterations. Default is 100.

step_size: int (Optional)
    step size for optimizer. Default is 1.0.

reg_type: str (Optional)
    regularization L1 or L2. Default is L2.

reg_param: double (Optional)
    regularization parameter. Default is 0.01.

mini_batch_fraction : double (Optional)
    mini batch fraction parameter. Default is 1.0.

Returns
-------
None

Examples
--------
::

    model = ia.SvmModel(name='mySVM')
    model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

