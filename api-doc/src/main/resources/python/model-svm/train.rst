Train frame model.

Creating a SVM Model using the observation column and label column of the train frame

Parameters
----------
frame : Frame
    frame to train the model on

observation_column : [ str | list of str ]
    Column containing the observations

label_column : str
    Column containing the label for each observation

num_opt_iterations: Int (Optional)
    number of iterations

step_size: Int (Optional)
    step size for optimizer

reg_type: String (Optional)
    regularization L1 or L2. Default is L2

reg_param: Double (Optional)
    regularization parameter

Returns
-------
None

Examples
--------
::

    model = ia.SvmModel(name='my SVM')
    model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column')

