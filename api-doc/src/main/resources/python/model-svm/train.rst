Train frame model.

Creating a SVM Model using the observation column/s and label column of the train frame

Parameters
----------
frame : Frame
    frame to train the model on

observation_column : list of str
    List of column names containing the observations

label_column : str
    Column name containing the label for each observation

intercept : Boolean (Optional)
    intercept value. Default is true

num_iterations: Int (Optional)
    number of iterations. Default is 100

step_size: Int (Optional)
    step size for optimizer. Default is 1.0

reg_type: String (Optional)
    regularization L1 or L2. Default is L2

reg_param: Double (Optional)
    regularization parameter. Default is 0.01

mini_batch_fraction : Double (Optional)
    mini batch fraction parameter. Default is 1.0

Returns
-------
None

Examples
--------
::

    model = ia.SvmModel(name='my SVM')
    model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

