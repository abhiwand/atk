Train SVM model based on another frame.

Creating a SVM Model using the observation column and label column of the train
frame.

Parameters
----------
frame : Frame
    A frame to train the model on.
label_column : str
    Column name containing the label for each observation.
observation_column : list of str
    Column(s) containing the observations.
intercept : bool (Optional)
    The algorithm adds an intercept. Default is true.
num_iterations: int (Optional)
    Number of iterations. Default is 100.
step_size: int (Optional)
    Step size for optimizer. Default is 1.0.
reg_type: str (Optional)
    Regularization L1 or L2. Default is L2.
reg_param: double (Optional)
    Regularization parameter. Default is 0.01.
mini_batch_fraction : double (Optional)
    Mini batch fraction parameter. Default is 1.0.

Examples
--------
.. only:: html

    .. code::

        >>> my_model = ia.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

.. only:: latex

    .. code::

        >>> my_model = ia.SvmModel(name='mySVM')
        >>> my_model.train(train_frame, ['name_of_observation_column'],
        ... 'name_of_label_column', false, 50, 1.0, "L1", 0.02, 1.0)

