Train Lib Svm model based on another frame.

Creating a lib Svm Model using the observation column and label column of the train
frame.

Parameters
----------
frame : Frame
    A frame to train the model on.

label_column : str
    Column name containing the label for each observation.

observation_column : list of str
    Column(s) containing the observations.

epsilon: double (Optional)
    Default is 0.001.

degree: int (Optional)
    Default is 3.

gamma: Double (Optional)
    Default is 0.0.

coef: double (Optional)
    Default is 0.0.

nu : double (Optional)
    Default is 0.5.

cache_size : double (Optional)
    Default is 100.0.

shrinking : int (Optional)
    Default is 1.

probability : int (Optional)
    Default is 0.

nr_weight : int (Optional)
    Default is 0.

c : double (Optional)
    Default is 1.0.

p : double (Optional)
    Default is 0.1.


Returns
-------
None

Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'], epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5, cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1, nr_weight=1)

.. only:: latex

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'],
        ...   epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5, cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1, nr_weight=1)

