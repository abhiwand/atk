Build a naive bayes model.

Train a NaiveBayesModel using the observation column, label column of the train frame and an optional lambda value.

Parameters
----------
frame : Frame
    A frame to train the model on.
label_column : str
    Column containing the label for each observation.
observation_column : list of str
    Column(s) containing the observations.
lambdaParameter : double (Optional)
    Additive smoothing parameter
    Default is 1.0

Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'],0.9)

.. only:: latex

    .. code::

        >>> my_model = ia.NaiveBayesModel(name='naivebayesmodel')
        >>> my_model.train(train_frame, 'name_of_label_column',['name_of_observation_column(s)'],0.9)

