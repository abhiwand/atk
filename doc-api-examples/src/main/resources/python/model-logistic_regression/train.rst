Examples
--------
.. only:: html

    .. code::

        >>> my_model = ia.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_label_column', ['name_of_observation_column(s)'], 'frequency_column', 'LBFGS', intercept=false, num_iterations=50)

.. only:: latex

    .. code::

        >>> my_model = ia.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame,'name_of_label_column', ['name_of_observation_column'], 'frequency_column',
        ... 'LBFGS', intercept=false, num_iterations=50)

