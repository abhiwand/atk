Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
        >>> my_model.predict(predict_frame, ['predict_for_observation_column'])

.. only:: latex

    .. code::

        >>> my_model = ia.LogisticRegressionModel(name='LogReg')
        >>> my_model.train(train_frame, 'name_of_observation_column',
        ... 'name_of_label_column')
        >>> my_model.predict(predict_frame, ['predict_for_observation_column'])

