Predict test frame labels and return metrics.

Predict the labels for a test frame and run classification metrics on predicted
and target labels.

Parameters
----------
predict_frame : Frame
    A frame whose labels are to be predicted.
label_column : str
    Column containing the actual label for each observation.
observation_column : list of str (Optional)
    Column(s) containing the observations whose labels are to be predicted and
    tested.
    By default, we test over the columns the LibsvmModel was trained on.

Returns
-------
Object
    Object with binary classification metrics.
    The data returned is composed of multiple components:
<object>.accuracy : double
     The degree of correctness of the test frame labels.
<object>.confusion_matrix : table
    A specific table layout that allows visualization of the performance of the
    test.
<object>.f_measure : double
    A measure of a test's accuracy.
    It considers both the precision and the recall of the test to compute
    the score.
<object>.precision : double
    The degree to which the correctness of the label is expressed.
<object>.recall : double
     The fraction of relevant instances that are retrieved.


Examples
--------
.. only:: html

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',['List_of_observation_column/s'])

        >>> metrics.f_measure
        0.66666666666666663

        >>> metrics.recall
        0.5

        >>> metrics.accuracy
        0.75

        >>> metrics.precision
        1.0

        >>> metrics.confusion_matrix

                      Predicted
                    _pos_ _neg__
        Actual  pos |  1     1
                neg |  0     2


.. only:: latex

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])
        >>> metrics = my_model.test(test_frame, 'name_of_label_column',
        ... ['List_of_observation_column/s'])

        >>> metrics.f_measure
        0.66666666666666663

        >>> metrics.recall
        0.5

        >>> metrics.accuracy
        0.75

        >>> metrics.precision
        1.0

        >>> metrics.confusion_matrix

                      Predicted
                    _pos_ _neg__
        Actual  pos |  1     1
                neg |  0     2


