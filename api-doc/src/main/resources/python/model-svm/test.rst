Predict test frame labels.

Predict the labels for a test frame and run classification metrics on predicted and target labels.


Parameters
----------
predict_frame : Frame
    frame whose labels are to be predicted.

label_column : str
    Column containing the actual label for each observation.

observation_column : list of str (Optional)
    Column(s) containing the observations whose labels are to be predicted and tested.
    By default, we test over the columns the SvmModel was trained on.

Returns
-------
An object with binary classification metrics.

  <object>.accuracy : double

  <object>.confusion_matrix : table

  <object>.f_measure : double

  <object>.precision : double

  <object>.recall : double

Examples
--------
::

    model = ia.SvmModel(name='mySVM')
    model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column')
    metrics = model.test(test_frame, 'name_of_label_column',['name_of_observation_column'])

    metrics.f_measure
    0.66666666666666663

    metrics.recall
    0.5

    metrics.accuracy
    0.75

    metrics.precision
    1.0

    metrics.confusion_matrix

                  Predicted
                _pos_ _neg__
    Actual  pos |  1     1
            neg |  0     2


