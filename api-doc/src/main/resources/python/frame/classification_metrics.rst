Model statistics of accuracy, precision, and others.

Calculate the accuracy, precision, confusion_matrix, recall and
:math:`F_{\\beta}` measure for a classification model.

*   The **f_measure** result is the :math:`F_{\\beta}` measure for a
    classification model.
    The :math:`F_{\\beta}` measure of a binary classification model is the
    harmonic mean of precision and recall.
    If we let:

    * beta :math:`\\equiv \\beta`,
    * :math:`T_{P}` denote the number of true positives,
    * :math:`F_{P}` denote the number of false positives, and
    * :math:`F_{N}` denote the number of false negatives

    then:

    .. math::

        F_{\\beta} = (1 + \\beta ^ 2) * \\frac{\\frac{T_{P}}{T_{P} + \\
        F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * \\
        \\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}} }

    The :math:`F_{\\beta}` measure for a multi-class classification model is
    computed as the weighted average of the :math:`F_{\\beta}` measure for
    each label, where the weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **recall** result of a binary classification model is the proportion
    of positive instances that are correctly identified.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{N}` denote the number of false negatives, then the model
    recall is given by: :math:`\\frac {T_{P}} {T_{P} + F_{N}}`.

    For multi-class classification models, the recall measure is computed as
    the weighted average of the recall for each label, where the weight is
    the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **precision** of a binary classification model is the proportion of
    predicted positive instances that are correct.
    If we let :math:`T_{P}` denote the number of true positives and
    :math:`F_{P}` denote the number of false positives, then the model
    precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.

    For multi-class classification models, the precision measure is computed
    as the weighted average of the precision for each label, where the
    weight is the number of instances of each label.
    The determination of binary vs. multi-class is automatically inferred
    from the data.

*   The **accuracy** of a classification model is the proportion of
    predictions that are correct.
    If we let :math:`T_{P}` denote the number of true positives,
    :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
    the total number of classified instances, then the model accuracy is
    given by: :math:`\\frac{T_{P} + T_{N}}{K}`.

    This measure applies to binary and multi-class classifiers.

*   The **confusion_matrix** result is a confusion matrix for a
    binary classifier model, formatted for human readability.
    See notes below.

Parameters
----------
label_column : str
    the name of the column containing the correct label for each
    instance.

pred_column : str
    the name of the column containing the predicted label for each
    instance.

pos_label : [ str | int | Null ] (optional)
    str or int for binary classifiers, Null for multi-class classifiers.
    The value to be interpreted as a positive instance.

beta : double (optional)
    beta value to use for :math:`F_{\\beta}` measure (default F1 measure
    is computed); must be greater than zero.
    Defaults to 1.

Notes
-----
**confusion_matrix** is not yet implemented for multi-class classifiers.

Returns
-------
object
<object>.accuracy : double
<object>.confusion_matrix : table
<object>.f_measure : double
<object>.precision : double
<object>.recall : double
    
Examples
--------
Consider the following sample data set in *frame* with actual data
labels specified in the *labels* column and the predicted labels in the
*predictions* column:

.. code::

    >>> frame.inspect()

      a:unicode   b:int32   labels:int32  predictions:int32
    /-------------------------------------------------------/
        red         1              0                  0
        blue        3              1                  0
        blue        1              0                  0
        green       0              1                  1

    >>> cm = frame.classification_metrics('labels', 'predictions', 1, 1)

    >>> cm.f_measure

    0.66666666666666663

    >>> cm.recall

    0.5

    >>> cm.accuracy

    0.75

    >>> cm.precision

    1.0

    >>> cm.confusion_matrix

                  Predicted
                 _pos_ _neg__
    Actual  pos |  1     1
            neg |  0     2

