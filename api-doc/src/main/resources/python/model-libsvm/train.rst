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
    set tolerance of termination criterion. Default is 0.001.

degree: int (Optional)
    Degree of the polynomial kernel function ('poly'). Ignored by all other kernels. Default is 3.

gamma: Double (Optional)
    Kernel coefficient for 'rbf', 'poly' and 'sigmoid'.  Default is 1/n_features.

coef: double (Optional)
    Independent term in kernel function. It is only significant in 'poly' and 'sigmoid'. Default is 0.0.

nu : double (Optional)
    Set the parameter nu of nu-SVC, one-class SVM, and nu-SVR. Default is 0.5.

cache_size : double (Optional)
    Specify the size of the kernel cache (in MB). Default is 100.0.

shrinking : int (Optional)
    Whether to use the shrinking heuristic. Default is 1(true).

probability : int (Optional)
    Whether to enable probability estimates. Default is 0(false).

nr_weight : int (Optional)
    Default is 0.

c : double (Optional)
    Penalty parameter C of the error term. Default is 1.0.

p : double (Optional)
    set the epsilon in loss function of epsilon-SVR. Default is 0.1.

svm_type: int (Optional)
    set type of SVM. Default is 2.
    0 -- C-SVC
	1 -- nu-SVC
	2 -- one-class SVM
	3 -- epsilon-SVR
	4 -- nu-SVR

kernel_type: int (Optional)
    Specifies the kernel type to be used in the algorithm. Default is 2.
    0 -- linear: u'*v
	1 -- polynomial: (gamma*u'*v + coef0)^degree
	2 -- radial basis function: exp(-gamma*|u-v|^2)
	3 -- sigmoid: tanh(gamma*u'*v + coef0)

weight_label: Array[Int] (Optional)
    Default is (Array[Int](0))

weight: Array[Double] (Optional)
    Default is (Array[Double](0.0))

Returns
-------
None

Examples
--------

.. only:: html

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'], epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5, cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1, nr_weight=1, svm_type=2, kernel_type=2)

.. only:: latex

    .. code::

        >>> my_model = ia.LibsvmModel(name='mySVM')
        >>> my_model.train(train_frame, 'name_of_label_column',['List_of_observation_column/s'],
        ...   epsilon=0.001, degree=3, gamma=0.11, coef=0.0, nu=0.5, cache_size=100.0, shrinking=1, probability=0, c=1.0, p=0.1, nr_weight=1, svm_type=2, kernel_type=2)

