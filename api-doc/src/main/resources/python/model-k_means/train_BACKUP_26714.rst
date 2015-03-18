Creating a KMeans Model using the observation columns of a train frame.

Upon training the 'k' cluster centers are computed.

Parameters
----------
frame : Frame
    Frame to train the model on

observation_columns : list of str
    Columns containing the observations

column_scalings : list of double
    Column scalings for each of the observation columns. The scaling value is multiplied by the corresponding value in the observation column.

k : Int (Optional)
    Desired number of clusters.
    This is an optional paramter with default value 2.

maxIterations : Int (Optional)
    Number of iterations for which the algorithm should run.
    This is an optional paramter with default value 20

epsilon : Double (Optional)
    Distance threshold within which we consider k-means to have converged.
    This is an optional parameter with default value 1e-4

initializationMode : String (Optional)
    The initialization technique for the algorithm.
    It could be either "random" or "k-means||".
    The default is "k-means||"
       
Returns
-------
results: dict
    Contains with keys

    cluster_size: dict
        contains with keys

        ClusterId : Int
            Number of elements in the cluster 'ClusterId'

    within_set_sum_of_squared_error: Double
        The set of sum of squared error for the model

Example
--------

.. code::

    >>> model = ia.KMeansModel(name='MyKMeansModel')
    >>> model.train(train_frame, ['name_of_observation_column1', 'name_of_observation_column2'],[1.0,2.0] 3, 10, 0.0002, "random")
