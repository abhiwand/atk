Train frame model.

Creating a KMeans Model using the observation columns of a train frame. Upon training the 'k' cluster centers are computed.

Parameters
----------
frame : Frame
    Frame to train the model on

observation_columns : List[str]
    Columns containing the observations

k : Int
    Desired number of clusters. This is an optional paramter with default value 2

maxIterations : Int
    Number of iterations for which the algorithm should run. This is an optional paramter with default value 20

epsilon : Double
    Distance threshold within which we consider k-means to have converged. This is an optional parameter with default value 1e-4

initializationMode : String
    The initialization technique for the algorithm. It could be either "random" or "k-means||". The default is "k-means||"
       
Returns
-------
Unit Return

Examples
--------
::

    model = ia.KMeansModel(name='MyKMeansModel')
    model.train(train_frame, ['name_of_observation_column1', 'name_of_observation_column2'], 3, 10, 0.0002, "random")

