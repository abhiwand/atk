Data Flow
=========

When using the Intel Data Platform: Analytics Toolkit, you will import your data, clean the data, combine or filter the data, and finally, make a graph of the data.


Data Import
-----------

Use the FrameBuilder component to upload data. In this release, we support CSV (Comma Separated Value), JSON, and XML formats. You will upload your data to the server through the file upload feature provided on GraphTrial.Intel.com. We will store your file on the HDFS file system of your instance, after which you can pull the data using FrameBuilder. When you complete this step, your data will be accessible thorough a BigDataFrame object.

>>> For example, here is how we import a CSV file:
>>> from intel_analytics.table.bigdataframe import get_frame_builder
>>> fb = get_frame_builder()
>>> csvfile = '/user/hadoop/movie_recommendations_raw.csv'
>>> frame = fb.build_from_csv('myframe',
                              csvfile, 
                              schema='user:long,vertex_type:chararray,movie:long,rating:long,splits:chararray',
                              overwrite=True)

To start, we import the frame builder, that is, the object that transforms your data set into a table. We use this fb object to create a BigDataFrame object that will store your data in an underlying HBase table.

Then we use the FrameBuilder to transform the data in the csv file to a BigDataFrame stored in an underlying HBase table. For other data types, you will use build_from_json for JSON files or build_from_xml for XML files.

In the last step of the example, 'myframe' is the table name of your data frame. The csv file has now been copied into the BigDataFrame and is ready to be cleaned and transformed using the advanced functionality of the BigDataFrame. The schema line describes the data in the csv file, so the BigDataFrame knows the type of data available. We set the overwrite flag to true so that the imported data overwrites the data already in the 'myframe' BigDataFrame, if any was present.


Feature Engineering
-------------------

Feature Engineering is an iterative process in which you gather data, clean it, run algorithms on it, and then look at the results. Then, you'll look at what you have and iterate again, looking for more data, or removing some data from the set until you have the desired result. You'll probably clean the data in a number of different ways, and then run your transforms again. We provide several methods in the Analytics Toolkit, but you can use features from other Python libraries as well to manipulate your data. The Intel Data Platform: Analytics Toolkit Python libraries have been specifically designed to handle very large data sets, so when using standard Python libraries, be aware that some of them are not designed to handle the very large data sets stored in Hadoop.


Data Cleaning
-------------

First, to clean your data, you will want to remove incomplete, incorrect, inaccurate, or corrupted data from your data set. You will use the BigDataFrame API to perform the data cleaning.

In the following examples, we entered the commands into the Intel Data Platform: Analytics Toolkit iPython notebook.

>>> Here's an example of cleaning data:
>>> frame.dropna()
>>> HTML(frame.inspect_as_html())

The first line performs the actual cleaning. In this case, the dropna() method removes all rows from the table that contain NA results in any column. The second line displays a sampling of the data frame's contents. In this case:


TBD: Table
##########

During the "cleaning phase," you will not only want to remove extraneous or erroneous data, you will want to take existing values and transform them into features you can use. This is where you manipulate the data, that is, actually crunch the data. You will use the BigDataFrame API calls to manipulate your data.


Feature Engineering or Data Transformation
------------------------------------------

The figure below shows part of an iPython notebook, transforming a movie rating to a logarithmic value, and displaying the results as an HTML output. The frame.transform method creates a new log_rating column that is the result of applying a LOG transform to the original rating column.


Graph Construction
------------------

Once you have imported your data, cleaned it, performed feature engineering on it (that is, manipulated the data), and now you are at the point where you can make a graph. You will use the BigGraph API calls to store the data in a graph.

There are two main steps to graph construction. First, you will configure graph builder to describe the graph, and then you build it.


Configure Your Graph
--------------------

You configure the graph builder that is going to create your graph. You are creating the recipe to build your graph by specifying how to manipulate the data in order to create this particular graph.

The first step specifies the sources for the vertices and edges, their labels and properties.

The code below shows an example of how to register (create) a graph from the BigDataFrame object where it contains columns that have users, movies, and ratings.


>>> from intel_analytics.graph.biggraph import get_graph_builder, GraphTypes
>>> gb = get_graph_builder(GraphTypes.Property, frame)
>>> gb.register_vertex('user',['vertex_type'])
>>> gb.register_vertex('movie')
>>> gb.register_edge(('user', 'movie', 'rates'), ['splits','rating']) 


Build Your Graph
----------------

After you have described the graph that you want to create, the second step is to build the graph and load its data into graph database.

To build a graph, we need only one line of code:

>>> graph = gb.build("mygraph", overwrite=True)

We name the graph mygraph, and we set overwrite to True, as above, to overwrite any existing graph with this name.
