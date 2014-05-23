Data Flow
=========

When using the Intel® Data Platform: Analytics Toolkit, you will import your data, clean the data, combine or filter the data, and finally, make a graph of the data.

Data Identification
-------------------
The first thing to do is to be able to identify your data.
Supported raw data formats are CSV, JSON, and XML.
At this stage we are just developing the information we will later need to import the data.

CSV File
~~~~~~~~
Example:

>>> This is an example of a CSV file being
    used for importing data.
    Bob, 1
    "Le Pew, Pepe", 2
    Joan of Arc, 2345

To import CSV data you need a schema, in other words, a way for the program to know what the format of the data is supposed to be.
The first steps are to create a FrameSchema object and then add column descriptions to it.
The order of the columns must match the order of the data.

>>> from intelanalytics.core.frame import FrameSchema
    my_schema = FrameSchema()
    my_schema.append([( 'name' : string )), ( "id" : int64 )])

This will create the schema called ``my_schema``, with two columns identified, ``name`` as a string and ``id`` as an int64.
Valid types can be found by polling the types file.

>>> from intelanalytics.core.types import *
    print supported_types

Now we define an object to hold the data.

>>> from intelanalytics.core.files import CsvFile
    my_csv = CsvFile(my_data_file.csv, my_schema, ,1)

JSON File
~~~~~~~~~
Example:

>>> | {
    |    "firstName": "John",
    |    "lastName": "Smith",
    |    "age": 25,
    |    "address": {
    |        "streetAddress": "21 2nd Street",
    |        "city": "New York",
    |        "state": "NY",
    |        "postalCode": "10021"
    |    },
    |    "phoneNumber": [
    |        {
    |            "type": "home",
    |            "number": "212 555-1239"
    |        },
    |        {
    |            "type": "fax",
    |            "number": "646 555-4567"
    |        }
    |    ],
    |    "gender":{
    |         "type":"male"
    |    }
    | }

Since the raw data has the data descriptors built in, the only things we have to do is define an object to hold the data.

>>> from intelanalytics.core.files import JsonFile
    my_json = JsonFile(my_data_file.json)

XML File
~~~~~~~~
Example:

>>> | <person>
    |   <firstName>John</firstName>
    |   <lastName>Smith</lastName>
    |   <age>25</age>
    |   <address>
    |     <streetAddress>21 2nd Street</streetAddress>
    |     <city>New York</city>
    |     <state>NY</state>
    |     <postalCode>10021</postalCode>
    |   </address>
    |   <phoneNumbers>
    |     <phoneNumber type="home">212 555-1234</phoneNumber>
    |     <phoneNumber type="fax">646 555-4567</phoneNumber>
    |   </phoneNumbers>
    |   <gender>
    |     <type>male</type>
    |   </gender>
    | </person>

The primitive values can also get encoded using attributes instead of tags:

>>> | <person firstName="John" lastName="Smith" age="25">
    |   <address streetAddress="21 2nd Street" city="New York" state="NY" postalCode="10021" />
    |   <phoneNumbers>
    |      <phoneNumber type="home" number="212 555-1234"/>
    |      <phoneNumber type="fax"  number="646 555-4567"/>
    |   </phoneNumbers>
    |   <gender type="male"/>
    | </person>

Since the raw data has the data descriptors built in, the only things we have to do is define an object to hold the data.

>>> from intelanalytics.core.files import XmlFile
    my_xml = XmlFile(my_data_file.xml)

Data Import or BigFrame Construction
------------------------------------
Now we have some idea of the data file, we will use that to import the data.

>>> from intelanalytics.core.frame import BigFrame
    my_frame_proxy = BigFrame(my_csv, "Test Data")

This could take a while depending upon the amount of raw data.
The raw data file has now been copied into a BigFrame object and is ready to be cleaned and transformed using the advanced functionality of the BigFrame.

Feature Engineering
-------------------
Feature Engineering is an iterative process in which you select data, clean it, run algorithms on it, and then look at the results.
Then, you'll look at what you have and iterate again, looking for more data, or removing some data from the set until you have the desired result.
You'll probably clean the data in a number of different ways, and then run your transforms again.
We provide several methods in the Analytics Toolkit, but you can use features from other Python libraries as well to manipulate your data.
The Intel® Data Platform: Analytics Toolkit Python libraries have been specifically designed to handle very large data sets, so when using standard Python libraries, be aware that some of them are not designed to handle these very large data sets.


Data Cleaning
~~~~~~~~~~~~~
First, to clean your data, you will want to remove incomplete, incorrect, inaccurate, or corrupted data from your data set.
You will use the BigFrame API to perform the data cleaning.

Here's an example of cleaning data. In this case, we are going to drop (erase/delete) any rows which have no data at all.

>>> my_frame_proxy.dropna(all)

Feature Engineering or Data Transformation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
During the "cleaning phase," you will not only want to remove extraneous or erroneous data, you will want to take existing values and transform them into features you can use.
This is where you manipulate the data, that is, actually crunch the data, using the BigFrame API.

>>> my_frame_proxy.add_column(w_price+markup, float32, "r_price")


Graph Construction
------------------

You have imported your data, cleaned it, performed feature engineering on it (that is, manipulated the data), and now you are at the point where you can make a graph.
You will use the BigGraph API calls to store the data in a graph.

There are two main steps to graph construction.
First, you will build a set of rules to describe the transformation from table to graph, and then you build it.


Building Rules
~~~~~~~~~~~~~~
First make rule objects.
These are the criteria for transforming the table data to graph data.

>>> from intelanalytics.core.graph import *
    my_vertex_rule = VertexRule( 'ID', my_frame_proxy['id'], (my_frame.title, my_frame_proxy('l_name')))
    This means the a vertex is created for each row of the BigFrame and it's unique, primary value/identification is ( "ID" : 1832 ) where the 1832 is the value in the id column.
    This vertex would also have a secondary value/identification of ( "Mr." : "Wayne" ) where "Mr." is the value in the title column and  "Wayne" is the value in the l_name column.

>>> my_edge_rule = EdgeRule( "my_first_edge", my_v_rule_1, my_v_rule_2 )
    This means that an edge is created between the vertexs (defined by their rules), and it is labeled "my_first_edge"
    e_rule_2 = EdgeRule( my_frame_proxy.transaction, v_rule_1, v_rule_2, ( "enjoys_beatings" : my_frame_proxy.beats_me ))
    This creates an edge labeled as what is in the column named transaction, going from v_rule_1 and v_rule_2, with a value/identification of ("enjoys_beatings" : and the value in the column named beats_me.



Build Your Graph
~~~~~~~~~~~~~~~~
Now that you have built some rules, let us put them to use and create a BigGraph object.

>>> my_graph = BigGraph( [my_vertex_rule, my_edge_rule, e_rule_2}, "People who love people" )


After you have described the graph that you want to create, the second step is to build the graph and load its data into graph database.

To build a graph, we need only one line of code:

>>> graph = gb.build("mygraph", overwrite=True)

This could take a while depending upon the amount of raw data.
The table database has now been copied into a BigGraph object and is ready to be analyzed using the advanced functionality of the BigGraph.
