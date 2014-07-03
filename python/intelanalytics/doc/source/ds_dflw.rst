=========
Data Flow
=========

When using the Analytics Toolkit, you will import your data, clean the data, combine or filter the data, and finally, make a :term:`graph` of the data.

-------------------
Data Identification
-------------------

The first thing to do is to be able to identify your data.
The currently supported raw data format is CSV.
Planned for future release are JSON and XML formats.

The first stage is creating the information necessary to import the data.

CSV File
========

For this example, we are going to use a datafile called Data.csv with three
columns of data, a first name, a last name, and an age.

To import CSV data you need a schema, in other words, a way for the program to know what the format of the data is supposed to be.
The first steps are to create a FrameSchema object and then add column descriptions to it.
The order of the columns must match the order of the data.
From within Python:

>>> from intelanalytics import *
... my_schema = FrameSchema()
... my_schema.append(( 'First Name', string ))
... my_schema.append(( 'Last Name', string ))
... my_schema.append(( 'Age', int))

This will create the schema called "my_schema", with three columns identified, "First Name" as a string, "Last Name" as a string, and "Age" as an integer.

Note:
    Valid types can be found by polling the types file from within Python:

>>> from intelanalytics import *
... print supported_types

Back to our example, now we create an object used to define the data layout:

>>> my_csv = CsvFile('Data.csv', my_schema, ,1)

.. TODO:: Other import data formats

    JSON File


    Example:

    >>> {
           "firstName": "John",
           "lastName": "Smith",
           "age": 25,
           "address": {
               "streetAddress": "21 2nd Street",
               "city": "New York",
               "state": "NY",
               "postalCode": "10021"
           },
           "phoneNumber": [
               {
                   "type": "home",
                   "number": "212 555-1239"
               },
               {
                   "type": "fax",
                   "number": "646 555-4567"
               }
           ],
           "gender":{
                "type":"male"
           }
        }

    Since the raw data has the data descriptors built in, the only things we have to do is define an object to hold the data.

    >>> from intelanalytics.core.files import JsonFile
        my_json = JsonFile(my_data_file.json)

    XML File

    Example:

    >>> <person>
          <firstName>John</firstName>
          <lastName>Smith</lastName>
          <age>25</age>
          <address>
            <streetAddress>21 2nd Street</streetAddress>
            <city>New York</city>
            <state>NY</state>
            <postalCode>10021</postalCode>
          </address>
          <phoneNumbers>
            <phoneNumber type="home">212 555-1234</phoneNumber>
            <phoneNumber type="fax">646 555-4567</phoneNumber>
          </phoneNumbers>
          <gender>
            <type>male</type>
          </gender>
        </person>

    The primitive values can also get encoded using attributes instead of tags:

    >>> <person firstName="John" lastName="Smith" age="25">
          <address streetAddress="21 2nd Street" city="New York" state="NY" postalCode="10021" />
          <phoneNumbers>
             <phoneNumber type="home" number="212 555-1234"/>
             <phoneNumber type="fax"  number="646 555-4567"/>
          </phoneNumbers>
          <gender type="male"/>
        </person>

    Since the raw data has the data descriptors built in, the only things we have to do is define an object to hold the data.

    >>> from intelanalytics.core.files import XmlFile
        my_xml = XmlFile(my_data_file.xml)

--------------------------------------------
Data Import or :term:`BigFrame` Construction
--------------------------------------------

With some idea of the data file, the next step is to use that information to import the data.

>>> my_frame = BigFrame(my_csv, "Personnel Data")

This could take a while depending upon the amount of raw data.
The raw data has now been copied into a :term:`BigFrame` object called "my_frame" and is ready to be cleaned and transformed using
the advanced functionality of the :term:`BigFrame` API.

-------------------
Feature Engineering
-------------------

Feature Engineering is an iterative process in which you select data, clean it, run algorithms on it, and then look at the results.
Then, you'll look at what you have and iterate again, looking for more data, or removing some data from the set until you have the desired result.
You'll probably clean the data in a number of different ways, and then run your transforms again.
The Analytics Toolkit Python libraries have been specifically designed to handle very large data sets,
so when using standard Python libraries, be aware that some of them are not designed to handle these very large data sets.


Data Cleaning
=============

First, to clean your data, you will want to remove incomplete, incorrect, inaccurate, or corrupted data from your data set.
You will use the :term:`BigFrame` API to perform the data cleaning.

Here's an example of cleaning data. In this case, we are going to drop (erase/delete) any rows which have no data at all.

>>> my_frame.dropna(all)

Data Transformation
===================

During the "cleaning phase," you will not only want to remove extraneous or erroneous data,
you will want to take existing values and transform them into features you can use.
This is where you manipulate the data, that is, actually crunch the data, using the :term:`BigFrame` API.

>>> my_frame.add_columns('Last Name'+'Age', string, "id")


------------------
Graph Construction
------------------

You have imported your data, cleaned it, performed feature engineering on it (that is, manipulated the data),
and now you are at the point where you can make a :term:`graph`.
You will use the :term:`BigGraph` API calls to store the data in a :term:`graph`.

There are two main steps to :term:`graph` construction.
First, you will build a set of rules to describe the transformation from table to :term:`graph`, and then you build it.


Building Rules
==============

First make rule objects.
These are the criteria for transforming the table data to :term:`graph` data.

>>> my_v_rule_1 = VertexRule( 'ID', my_frame['id'], (my_frame.title, my_frame('id')))

This means the a :term:`vertex` is created for each row of the :term:`BigFrame` and it's unique, primary value/identification
is "( 'ID' : 'Smit32' )" where the "Smit32" is the value in the "id" column.
This :term:`vertex` would also have a secondary value/identification of "( 'Mr.' : 'Wayne' )" where "Mr." is the value in the "title" column and  "Wayne" is the value in the "Last Name" column.

>>> my_v_rule_2 = VertexRule( 'LAST', my_frame['Last Name'], (my_frame.title, my_frame('Last Name')))
... my_edge_rule = EdgeRule( "my_first_edge", my_v_rule_1, my_v_rule_2 )

This means that an :term:`edge` rule (my_edge_rule) is created between the vertices "my_v_rule_1" and "my_v_rule_2" (defined by their rules),
and it is labeled "my_first_edge".

Build Your Graph
================

Now that you have built some rules, let us put them to use and create a :term:`BigGraph` object.

>>> my_graph = BigGraph( [my_v_rule_1, my_v_rule_2, my_edge_rule], "graphfilename" )

This could take a while depending upon the amount of raw data.

The table database has now been copied into a :term:`BigGraph` object and is ready to be analyzed using the advanced
functionality of the :term:`BigGraph` API.

