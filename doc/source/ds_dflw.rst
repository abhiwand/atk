=====================
Process Flow Examples
=====================

.. contents:: Table of Contents
    :local:

.. toctree::
    :hidden:

    ds_apir

When using the toolkit, you will import your data, perform cleaning operations on it, possibly combine it
with other data sets, and finally, analyze it.

The first thing to do is to load the toolkit.
This is stored in the intelanalytics folder and it's sub-folders.

.. _pythonpath:

It is recommended that you add the location of the *intelanalytics* directory to the PYTHONPATH
environmental variable prior to starting Python.
This can be done from a shell script, similar to::

    PYTHONPATH=/usr/lib/
    export PYTHONPATH
    python

This way, from inside Python, it is easy to load the toolkit::

    import intelanalytics as ia

To test whether you have imported the toolkit properly type::

    print valid_data_types

You should see something like this::

    float32, float64, int32, int64, str, unicode

.. _Importing Data:

--------------
Importing Data
--------------

.. _valid_data_types:

Your data is composed of different data types.
It could be composed of strings, integers, logic(True or False), floating point numbers, and other types.
Each row of data is probably a combination of these.
To maintain a database structure, each column of data can only hold one type of data.

Types Of Raw Data
=================

The only currently supported raw data format is comma-separated variables (CSV), but JSON and XML will be
supported in future releases.

.. _example_files.csvfile:

Importing a CSV File.
---------------------

A CSV file looks similar to this::

    "string",123,True,"again",25.125
    "next",,,"or not",1.0
    ,1,False,"again?",

Lines of data, with individual pieces of data separated by a delimiter, in this case the comma character.
You need to import your data into the database file in a way that the toolkit can understand and access it.
The first thing to do is to tell the toolkit how your data is formatted.
A database file can be viewed as a table with rows and columns.
Each column has a unique name and holds a specific data type.
Each row holds a set of data.

To import CSV data you need a :term:`schema` defining the structure of your data.
Schemas are constructed as a list of tuples, each defining a column in the database, each tuple being
composed of a string and a data type.
The string is the name of the column, and the data type must be valid
(see :ref:`Valid Data Types <valid_data_types>`).
Unicode in column names will likely cause the drop_frames() function (and others) to fail, and it is not
supported.
The order of the columns in the schema must match the order of columns in the data.

Let's start with a file *Data.csv* whose contents look like this::

    1,"Easy on My Mind"
    2,"No Rest For The Wicked"
    ,"Does Your Chewing Gum"
    4,
    5,""

Create the schema *schema_ab* with two columns: *a* (int32), and *b* (string):

.. code::

    schema_ab = [('a', int32), ('b', string)]

When `defining schemas`, if the parser should ignore the field, the type is assigned *ignore*, and the
name should be an empty string ``''``::

    schema_2 = [('column_a', str), ('', ignore), ('more_data', str)]

The delimiter can be declared using the key word ``delimiter``.
This would be a benefit if the delimiter is something other than a comma, for example, ``\t`` for
tab-delimited records.
If there are lines at the beginning of the file that should be skipped, the number of lines to skip can be
passed in with the ``skip_header_lines`` parameter.

Now we use the schema and the file name to create objects used to define the data layouts::

    my_csv = CsvFile('Data.csv', schema_ab)
    csv1 = CsvFile("data.txt", schema_ab)
    csv2 = CsvFile(file_name="more_data.txt", schema=schema_ab)
    csv3 = CsvFile("different_data.txt", schema=[('x', float32), ('', ignore), ('y', int64)])

    raw_csv_data_file = "my_data.csv"
    column_schema_list = [("x", float32), ("y", float32), ("z", bool)]
    csv4 = CsvFile(raw_csv_data_file,
                   column_schema_list,
                   delimiter='|',
                   skip_header_lines=2)


.. _example_frame.bigframe:

-----
Frame
-----

A :term:`Frame` is a class of objects capable of accessing and controlling a :term:`frame` containing
"big data".
The frame is visualized as a table structure of rows and columns.
It can handle large volumes of data, because it is designed to work with data spread over multiple
clusters.

Create A Frame
==============

A new frame is created:
    1. as "empty"", with no columns defined,
    #. as defined by a schema, or
    #. by copying (all or a part of) another frame.

Examples:
---------
To create an empty frame and a Frame object, *f*, to access it::

    f = Frame()

To create a frame defined by the schema *my_csv*, import the data, name the frame "bf", and create a
Frame object, *my_frame*, to access it::

    my_frame = Frame(my_csv, 'bf')

To create a new frame, identical to the frame named *bf* (except for the name, because the name must always
be unique), and create a Frame object *f2* to access it::

    f2 = Frame(my_frame)

To create a new frame with only columns *a* and *c* from the original frame *bf*, and save the Frame
object as *f3*::

    f3 = Frame(my_frame[['a', 'c']])

Frames (capital 'F') are not the same thing as frames (lower case 'f').
Frames contain data, viewed similarly to a table, while Frames are descriptive pointers to the data.
Commands such as ``f4 = my_frame`` will only give you a copy of the Frame proxy pointing to the same data.

.. _example_frame.append:

Append:
-------
The ``append`` function adds more rows and columns to a frame.
If columns are the same in both name and data type, the appended data will go into the existing column.
Columns and rows are added to the database structure, and data is imported as appropriate.

As an example, let's start with a frame containing two columns *a* and *b*.
The frame can be accessed by Frame *BF1*.
We can look at the data and structure of the database by using the ``inspect`` function::

    BF1.inspect()

    a:str       b:int32
    -------------------
    apple           182
    bear             71
    car            2048

To this frame we combine another frame with one column *c*.
This frame can be accessed by Frame *BF2*::

    BF2.inspect()

    c:str
    -----
    dog
    cat

With *append*::

    BF1.append(BF2)

The result is that the first frame would have the data from both frames.
It would still be accessed by Frame *BF1*::

    BF1.inspect()

    a:str       b:int32     c:str
    -----------------------------
    apple           182     None
    bear             71     None
    car            2048     None
    None           None     dog
    None           None     cat

See also the *join* method in the :doc:`API <ds_apic>` section.

.. _example_frame.inspect:

Inspect The Data
================
IAT provides several functions that allow you to inspect your data, including .count(), .len(),
.inspect(), and .take().

Examples
--------
To count the number of rows of data, you could do it this way::

    my_frame.count()

To count the number of columns, you use this function::

    my_frame.len()

To print the first two rows of data::

    print my_frame.inspect(2)

    a:float32          b:int64   
    --------------------------
      12.3000              500    
     195.1230           183954    

To create a new frame using the existing frame, use .take()::

    my_frame.take(10, offset=200)
 
Here, we've created a frame of 10 rows, beginning at row 200, from the frame accessed by *my_frame*.

.. _Clean The Data:

Clean The Data
==============

Cleaning data involves removing incomplete, incorrect, inaccurate, or corrupted information from the data
set.
The Frame API should be used for this.
While these Python libraries do not support all Python functionality, they have been specifically designed
to handle very large data sets, so when using some Python libraries, be aware that some of them are not
designed to handle these very large data sets.

.. warning::

    Unless stated otherwise, cleaning functions use the Frame proxy to operate directly on the data,
    so it changes the data in the frame, rather than return a new frame with the changed data.
    It is recommended that you copy the data to a new frame on a regular basis and work on the new frame.
    This way, you have a fall-back if something does not work as expected::

        next_frame = Frame(last_frame)

In general, the following functions select rows of data based upon the data in the row.
For details about row selection based upon its data see :doc:`ds_apir`

.. _example_frame.drop_rows:

Drop Rows:
----------

The ``drop`` function takes a predicate function and removes all rows for which the predicate evaluates to
``True``.

Examples:
~~~~~~~~~

To drop all rows where column *b* contains a negative number::

    my_frame.drop_rows(lambda row: row['b'] < 0)

To drop all rows where column *a* is empty::

    my_frame.drop_rows(lambda row: row['a'] is None)

To drop all rows where any column is empty::

    my_frame.drop_rows(lambda row: any([cell is None for cell in row]))

.. _example_frame.filter:

Filter Rows:
------------

The ``filter`` function is like ``drop``, except it removes all rows for which the predicate evaluates to
False.

Examples:
~~~~~~~~~

To keep only those rows where field *b* is in the range 0 to 10::

    my_frame.filter(lambda row: 0 >= row['b'] >= 10)

.. _example_frame.drop_duplicates:

Drop Duplicates:
----------------

The ``drop_duplicates`` function performs a row uniqueness comparison across the whole table.

Examples:
~~~~~~~~~

To drop any rows where the data in column *a* and column *b* are duplicates of some previously evaluated
row::

    my_frame.drop_duplicates(['a', 'b'])

Drop any rows where the data matches some previously-implemented evaluation row in all columns::

    my_frame.drop_duplicates()
 
.. _example_frame.drop_columns:

Drop Columns:
-------------

Columns can be dropped either with a string matching the column name or a list of strings::

    my_frame.drop_columns('b')
    my_frame.drop_columns(['a', 'c'])

.. _example_frame.rename_columns:

Rename Columns:
---------------

Columns can be renamed by giving the existing column name and the new name, in the form of a dictionary.
Unicode characters should not be used for column names.

Rename column *a* to "id"::

    my_frame.rename_columns(('a': 'id'))

Rename column *b* to "author" and *c* to "publisher"::

    my_frame.rename_columns(('b': 'author', 'c': 'publisher'))

.. _Transform The Data:

Transform The Data
==================

Often, you will need to create new data based upon the existing data.
For example, you need the first name combined with the last name, or
you need the number times John spent more than five dollars, or
you need the average age of students attending a college.

.. _example_frame.add_columns:

Add Columns:
------------

Columns can be added to the frame using values from other columns as their value.

Add a column *column3* as an int32 and fill it with the contents of *column1* and *column2* multiplied
together::

    my_frame.add_columns(lambda row: row.column1 * row.column2, ('column3', int32))

Add a new column *all_ones* and fill the entire column with the value 1::

    my_frame.add_columns(lambda row: 1, ('all_ones', int32))

Add a new column *a_plus_b* and fill the entire column with the value of column *a* plus column *b*::

    my_frame.add_columns(lambda row: row.a + row.b, ('a_plus_b', int32))

Add a new column *a_lpt* and fill the value according to this table:

+-------------------------------------------+-------------------------------------------+
| value in column *a*                       | value for column *a_lpt*                  |
+===========================================+===========================================+
| None                                      | None                                      |
+-------------------------------------------+-------------------------------------------+
| Between 30 and 127 (inclusive)            | column *a* times 0.0046 plus 0.4168       |
+-------------------------------------------+-------------------------------------------+
| Between 15 and 29 (inclusive)             | column *a* times 0.0071 plus 0.3429       |
+-------------------------------------------+-------------------------------------------+
| Between -127 and 14 (inclusive)           | column *a* times 0.0032 plus 0.4025       |
+-------------------------------------------+-------------------------------------------+
| None of the above                         | None                                      |
+-------------------------------------------+-------------------------------------------+

An example of Piecewise Linear Transformation::

    def transform_a(row):
        x = row['a']
        if x is None:
            return None
        if 30 <= x <= 127:
            m, c = 0.0046, 0.4168
        elif 15 <= x <= 29:
            m, c = 0.0071, 0.3429
        elif -127 <= x <= 14:
            m, c = 0.0032, 0.4025
        else:
            return None
        return m * x + c

    my_frame.add_columns(transform_a, ('a_lpt', float32))

Create multiple columns at once by making a function return a list of values for the new frame columns::

    my_frame.add_columns(lambda row: [abs(row.a), abs(row.b)], [('a_abs', int32), ('b_abs', int32)])

.. _ds_dflw_frame_examine:

Examining the Data
==================

Let's say we want to get some standard statistical information about *my_frame*.
We can use the frame function *column_summary_statistics*::

    my_frame.column_summary_statistics()

.. _example_frame.group_by:

Group_by (and Aggregate):
-------------------------

Group rows together based on matching column values and then apply :term:`aggregation functions` on each
group, producing a **new** frame.

This needs two parameters:

1. the column(s) to group on
#. the aggregation function(s)

Aggregation based on columns:
    Given a frame with columns *a*, *b*, *c*, and *d*;
    Create a new frame and a Frame *grouped_data* to access it;
    Group by unique values in columns *a* and *b*;
    Average the grouped values in column *c* and save it in a new column *c_avg*;
    Add up the grouped values in column *c* and save it in a new column *c_sum*;
    Get the standard deviation of the grouped values in column *c* and save it in a new column *c_stdev*;
    Average the grouped values in column *d* and save it in a new column *d_avg*;
    Add up the grouped values in column *d* and save it in a new column *d_sum*::

        grouped_data = my_frame.group_by(['a', 'b'], { 'c': [agg.avg, agg.sum, agg.stdev],
            'd': [agg.avg, agg.sum]})

    Note:
        The only columns in the new frame will be the grouping columns and the generated columns.
        In this case, regardless of the original frame size, you will get seven columns:

        .. hlist::
            :columns: 7

            * *a*
            * *b*
            * *c_avg*
            * *c_sum*
            * *c_stdev*
            * *d_avg*
            * *d_sum*

Aggregation based on full row:
    Given a frame with columns *a*, and *b*;
    Create a new frame and a Frame *gr_data* to access it;
    Group by unique values in columns *a* and *b*;
    Count the number of rows in each group and put that value in column *count*::

        gr_data = my_frame.group_by(['a', 'b'], agg.count)

    Note:
        agg.count is the only full row aggregation function supported at this time

Aggregation based on both column and row together:
    Given a frame with columns *a*, *b*, *c*, and *d*;
    Group by unique values in columns *a* and *b*;
    Count the number of rows in each group and put that value in column *count*:
    Average the grouped values in column *c* and save it in a new column *c_avg*;
    Add up the grouped values in column *c* and save it in a new column *c_sum*;
    Get the standard deviation of the grouped values in column *c* and save it in a new column *c_stdev*;
    Average the grouped values in column *d* and save it in a new column *d_avg*;
    Add up the grouped values in column *d* and save it in a new column *d_sum*::

        my_frame.group_by(['a', 'b'], [agg.count, { 'c': [agg.avg, agg.sum, agg.stdev],
            'd': [agg.avg, agg.sum]}])

Supported aggregation functions:

..  hlist::
    :columns: 5

    * avg
    * count
    * max
    * mean
    * min
    * :term:`quantile`
    * stdev
    * sum
    * :term:`variance <Bias-variance tradeoff>`
    * distinct



.. _example_frame.join:

Join:
-----

Create a **new** frame from a JOIN operation with another frame.

Given two frames *my_frame* (columns *a*, *b*, *c*) and *your_frame* (columns *b*, *c*, *d*).
For the sake of readability, in these examples we will refer to the frames and the Frames by the same
name, unless needed for clarity::

    my_frame.inspect()                      

    a:str       b:str       c:str           
    --------------------------------------  
    alligator   bear        cat             
    auto        bus         car             
    apple       berry       cantelope       
    mirror      frog        ball

    your_frame.inspect()
                                        
    b:str       c:int32     d:str
    ------------------------------------
    bus             871     dog
    berry          5218     frog
    blue              0     log         

Column *b* in both frames is a unique identifier used to tie the two frames together.
Join *your_frame* to *my_frame*, creating a new frame with a new Frame to access it;
Include all data from *my_frame* and only that data from *your_frame* which has a value
in *b* that matches a value in *my_frame* *b*::

    our_frame = my_frame.join(your_frame, 'b', how='left')

Result is *our_frame*::

    our_frame.inspect()

    a:str       b:str       c_L:str         c_R:int32   d:str
    ----------------------------------------------------------------
    alligator   bear        cat                  None   None
    auto        bus         car                   871   dog
    apple       berry       cantelope            5281   frog
    mirror      frog        ball                 None   None

Do it again but this time include only data from *my_frame* and *your_frame* which have matching values in *b*::

    inner_frame = my_frame.join(your_frame, 'b')
    or
    inner_frame = my_frame.join(your_frame, 'b', how='inner')

Result is *inner_frame*::

    inner_frame.inspect()

    a:str       b:str       c_L:str         c_R:int32   d:str
    ----------------------------------------------------------------
    auto        bus         car                   871   dog
    apple       berry       cantelope            5218   frog

Do it again but this time include any data from *my_frame* and *your_frame* which do not have matching
values in *b*::

    outer_frame = my_frame.join(your_frame, 'b', how='outer')

Result is *outer_frame*::

    outer_frame.inspect()

    a:str       b:str       c_L:str     c_R:int32   d:str
    ----------------------------------------------------------------
    alligator   bear        cat              None   None
    mirror      frog        ball             None   None
    None        None        None                0   log

If column *b* in *my_frame* and column *d* in *your_frame* are the tie:
Do it again but include all data from *your_frame* and only that data in *my_frame* which has a value in
*b* that matches a value in *your_frame* *c*::

    right_frame = my_frame.join(your_frame, left_on='b', right_on='d', how='right')

Result is *right_frame*::

    right_frame.inspect()

    a:str       b_L:str     c:str       b_R:str     c:int32     d:str
    ----------------------------------------------------------------------------
    None        None        None        bus             871     dog
    mirror      frog        ball        berry          5218     frog
    None        None        None        blue              0     log

.. _example_frame.flatten_column:

Flatten Column:
---------------

The function ``flatten_column`` creates a **new** frame by splitting a particular column and returns a
Frame object.
The column is searched for rows where there is more than one value, separated by commas.
The row is duplicated and that column is spread across the existing and new rows.

Given that I have a frame accessed by Frame *my_frame* and the frame has two columns *a* and *b*.
The "original_data"::

    1-"solo,mono,single"
    2-"duo,double"

I run my commands to bring the data in where I can work on it::

    my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', string)], delimiter='-')
    my_frame = Frame(source=my_csv)

I look at it and see::

    my_frame.inspect()

    a:int32   b:string
    ----------------------------------
      1       solo, mono, single
      2       duo, double

Now, I want to spread out those sub-strings in column *b*::

    your_frame = my_frame.flatten_column('b')

Now I check again and my result is::

    your_frame.inspect()

    a:int32   b:str
    ------------------
      1       solo
      1       mono
      1       single
      2       duo
      2       double

.. TODO:: Miscellaneous Notes
    Misc Notes

    Discuss statistics, mean, standard deviation, etcetra.

----------
TitanGraph
----------

For the examples below, we will use a Frame *my_frame*, which accesses an arbitrary frame of data
consisting of the following columns:

    +-----------+-----------+-----------+-----------+
    | emp_id    | name      | manager   | years     |
    +===========+===========+===========+===========+
    | 00001     | john      | None      | 5         |
    +-----------+-----------+-----------+-----------+
    | 00002     | paul      | 00001     | 4         |
    +-----------+-----------+-----------+-----------+
    | 00003     | george    | 00001     | 3         |
    +-----------+-----------+-----------+-----------+
    | 00004     | ringo     | 00001     | 2         |
    +-----------+-----------+-----------+-----------+

.. _ds_dflw_building_rules:

Building Rules
==============

First we make rule objects. These are the criteria for transforming the table data to graph data.

Vertex Rule:
------------

To create a rule for :term:`vertices`, one needs to define:

1.  The label for the vertices, for example, the string "empID".
#.  The identification value of each vertex, for example, the column "emp_id" of our frame.
#.  The properties of the vertex.

Note:
    The properties of a vertex:

    1.  Consist of a label and its value. For example, the property *name* with its value taken from
        column *name* of our frame.
    #.  Are optional, which means a vertex might have zero or more properties.

Vertex Rule Example:
~~~~~~~~~~~~~~~~~~~~

Create a vertex rule called “employee” from the above frame::

    employee = VertexRule(‘empID”, my_frame[“emp_id”], {“name”: my_frame[“name”]})

The created vertices will be grouped under the label “empID”, will have an identification based on the
values from the column *emp_id*, and will have a property *name* with its value from the specified frame
column *name*.

Create another vertex rule called “manager”::

    manager = VertexRule(‘empID”, my_frame[“manager”])

The identification values for these vertices will be taken from column *manager* of the frame.

Both vertex rules will be grouped under label *empID* (we will consider managers to also be employees in
these examples).

Edge Rule:
----------
 
An edge is a link that connects two vertices, in our case, they are *tail* and *head*. An edge can have
properties similar to a vertex.

To create a rule for an edge, one needs to define:

1.  The label or identification for the edge, for example, the string “worksUnder”
#.  The tail vertex specified in the previously defined vertex rule.
#.  The head vertex specified in the previously defined vertex rule.
#.  The properties of the edge:

    A.  consist of a label and its value, for example, the property *name* with value taken from column
        *name* of a frame

    #.  are optional, which means an edge might have zero or more properties

Edge Rule Example:
~~~~~~~~~~~~~~~~~~

Create an edge called “reports” from the same frame (accessed by Frame *my_frame*) as above, using
previously defined *employee* and *manager* rules, and link them together::

    reports = EdgeRule("worksUnder", employee, manager, { "years": my_frame[“years”] })

This rule ties the vertices together, and also defines the property *years*, so the edges created will
have this property with the value from the frame column *years*.

Use of bidirectional:
~~~~~~~~~~~~~~~~~~~~~
In the edge rule, the user can specify whether or not the edge is :term:`directed <Undirected Graph>`.

In the example above, using the *employee* and *manager* vertices, there is an edge created to link both
of them with label “worksUnder”.
This edge is considered “directed” since an employee reports to a manager but not vice versa.
The bidirectional flag will create an extra edge going in the opposite direction for every edge.
To enable use the parameter ``bidirectional`` in the edge rule and set it to ``True``,
as shown in example below::

    reports = EdgeRule("worksUnder", employee, manager, { "years": f[“years”]},
        bidirectional = True)

.. _ds_dflw_building_a_graph:

Building a Graph From a Set of Rules
====================================

Now that you have built some rules, let us put them to use and create a graph by calling TitanGraph.
We will give the graph the name “employee_graph”::

    my_graph = TitanGraph([employee, manager, reports], “employee_graph”)

The graph is then created in the underlying graph database structure and
the access control information is saved into the TitanGraph object *my_graph*.
The data is ready to be analyzed using the :doc:`ds_ml` algorithms in the TitanGraph API.

Similar to what was discussed for Frame, what gets returned is not all the data,
but a proxy (descriptive pointer) for the data.
Commands such as g4 = my_graph will only give you a copy of the proxy, pointing to the same graph.

--------------
Error Handling
--------------

Examples::

    ia.errors.last  # full exception stack trace and message of the last exception
        raised at the API layer
    ia.errors.show_details  # toggle setting to show full stack trace, False by default

The above commands may have been split for enhanced readability in some medias.

