=====================
Process Flow Examples
=====================

.. contents:: Table of Contents
    :local:

.. toctree::
    :hidden:

    ds_apir


-----------------
Python Path Setup
-----------------

.. _pythonpath:

It is recommended that the location of the 'intelanalytics' directory be added
to the PYTHONPATH environmental variable prior to starting Python.
This can be done from a shell script, like this:

.. code::

    PYTHONPATH=$PYTHONPATH:/usr/lib/
    export PYTHONPATH
    python

This way, from inside Python, it is easy to load and connect to the |IAT|'s
REST server::

    import intelanalytics as ia
    ia.connect()

------------
Data Sources
------------

.. _valid_data_types:

Data is made up of variables of heterogeneous type (for example, strings,
integers, and floats) that can be organized as a collection of rows and columns.
Each row corresponds to the data associated with one observation, and each
column corresponds to a variable being observed.

.. TODO::

    Make writeup for database connectivity.

Connect to the server::

    import intelanalytics as ia
    ia.connect()

Sometimes it is helpful to see the details of the python stack trace upon error.
Setting the show_details to True causes the full python stack trace to be
printed, rather than a friendlier digest. ::

    ia.errors.show_details = True

.. TODO:: Move these to a new section discussing "clean up" or object life time
    section

    Clean up any previous frames (optional)::

    for name in ia.get_frame_names():
        print 'deleting frame: %s' %name
        ia.drop_frames(name)

    Clean up any previous graphs (optional)::

    for name in ia.get_graph_names():
        print 'deleting graph: %s' %name
        ia.drop_graphs(name)

To see the data types supported by the |IAT|::

    print ia.valid_data_types

You should see a string of variable types similar to this::

    float32, float64, int32, int64, unicode
    (and aliases: float->float64, int->int32, long->int64, str->unicode)

.. note::

    Although the |IAT| utilizes the Numpy package, numpy values of positive
    infinity (np.inf), negative infinity (-np.inf) or nan (np.nan) are treated
    as None.
    Results of any user-defined functions which deal with such values are
    automatically converted to None, so any further usage of those data points
    should treat the values as None.

.. _Importing Data:

Types Of Raw Data
=================

The currently supported raw data formats are |CSV| and LineFile.

.. _example_files.csvfile:

Importing a |CSV| file.
-----------------------

Some example rows from a |CSV| file could look like the below::

    "string",123,"again",25.125
    "next",5,"or not",1.0
    "fail",1,"again?",11.11

|CSV| files contain rows of information separated by new-line characters.
Within each row, the data fields are separated from each other by some standard
character(s).
In the above example, the separating character is a comma (,).
To import data into the |IAT|, you must tell the system how the input file
is formatted.
This is done by defining a schema.
Schemas are constructed as a list of tuples, each of which contains pairs of
ASCII-character names and data types (see :ref:`Valid Data Types
<valid_data_types>`), ordered according to the order of columns in the input
file.

Given a file *datasets/small_songs.csv* whose contents look like this::

    1,"Easy on My Mind"
    2,"No Rest For The Wicked"
    3,"Does Your Chewing Gum"
    4,"Gypsies, Tramps, and Theives"
    5,"Symphony No. 5"

For easier reuse, create a variable to hold the file name::

    my_data_file = "datasets/small_songs.csv"

Create the schema *my_schema* with two columns: *id* (int32), and *title*
(str)::

    my_schema = [('id', ia.int32), ('title', str)]

The schema and file name are used in the CsvFile() command to describe the file
format::

    my_csv_description = ia.CsvFile(my_data_file, my_schema)
    
The default delimiter to separate column data is a comma, but it can be
declared using the key word ``delimiter``::

    my_csv_description = ia.CsvFile(my_data_file, my_schema, delimiter = ",")

This can be helpful if the delimiter is something other than a comma, for
example, ``\t`` for tab-delimited records.
If there are lines at the beginning of the file that should be skipped, the
number of lines to skip can be passed in with the ``skip_header_lines``
parameter::

    csv_description = ia.CsvFile(my_data_file, my_schema, skip_header_lines = 5)

.. only:: html

    Now we use the schema and the file name to create objects used to define
    the data layouts::

        my_csv = ia.CsvFile(my_data_file, my_schema)
        csv1 = ia.CsvFile(file_name="data1.csv", schema=schema_ab)
        csv2 = ia.CsvFile(file_name="more_data.txt", schema=schema_ab)

        raw_csv_data_file = "datasets/my_data.csv"
        column_schema_list = [("x", ia.float64), ("y", ia.float64), ("z", str)]
        csv4 = ia.CsvFile(file_name=raw_csv_data_file, schema=column_schema_list, delimiter='|', skip_header_lines=2)


.. only:: latex

    Now we use the schema and the file name to create objects used to define
    the data layouts::

        my_csv = ia.CsvFile(my_data_file, my_schema)
        csv1 = ia.CsvFile(file_name="data1.csv", schema=schema_ab)
        csv2 = ia.CsvFile(file_name="more_data.txt", schema=schema_ab)

        raw_csv_data_file = "datasets/my_data.csv"
        column_schema_list = [("x", ia.float64), ("y", ia.float64), ("z", str)]
        csv4 = ia.CsvFile(file_name=raw_csv_data_file,  \\
            schema=column_schema_list, delimiter='|', skip_header_lines=2)


.. _example_frame.frame:

------
Frames
------

A :term:`Frame (capital F)` is a class of objects capable of accessing and
controlling a :term:`frame (lower case f)` containing "big data".
The frame is visualized as a two-dimensional table structure of rows and
columns.
The |IAT| can handle frames with large volumes of data, because it is
designed to work with data spread over multiple machines.

Create A Frame
==============

There are several ways to create frames:

#.  as "empty", with no schema or data
#.  with a schema and data
#.  by copying (all or a part of) another frame

Examples:
---------
Create an empty frame::

    my_frame = ia.Frame()

The Frame *my_frame* is now a Python object which references an empty frame
that has been created on the server.

To create a frame defined by the schema *my_csv*, import the data, name the
frame *myframe*, and create a Frame object, *my_frame*, to access it::

    my_frame = ia.Frame(source=my_csv4, name='myframe')

To create a new frame, identical to the frame named *myframe* (except for
the name, because the name must always be unique), and create a Frame object
*f2* to access it::

    f2 = my_frame.copy()
    f2.name = "copy_of_myframe"

To create a new frame with only columns *a* and *c* from the original frame
*myframe*, and save the Frame object as *f3*::

    f3 = my_frame.copy(['x', 'z'])
    f3.name = "copy_of_myframe2"

To create a frame copy of the original columns *x* and *z*, but only those
rows where *z* is TRUE::

    f4 = my_frame.copy(['x', 'z'], where = (lambda row: "TRUE" in row.z))
    f4.name = "copy_of_myframe_true"

Frames (capital 'F') are not the same thing as frames (lower case 'f').
Frames (lower case 'f') contain data, viewed similarly to a table, while
Frames are descriptive pointers to the data.
Commands such as ``f4 = my_frame`` will only give you a copy of the Frame
proxy pointing to the same data.

Let's create a Frame and check it out::

    small_songs = ia.Frame(my_csv, name = "small_songs")
    small_songs.inspect()
    small_songs.get_error_frame().inspect()

.. _example_frame.append:

Append:
-------
The ``append`` function adds rows and columns to a frame.
If columns are the same in both name and data type, the appended data will
go into the existing column.
Columns and rows are added to the database structure, and data is imported
as appropriate.

As an example, let's start with a frame containing two columns *a* and *b*.
The frame can be accessed by Frame *my_frame*.
We can look at the data and structure of the database by using the
``inspect`` function::

    BF1.inspect()

      a:str   b:ia.int64
    /--------------------/
      apple          182
      bear            71
      car           2048

To this frame we combine another frame with one column *c*.
This frame can be accessed by Frame *BF2*::

    BF2.inspect()

      c:str
    /-------/
      dog
      cat

With *append*::

    BF1.append(BF2)

The result is that the first frame would have the data from both frames.
It would still be accessed by Frame *BF1*::

    BF1.inspect()

      a:str     b:ia.int64     c:str
    /--------------------------------/
      apple        182         None
      bear          71         None
      car         2048         None
      None        None         dog
      None        None         cat

.. only:: html

    Try this example with data files *objects1.csv* and *objects2.csv*::

        objects1 = ia.Frame(ia.CsvFile("datasets/objects1.csv", schema=[('Object', str), ('Count', ia.int64)], skip_header_lines=1), 'objects1')
        objects2 = ia.Frame(ia.CsvFile("datasets/objects2.csv", schema=[('Thing', str)], skip_header_lines=1), 'objects2')

        objects1.inspect()
        objects2.inspect()

        objects1.append(objects2)
        objects1.inspect()

.. only:: latex

    Try this example with data files *objects1.csv* and *objects2.csv*::

        objects1 = ia.Frame(ia.CsvFile("datasets/objects1.csv", \\
            schema=[('Object', str), ('Count', ia.int64)], \\
            skip_header_lines=1), 'objects1')
        objects2 = ia.Frame(ia.CsvFile("datasets/objects2.csv", \\
            schema=[('Thing', str)], skip_header_lines=1), 'objects2')

        objects1.inspect()
        objects2.inspect()

        objects1.append(objects2)
        objects1.inspect()

See also the *join* method in the :doc:`API <ds_apic>` section.

.. _example_frame.inspect:

Inspect The Data
================
|IA| provides several functions that allow you to inspect your data,
including inspect(), and .take().
The Frame structure also contains frame information like .row_count.

Examples
--------
To see the number of rows::

    objects1.row_count

To see the number of columns::

    len(objects1.schema)

To see all the Frame data::

    objects1

To see two rows of data::

    print objects1.inspect(2)

    # Gives you something like this:
      a:ia.float64  b:ia.int64   
    /--------------------------/
        12.3000            500    
       195.1230         183954    

To see a subsection of data from the existing frame::

    subset_of_objects1 = objects1.take(3, offset=2)
    print subset_of_objects1
 
    # Gives you something like this:
    [[12.3, 500], [195.123, 183954], [12.3, 500]]

Here, we see a list of lists of data from *myframe*, containing 10 lists.
Each list has the data from a row in the frame accessed by *my_frame*,
beginning at row 200.

.. note::
    The sequence of the data is NOT guaranteed to match the sequence of the
    input file.
    This command might or might not return the same data you would see in
    lines 201 through 210 of the input file.

.. only:: html

    Some more examples to try::

        animals = ia.Frame(ia.CsvFile("datasets/animals.csv", schema=[('User', ia.int32), ('animals', str), ('Int1', ia.int64), ('Int2', ia.int64), ('Float1', ia.float64), ('Float2', ia.float64)], skip_header_lines=1), 'animals')
        animals.inspect()
        freq = animals.top_k('animals', animals.row_count)
        freq.inspect(freq.row_count)

        from pprint import *
        summary = {}
        for col in ['Int1', 'Int2', 'Float1', 'Float2']:
            summary[col] = animals.column_summary_statistics(col)
            pprint(summary[col])

.. only:: latex

    Some more examples to try::

        animals = ia.Frame(ia.CsvFile("datasets/animals.csv", \\
            schema=[('User', ia.int32), ('animals', str), ('Int1', ia.int64), \\
            ('Int2', ia.int64), ('Float1', ia.float64), ('Float2', \\
            ia.float64)], skip_header_lines=1), 'animals')
        animals.inspect()
        freq = animals.top_k('animals', animals.row_count)
        freq.inspect(freq.row_count)

        from pprint import *
        summary = {}
        for col in ['Int1', 'Int2', 'Float1', 'Float2']:
            summary[col] = animals.column_summary_statistics(col)
            pprint(summary[col])


.. _Clean The Data:

Clean The Data
==============

The process of "data cleaning" encompasses the identification and removal of
incomplete, incorrect, or mal-formed information in a data set.
While IAT's Frame API provides much of the functionality necessary for these
tasks, it's important to keep in mind that it was designed with scalability
in mind.
Thus, using external Python packages for these tasks, while possible, may
not provide the same level of efficiency.

.. warning::

    Unless stated otherwise, cleaning functions use the Frame proxy to
    operate directly on the data, so it changes the data in the frame,
    rather than return a new frame with the changed data.
    It is recommended that you copy the data to a new frame on a regular
    basis and work on the new frame.
    This way, you have a fall-back if something does not work as expected::

        next_frame = ia.Frame(last_frame)

In general, the following functions select rows of data based upon the data
in the row.
For details about row selection based upon its data see :doc:`ds_apir`.

Example of data cleaning::

    def clean_animals(row):
        if 'basset hound' in row.animals:
            return 'dog'
        elif 'ginea pig' in row.animals:
            return 'guinea pig'
        else:
            return row.animals

    animals.add_columns(clean_animals, ('animals_cleaned', str))
    animals.drop_columns('animals')
    animals.rename_columns({'animals_cleaned' : 'animals'})

.. _example_frame.drop_rows:

Drop Rows:
----------

The ``drop`` function takes a predicate function and removes all rows for
which the predicate evaluates to ``True``.

Examples:
~~~~~~~~~
Drop any rows in the animals frame where the value in column *Int2* is
negative::

    animals.drop_rows(lambda row: row['Int2'] < 0)

To drop any rows where *Float1PlusFloat2* is empty::

    my_frame.drop_rows(lambda row: row['a'] is None)

To drop any rows where any column is empty::

    my_frame.drop_rows(lambda row: any([cell is None for cell in row]))

.. _example_frame.filter:

Filter Rows:
------------

The ``filter`` function is like ``drop``, except it removes all rows for
which the predicate evaluates to False.

Examples:
~~~~~~~~~

To keep only those rows where field *b* is in the range 0 to 10::

    my_frame.filter(lambda row: 0 >= row['b'] >= 10)

.. _example_frame.drop_duplicates:

Drop Duplicates:
----------------

The ``drop_duplicates`` function performs a row uniqueness comparison across
the whole table.

Examples:
~~~~~~~~~

To drop any rows where the data in *Float1PlusFloat2* and column *b* are
duplicates of some previously evaluated row::

    my_frame.drop_duplicates(['a', 'b'])

To drop all duplicate rows where the columns *User* and *animals* are
duplicate::

    animals.drop_duplicates(['User', 'animals'])
    animals.inspect(animals.row_count)
 
.. _example_frame.drop_columns:

Drop Columns:
-------------

Columns can be dropped either with a string matching the column name or a
list of strings::

    my_frame.drop_columns('b')
    my_frame.drop_columns(['a', 'c'])

.. _example_frame.rename_columns:

Rename Columns:
---------------

Columns can be renamed by giving the existing column name and the new name,
in the form of a dictionary.
Unicode characters should not be used for column names.

Rename *Float1PlusFloat2* to "id"::

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

Columns can be added to the frame using values from other columns as their
value.

.. only:: html

    Add a column *Int1xInt2* as an ia.float64 and fill it with the contents
    of column *Int1* and column *Int2* multiplied together::

        animals.add_columns(lambda row: row.Int1*row.Int2, ('Int1xInt2', ia.float64))

.. only:: latex

    Add a column *Int1xInt2* as an ia.float64 and fill it with the contents
    of column *Int1* and column *Int2* multiplied together::

        animals.add_columns(lambda row: row.Int1*row.Int2, ('Int1xInt2', \\
        ia.float64))

Add a new column *all_ones* and fill the entire column with the value 1::

    animals.add_columns(lambda row: 1, ('all_ones', ia.int64))

.. only:: html

    Add a new column *Float1PlusFloat2* and fill the entire column with the
    value of column *Float1* plus column *Float2*, then save a summary of
    the frame statistics::

        animals.add_columns(lambda row: row.Float1 + row.Float2, ('Float1PlusFloat2', ia.float64))
        summary['Float1PlusFloat2'] = animals.column_summary_statistics('Float1PlusFloat2')

.. only:: latex

    Add a new column *Float1PlusFloat2* and fill the entire column with the
    value of column *Float1* plus column *Float2*, then save a summary of
    the frame statistics::

        animals.add_columns(lambda row: row.Float1 + row.Float2, \\
        ('Float1PlusFloat2', ia.float64))
        summary['Float1PlusFloat2'] = \\
        animals.column_summary_statistics('Float1PlusFloat2')

Add a new column *PWL*, type ia.float64, and fill the value according to
this table:

+-------------------------------------+---------------------------------------------+
| value in column *Float1PlusFloat2*  | value for column *PWL*                      |
+=====================================+=============================================+
| None                                | None                                        |
+-------------------------------------+---------------------------------------------+
| Less than 50                        | *Float1PlusFloat2* times 0.0046 plus 0.4168 |
+-------------------------------------+---------------------------------------------+
| Between 15 and 29 (inclusive)       | *Float1PlusFloat2* times 0.0071 plus 0.3429 |
+-------------------------------------+---------------------------------------------+
| Between -127 and 14 (inclusive)     | *Float1PlusFloat2* times 0.0032 plus 0.4025 |
+-------------------------------------+---------------------------------------------+
| None of the above                   | None                                        |
+-------------------------------------+---------------------------------------------+

An example of Piecewise Linear Transformation::

    def piecewise_linear_transformation(row):
        x = row.Float1PlusFloat2
        if x is None:
            return None
        elif x < 50:
            m, c =0.0046, 0.4168
        elif 50 <= x < 81:
            m, c =0.0071, 0.3429
        elif 81 <= x:
            m, c =0.0032, 0.4025
        else:
            return None
        return m * x + c

    animals.add_columns(piecewise_linear_transformation, ('PWL', ia.float64))

.. only:: html

    Create multiple columns at once by making a function return a list of
    values for the new frame columns::

        animals.add_columns(lambda row: [abs(row.Int1), abs(row.Int2)], [('Abs_Int1', ia.int64), ('Abs_Int2', ia.int64)])

.. only:: latex

    Create multiple columns at once by making a function return a list of
    values for the new frame columns::

        animals.add_columns(lambda row: [abs(row.Int1), abs(row.Int2)], \\
            [('Abs_Int1', ia.int64), ('Abs_Int2', ia.int64)])

.. _ds_dflw_frame_examine:

Examining the Data
==================

To get standard descriptive statistics information about my_frame, use the frame function column_summary_statistics::

    my_frame.column_summary_statistics()

.. _example_frame.group_by:

Group by (and aggregate):
-------------------------

Rows can be grouped together based on matching column values, after which an
aggregation function can be applied on each group, producing a new frame.

Example process of using aggregation based on columns:

#.  given our frame of animals
#.  create a new frame and a Frame *grouped_animals* to access it
#.  group by unique values in column *animals*
#.  average the grouped values in column *Int1* and save it in a
    column *Int1_avg*
#.  add up the grouped values in column *Int1* and save it in a
    column *Int1_sum*
#.  get the standard deviation of the grouped values in column
    *Int1* and save it in a column *Int1_stdev*
#.  average the grouped values in column *Int2* and save it in a
    column *Int2_avg*
#.  add up the grouped values in column *Int2* and save it in a
    column *Int2_sum*

.. only:: html

    Code::

        grouped_animals = animals.group_by('animals', {'Int1': [ia.agg.avg, ia.agg.sum, ia.agg.stdev], 'Int2': [ia.agg.avg, ia.agg.sum]})
        grouped_animals.inspect()

.. only:: latex

    Code::

        grouped_animals = animals.group_by('animals', {'Int1': [ia.agg.avg, \\
            ia.agg.sum, ia.agg.stdev], 'Int2': [ia.agg.avg, ia.agg.sum]})
        grouped_animals.inspect()

.. note::

    The only columns in the new frame will be the grouping column and the
    generated columns.
    In this case, regardless of the original frame size, you will get six
    columns.

Example process of using aggregation based on both column and row together:

#.  Using our data accessed by *animals*, create a new frame and a Frame
    *grouped_animals2* to access it
#.  Group by unique values in columns *animals* and *Int1*
#.  Using the data in the *Float1* column, calculate each group's average,
    standard deviation, variance, minimum, and maximum
#.  Count the number of rows in each group and put that value in column
    *Int2_COUNT*
#.  Count the number of distinct values in column *Int2* for each group and
    put that number in column *Int2_count_distinct*

.. only:: html

    Code::

        grouped_animals2 = animals.group_by(['animals', 'Int1'], {'Float1': [ia.agg.avg, ia.agg.stdev, ia.agg.var, ia.agg.min, ia.agg.max], 'Int2': [ia.agg.count, ia.agg.count_distinct]})

.. only:: latex

        grouped_animals2 = animals.group_by(['animals', 'Int1'], {'Float1': \\
            [ia.agg.avg, ia.agg.stdev, ia.agg.var, ia.agg.min, ia.agg.max], \\
            'Int2': [ia.agg.count, ia.agg.count_distinct]})

Example process of using aggregation based on row:

#.  Using our data accessed by *animals*, create a new frame and a Frame
    *grouped_animals2* to access it
#.  Group by unique values in columns *animals* and *Int1*
#.  Count the number of rows in each group and put that value in column
    *COUNT*

.. only:: html

    Code::

        grouped_animals2 = animals.group_by(['animals', 'Int1'], ia.agg.count)

.. only:: latex

    Code::

        grouped_animals2 = animals.group_by(['animals', 'Int1'], \\
            ia.agg.count)

.. note::

    agg.count is the only full row aggregation function supported at this
    time.

Aggregation currently supports using the following functions:

..  hlist::
    :columns: 5

    * avg
    * count
    * count_distinct
    * max
    * mean
    * min
    * stdev
    * sum
    * :term:`variance <Bias-variance tradeoff>`

.. _example_frame.join:

Join:
-----

Create a **new** frame from a JOIN operation with another frame.

Given two frames *my_frame* (columns *a*, *b*, *c*) and *your_frame* (columns
*b*, *c*, *d*).
For the sake of readability, in these examples we will refer to the frames and
the Frames by the same name, unless needed for clarity::

    my_frame.inspect()                      

      a:str       b:str       c:str
    /-----------------------------------/
      alligator   bear        cat
      auto        bus         car
      apple       berry       cantelope     
      mirror      frog        ball

    your_frame.inspect()
                                        
      b:str       c:ia.int64     d:str
    /----------------------------------/
      bus             871        dog
      berry          5218        frog
      blue              0        log         

Column *b* in both frames is a unique identifier used to tie the two frames
together.
Join *your_frame* to *my_frame*, creating a new frame with a new Frame to
access it;
Include all data from *my_frame* and only that data from *your_frame* which
has a value in *b* that matches a value in *my_frame* *b*::

    our_frame = my_frame.join(your_frame, 'b', how='left')

Result is *our_frame*::

    our_frame.inspect()

      a:str       b:str       c_L:str      c_R:ia.int64   d:str
    /-----------------------------------------------------------/
      alligator   bear        cat          None           None
      auto        bus         car           871           dog
      apple       berry       cantelope    5281           frog
      mirror      frog        ball         None           None

Do it again but this time include only data from *my_frame* and *your_frame*
which have matching values in *b*::

    inner_frame = my_frame.join(your_frame, 'b')
    or
    inner_frame = my_frame.join(your_frame, 'b', how='inner')

Result is *inner_frame*::

    inner_frame.inspect()

      a:str       b:str       c_L:str      c_R:ia.int64   d:str
    /-----------------------------------------------------------/
      auto        bus         car             871         dog
      apple       berry       cantelope      5218         frog

.. only:: html

    If column *b* in *my_frame* and column *d* in *your_frame* are the common
    column:
    Doing it again but including all data from *your_frame* and only that data
    in *my_frame* which has a value in *b* that matches a value in
    *your_frame* *d*::

        right_frame = my_frame.join(your_frame, left_on='b', right_on='d', how='right')

.. only:: latex

    If column *b* in *my_frame* and column *d* in *your_frame* are the common
    column:
    Doing it again but including all data from *your_frame* and only that data
    in *my_frame* which has a value in *b* that matches a value in
    *your_frame* *d*::

        right_frame = my_frame.join(your_frame, left_on='b', right_on='d', \\
        how='right')

Result is *right_frame*::

    right_frame.inspect()

      a:str      b_L:str      c:str      b_R:str    c:ia.int64   d:str
    /---------------------------------------------------------------------/
      None       None         None       bus         871         dog
      mirror     frog         ball       berry      5218         frog
      None       None         None       blue          0         log

.. _example_frame.flatten_column:

Flatten Column:
---------------

The function ``flatten_column`` creates a **new** frame by splitting a
particular column and returns a Frame object.
The column is searched for rows where there is more than one value,
separated by commas.
The row is duplicated and that column is spread across the existing and new
rows.

Given a frame accessed by Frame *my_frame* and the frame has two columns
*a* and *b*.
The "original_data"::

    1-"solo,mono,single"
    2-"duo,double"

.. only:: html

    Bring the data in where it can by worked on::

        my_csv = ia.CsvFile("original_data.csv", schema=[('a', ia.int64), ('b', str)], delimiter='-')
        my_frame = ia.Frame(source=my_csv)

.. only:: latex

    Bring the data in where it can by worked on::

        my_csv = ia.CsvFile("original_data.csv", schema=[('a', ia.int64), \\
        ('b', str)], delimiter='-')
        my_frame = ia.Frame(source=my_csv)

Check the data::

    my_frame.inspect()

      a:ia.int64   b:string
    /---------------------------------/
          1        solo, mono, single
          2        duo, double

Spread out those sub-strings in column *b*::

    your_frame = my_frame.flatten_column('b')

Now check again and the result is::

    your_frame.inspect()

      a:ia.int64   b:str
    /-----------------------/
        1          solo
        1          mono
        1          single
        2          duo
        2          double

.. _ds_dflw_building_a_graph:

--------------
Seamless Graph
--------------

For the examples below, we will use a Frame *my_frame*, which accesses an
arbitrary frame of data consisting of the following columns:

    +----------+---------+-------------------+-------+
    | Employee | Manager | Title             | Years |
    +==========+=========+===================+=======+
    | Bob      | Steve   | Associate         | 1     |
    +----------+---------+-------------------+-------+
    | Jane     | Steve   | Sn Associate      | 3     |
    +----------+---------+-------------------+-------+
    | Anup     | Steve   | Associate         | 3     |
    +----------+---------+-------------------+-------+
    | Sue      | Steve   | Market Analyst    | 1     |
    +----------+---------+-------------------+-------+
    | Mohit    | Steve   | Associate         | 2     |
    +----------+---------+-------------------+-------+
    | Steve    | David   | Marketing Manager | 5     |
    +----------+---------+-------------------+-------+
    | Larry    | David   | Product Manager   | 3     |
    +----------+---------+-------------------+-------+
    | David    | Rob     | VP of Sales       | 7     |
    +----------+---------+-------------------+-------+

    download :download:`here <_downloads/employees.csv>`

Fill the Frame
==============
.. only:: html

    We need to bring the data into a frame::

        employees_frame = ia.Frame(ia.CsvFile("datasets/employees.csv", schema = [('Employee', str),
            ('Manager', str), ('Title', str), ('Years', ia.int64)], skip_header_lines=1), 'employees_frame')
        employees_frame.inspect()

.. only:: latex

    We need to bring the data into a frame::

        employees_frame = ia.Frame(ia.CsvFile("datasets/employees.csv",     \\
          schema = [('Employee', str), ('Manager', str), ('Title', str),    \\
          ('Years', ia.int64)], skip_header_lines=1), 'employees_frame')
        employees_frame.inspect()

Build a Graph
=============

Make an empty graph and give it a name::

    my_graph = ia.graph()
    my_graph.name = "eat_at_joes"

Define the vertex types::

    my_graph.define_vertex_type("employee")
    my_graph.define_vertex_type("manager")
    my_graph.define_vertex_type("title")
    my_graph.define_vertex_type("years")

Define the edge type::

    my_graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=True)

Add data::

    my_graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
    my_graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

Inspect the graph::

    my_graph.vertex_count
    my_graph.edge_count
    my_graph.vertices['Employee'].inspect(20)
    my_graph.edges['worksunder'].inspect(20)

Other Graph Options
===================

Export the graph to a TitanGraph::

    my_titan_graph = my_graph.export_to_titan("titan_graph")

Make a VertexFrame::

    my_vertex_frame = my_graph.vertices("employee")

Make a EdgeFrame::

    my_edge_frame = my_graph.edges("worksunder")

