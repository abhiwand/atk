=====================
Python User Functions
=====================

A :term:`Python User Function (PUF)` is a python function written by the user on the client-side which can execute in a distributed fashion
on the cluster.
The function is serialized and copies are distributed throughout the cluster as part of command execution.
Various API command methods accept PUFs as parameters.
All PUFs run under the constraints of the particular command.

-------------
Frame Row PUF
-------------

A Frame Row PUF is a PUF which operates on a single row of a frame.
The function has one parameter, a *row* object.
Here is an example of a Row PUF that returns True for a row where the column named “score” has a value greater than zero::

    def my_custom_row_func(row):
        return row['score'] > 0

This function would be useful in a Frame filter command, which filters a data frame keeping only those rows which meet certain criteria,
-- in this case, only rows with scores greater than zero::

    csv = CsvFile(“tresults.txt”, [(‘test’, str), (‘score’, int32)])

    frame = BigFrame(csv)

    frame.filter(my_custom_row_func)

In this example, the filter command iterates over every row in the frame and evaluates the user-defined function on each one and
keeps only those rows which evaluate to True.

Row Object Parameter
====================

The Row object is a read-only dictionary-like structure which contains the cell values for a particular row.
The values are accessible using the column name, with typical Python square bracket lookup, as shown in the example above.
The value of cell in column 'score' is accessed like this::

    row['score']

The cell values may also be accessed using *dot-member* notation.
Here is an equivalent row function::

    def my_custom_row_func2(row):
        return row.score > 0

The *dot-member* notation is provided for convenience (it follows the pandas DataFrame technique) and only works for columns
whose names are legal python variable names (i.e. alphanumeric plus underscore, not starting with a number).
Columns whose names do not meet this criteria must be referenced using square brackets with strings.

A *row* object instance may not be written to.
Assigning a value to a cell (existing or a new one) will cause an error.
New values must be added to a frame using the Frame’s add_columns method.

The *row* object supports a few dictionary-like methods:

* *keys()* -- returns a list of column names
* *values()* -- returns a list of column values
* *items()* -- returns a list of (key, value) tuples
* *types()* -- returns a list of column types

These methods all produce lists in the same order, in other words, it is safe to correlate their indices.

Also, iterating on the row object is the equivalent of iterating on items().
For example::

    def row_sum(row):
        """
        sums the values in the row, except for column "name"
        """
        try:
            s = 0
            for k, v in row:
                if k != 'name':
                    sum += v
            return s
        except:
            return -1

    frame.add_columns(row_sum, ('sum', int32))

.. Note::

    This example is for illustration only.
    There are other, perhaps more pythonic, ways of doing this, like using a list comprehension.

--------------
PUF Guidelines
--------------

Here are some guidelines to follow when writing a PUF:

#. Error handling:
    Include error handling.
    If the function execution raises an exception, it will cause the entire command to fail and possible leave the BigFrame
    or BigGraph in an incomplete state.
    The best practice is to put all our PUF functionality in a ``try: except:`` block, where the ``except:`` clause returns
    a default value or performs a benign side effect.
    See the ``row_sum`` function example above, where we used a ``try: except:`` block and produced a -1 for rows which caused errors.

#. Dependencies:
    All dependencies used in the PUF must be available in **the same Python code file** as the PUF or available in the server's
    installed Python libraries.
    The serialization technique to get the code distributed throughout the cluster will only serialize dependencies in the same
    Python module (in other words, file) right now.
#. Simplicity:
    Stay within the intended simple context of the given command, like a row operation.
    Do not try to call other API methods or perform fancy system operations (which will fail due to permissions).
#. Performance:
    Be mindful of performance.
    These functions execute on each row of data, in other words, several times.
#. Printing:
    Printing (to stdout, stderr, …) within our PUF will not show up in the client REPL.
    Such messages will usually end up in the server logs.
    In general, avoid printing.
#. Lambda:
    Lambda syntax is valid, but discouraged::

        frame.filter(lambda row: row.score > 0)

    This is legal and attractively shorter to write.
    However, lambdas do not provide error handling, nor do they have a “name” that would be useful in exception stack traces.
    They cannot be tested in isolation nor have embedded documentation.
    Lambdas are not very shareable.
#. Closures:
    Closures are read-only.
    Any closed over variables are copied during serialization, so it is not possible to obtain side-effects.
#. Multiple executions:
    Do not make any assumptions about how many times the function may get executed.
#. Parameterizing PUFs:
    Parameterizing PUFs is possible using Python techniques of closures and nesting function definitions.
    For example, the Row PUF only takes a single row object parameter.
    It could be useful to have a row function that takes a few other parameters.
    Let’s augment the row_sum function above to take a list of columns to ignore::

        def get_row_sum_func(ignore_list):
            """
            returns a row function which sums the values in the row,
            except for ignored columns
            """
            def row_sum2(row):
                try:
                    s = 0
                    for k, v in row:
                        if k not in ignore_list:
                            s += v
                    return s
                except:
                    return -1
                return row_sum2

        frame.add_columns(get_row_sum_func(['name', 'address']), ('sum', int32))

    The ``row_sum2`` function closes over the *ignore_list* argument making it available to the row function that executes on each row.
