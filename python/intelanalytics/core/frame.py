##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

import logging


logger = logging.getLogger(__name__)
from intelanalytics.meta.api import get_api_decorator, check_api_is_loaded, api_context
api = get_api_decorator(logger)

from intelanalytics.meta.udf import has_python_user_function_arg
from intelanalytics.core.iatypes import valid_data_types
from intelanalytics.core.column import Column
from intelanalytics.core.errorhandle import IaError
from intelanalytics.meta.namedobj import name_support
from intelanalytics.meta.metaprog import CommandLoadable, doc_stubs_import

#from intelanalytics.core.deprecate import deprecated, raise_deprecation_warning


def _get_backend():
    from intelanalytics.meta.config import get_frame_backend
    return get_frame_backend()

__all__ = ["drop_frames", "drop_graphs", "EdgeRule", "Frame", "get_frame", "get_frame_names", "get_graph", "get_graph_names", "TitanGraph", "VertexRule"]

# BaseFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubs_BaseFrame
    doc_stubs_import.success(logger, "DocStubsBaseFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsBaseFrame", e)
    class DocStubs_BaseFrame(object): pass


# Frame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsFrame
    doc_stubs_import.success(logger, "DocStubsFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsFrame", e)
    class DocStubsFrame(object): pass


# VertexFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsVertexFrame
    doc_stubs_import.success(logger, "DocStubsVertexFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsVertexFrame", e)
    class DocStubsVertexFrame(object): pass


# EdgeFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsEdgeFrame
    doc_stubs_import.success(logger, "DocStubsEdgeFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsEdgeFrame", e)
    class DocStubsEdgeFrame(object): pass


@api
@name_support('frame')
class _BaseFrame(DocStubs_BaseFrame, CommandLoadable):
    _entity_type = 'frame'

    def __init__(self):
        CommandLoadable.__init__(self)

    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name == '_backend':
            raise AttributeError('_backend')
        try:
            return super(_BaseFrame, self).__getattribute__(name)
        except AttributeError:
            return self._get_column(name, AttributeError, "Attribute '%s' not found")

    # We are not defining __setattr__.  Columns must be added explicitly

    def __getitem__(self, key):
        if isinstance(key, slice):
            raise TypeError("Slicing not supported")
        return self._get_column(key, KeyError, '%s')

    def _get_column(self, column_name, error_type, error_msg):
        data_type_dict = dict(self.schema)
        try:
            if isinstance(column_name, list):
                return [Column(self, name, data_type_dict[name]) for name in column_name]
            return Column(self, column_name, data_type_dict[column_name])
        except KeyError:
            raise error_type(error_msg % column_name)

    # We are not defining __setitem__.  Columns must be added explicitly

    # We are not defining __delitem__.  Columns must be deleted w/ drop_columns

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(_BaseFrame, self).__repr__() + " (Unable to collect metadata from server)"

    def __len__(self):
        try:
            return len(self.schema)
        except:
            IaError(logger)

    def __contains__(self, key):
        return key in self.column_names  # not very efficient, usage discouraged

    class _FrameIter(object):
        """
        (Private)
        Iterator for Frame - frame iteration works on the columns, returns BigColumn objects
        (see Frame.__iter__)

        Parameters
        ----------
        frame : Frame
            A Frame object.

        """

        def __init__(self, frame):
            self.frame = frame
            # Grab schema once for the duration of the iteration
            # Consider the behavior here --alternative is to ask
            # the backend on each iteration (and there's still a race condition)
            self.schema = frame.schema
            self.i = 0  # the iteration index

        def __iter__(self):
            return self

        def next(self):
            if self.i < len(self.schema):
                column = Column(self.frame, self.schema[self.i][0], self.schema[self.i][1])
                self.i += 1
                return column
            raise StopIteration

    def __iter__(self):
            return Frame._FrameIter(self)

    def __eq__(self, other):
            if not isinstance(other, Frame):
                return False
            return self._id == other._id

    def __hash__(self):
        return hash(self._id)


    @property
    @api
    def column_names(self):
        """
        Column identifications in the current Frame.

        Returns
        -------
        list : list of str
            A list of the names of the columns.

        Examples
        --------
        Create a Frame object from the data described by schema *my_csv*.
        Get the column names::

            my_frame = ia.Frame(source='my_csv')
            my_columns = my_frame.column_names
            print my_columns

        Now, assuming the schema *my_csv* described three columns *col1*,
        *col2*, and *col3*, the result is::

            ["col1", "col2", "col3"]

        """
        return [name for name, data_type in self._backend.get_schema(self)]

    @property
    @api
    def name(self):
        """
        Retrieve the name of the current frame.

        Returns
        -------
        str : identification
            The name of the frame

        Examples
        --------
        Create a frame and give it the name "Flavor Recipes"; read the name back to check it::

            frame = ia.Frame(name="Flavor Recipes")
            given_name = frame.name
            print given_name

        The result given is::

            "Flavor Recipes"

        """
        return self._backend.get_name(self)

    @name.setter
    @api
    def name(self, value):
        """
        Assign the name to the current frame.

        Examples
        --------
        Assign the name "movies" to the current frame::

            my_frame.name = "movies"

        """
        self._backend.rename_frame(self, value)

    @property
    @api
    def row_count(self):
        """
        Number of rows in the current frame.

        Returns
        -------
        int : quantity
            The number of rows in the frame

        Examples
        --------
        Get the number of rows::

            my_frame.row_count

        The result given is::

            81734

        """
        return self._backend.get_row_count(self, None)


    @property
    @api
    def schema(self):
        """
        Current frame column names and types.

        The schema of the current frame is a list of column names and
        associated data types.
        It is retrieved as a list of tuples.
        Each tuple has the name and data type of one of the frame's columns.

        Returns
        -------
        list : list of tuples

        Examples
        --------
        Given that we have an existing data frame *my_data*, create a Frame,
                then show the frame schema::

            BF = ia.get_frame('my_data')
            print BF.schema

        The result is::

            [("col1", str), ("col1", numpy.int32)]

        """
        return self._backend.get_schema(self)

    @api
    @has_python_user_function_arg
    def add_columns(self, func, schema):
        """
        Add columns to current frame.

        Assigns data to column based on evaluating a function for each row.

        Parameters
        ----------
        func : row function
            Function which takes the values in the row and produces a value
            or collection of values for the new cell(s).

        schema : [ tuple | list of tuples ]
            The schema for the results of the functions, indicating the new
            column(s) to add.  Each tuple provides the column name and data
            type and is of the form (str, type).

        Notes
        -----
        1)  The row function ('func') must return a value in the same format as
            specified by the schema.
            See :doc:`ds_apir`.
        #)  Unicode in column names is not supported and will likely cause the
            drop_frames() function (and others) to fail!

        Examples
        --------
        Given a Frame *my_frame* identifying a data frame with two int32
        columns *column1* and *column2*.
        Add a third column *column3* as an int32 and fill it with the
        contents of *column1* and *column2* multiplied together::

            my_frame.add_columns(lambda row: row.column1*row.column2, ('column3', int32))

        The frame now has three columns, *column1*, *column2* and *column3*.
        The type of *column3* is an int32, and the value is the product of
        *column1* and *column2*.

        Add a str column *column4* that is empty::

            my_frame.add_columns(lambda row: '', ('column4', str))

        The Frame object *my_frame* now has four columns *column1*, *column2*,
        *column3*, and *column4*.
        The first three columns are int32 and the fourth column is str.
        Column *column4* has an empty str ('') in every row.

        Multiple columns can be added at the same time.
        Add a column *a_times_b* and fill it with the contents of column *a*
        multiplied by the contents of column *b*.
        At the same time, add a column *a_plus_b* and fill it with the contents
        of column *a* plus the contents of column *b*:
        
        .. only:: html

            ::

                my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b], [("a_times_b", float32), ("a_plus_b", float32))

        .. only:: latex

            ::

                my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b],    \\
                    [("a_times_b", float32), ("a_plus_b", float32))

        Two new columns are created, "a_times_b" and "a_plus_b", with the
        appropriate contents.

        Given a frame of data and Frame *my_frame* points to it.
        In addition we have defined a function *func*.
        Run *func* on each row of the frame and put the result in a new int
        column *calculated_a*::

            my_frame.add_columns( func, ("calculated_a", int))

        Now the frame has a column *calculated_a* which has been filled with
        the results of the function *func*.

        Functions must return their value in the same format as the column is
        defined.
        In most cases this is automatically the case, but sometimes it is less
        obvious.
        Given a function *function_b* which returns a value in a list, store
        the result in a new column *calculated_b*::

            my_frame.add_columns(function_b, ("calculated_b", float32))

        This would result in an error because function_b is returning a value
        as a single element list like [2.4], but our column is defined as a
        tuple.
        The column must be defined as a list::

            my_frame.add_columns(function_b, [("calculated_b", float32)])

        More information on row functions can be found at :doc:`ds_apir`

        """
        # For further examples, see :ref:`example_frame.add_columns`.
        self._backend.add_columns(self, func, schema)

    @api
    def copy(self, columns=None, where=None, name=None):
        """
        Create new frame from current frame.

        Copy frame or certain frame columns entirely or filtered.
        Useful for frame query.

        Parameters
        ----------
        columns : [ str | list of str | dict ] (optional)
            If not None, the copy will only include the columns specified.
            If a dictionary is used, the str pairs represent a column
            renaming, {source_column_name: destination_column_name}.
            Default is None.

        where : row function (optional)
            If not None, only those rows which evaluate to True will be copied.
            Default is None.

        name : str (optional)
            Name of the copied frame.
            Default is None.

        Returns
        -------
        Frame | access to new frame
            A new Frame object accessing data in a new frame which is a copy of
            the original frame.

        Examples
        --------
        Build a Frame from a csv file with 5 million rows of data; call the
        frame "cust"::

            my_frame = ia.Frame(source="my_data.csv")
            my_frame.name("cust")

        Given the frame has columns *id*, *name*, *hair*, and *shoe*.
        Copy it to a new frame::

            your_frame = my_frame.copy()

        Now we have two frames of data, each with 5 million rows.
        Checking the names::

            print my_frame.name()
            print your_frame.name()

        Gives the results::

            "cust"
            "frame_75401b7435d7132f5470ba35..."

        Now, let's copy *some* of the columns from the original frame::

            our_frame = my_frame.copy(['id', 'hair'])

        Our new frame now has two columns, *id* and *hair*, and has 5 million
        rows.
        Let's try that again, but this time change the name of the *hair*
        column to *color*::

            last_frame = my_frame.copy(('id': 'id', 'hair': 'color'))

        """
        return self._backend.copy(self, columns, where, name)

    @api
    def count(self, where):
        """
        Counts the number of rows which meet given criteria.

        Parameters
        ----------
        where : function
            Function or :term:`lambda` which takes a row argument and evaluates
            to a boolean value.

        Returns
        -------
        int : count
            number of rows for which the where function evaluated to True
        """
        return self._backend.get_row_count(self, where)

    def download(self, count=100, offset=0, columns=None):
        """
        Download a frame from the server into client workspace.

        Copies an intelanalytics Frame into a Pandas DataFrame.

        Parameters
        ----------
        count : int (optional)
            The number of rows to copy from the currently active intelanalytics
            Frame.
            Default is 100.

        offset : int (optional)
            The number of rows to skip before copying.
            Default is 0.

        columns : str or iterable of str (optional)
            The columns to be included in the result.
            Default is all.

        Returns
        -------
        Pandas Data Frame
            A new pandas data frame object containing copies of all or subset
            of the original frame.

        Examples
        --------
        Frame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 500 rows::

            pandas_frame = my_frame.download( 500 )

        We now have a new frame accessed by a pandas DataFrame *pandas_frame*
        with a copy of the first 500 rows of the original frame.

        If we use the function with an offset like::

            pandas_frame = my_frame.take( 500, 100 )

        We end up with a new frame accessed by the pandas DataFrame
        *pandas_frame* again, but this time it has a copy of rows 101 to 600 of
        the original frame.

        """
        import pandas as pd
        from intelanalytics.core.iatypes import vector
        result = self._backend.take(self, count, offset, columns)
        headers, data_types = zip(*result.schema)

        pandas_df = pd.DataFrame(result.data, columns=headers)

        for i, dtype in enumerate(data_types):
            dtype_str = valid_data_types.to_string(dtype) if dtype != vector else "object"
            pandas_df[[headers[i]]] = pandas_df[[headers[i]]].astype(dtype_str)
        return pandas_df


    @api
    @has_python_user_function_arg
    def drop_rows(self, predicate):
        """
        Erase any rows in the current frame which qualify.

        Parameters
        ----------
        predicate : function
            Function or :term:`lambda` which takes a row argument and
            evaluates to a boolean value.

        Examples
        --------
        For this example, my_frame is a Frame object accessing a frame with
        lots of data for the attributes of ``lions``, ``tigers``, and ``ligers``.
        Get rid of the ``lions`` and ``tigers``::

            my_frame.drop_rows(lambda row: row.animal_type == "lion" or
                row.animal_type == "tiger")

        Now the frame only has information about ``ligers``.

        More information on row functions can be found at :doc:`ds_apir`


        """
        # For further examples, see :ref:`example_frame.drop_rows`
        self._backend.drop(self, predicate)

    @api
    @has_python_user_function_arg
    def filter(self, predicate):
        """
        Save any rows that qualify, erase the others.

        Select all rows which satisfy a predicate.

        Parameters
        ----------
        predicate : function
            Function definition or lambda which takes a row argument and
            evaluates to a boolean value.

        Examples
        --------
        For this example, *my_frame* is a Frame object with lots of data for
        the attributes of ``lizards``, ``frogs``, and ``snakes``.
        Get rid of everything, except information about ``lizards`` and
        ``frogs``::

            def my_filter(row):
                return row['animal_type'] == 'lizard' or row['animal_type'] == "frog"

            my_frame.filter(my_filter)

        The frame now only has data about ``lizards`` and ``frogs``.

        More information on row functions can be found at :doc:`ds_apir`.



        """
        # For further examples, see :ref:`example_frame.filter`
        self._backend.filter(self, predicate)

    @api
    def get_error_frame(self):
        """
        Create a frame with recording any errors.

        When a frame is loaded, parse errors go into a separate data frame so
        they can be inspected.
        No error frame is created if there were no parse errors.

        Returns
        -------
        Frame : error frame object
            A new object accessing a frame that contains the parse errors of
            the currently active Frame or None if no error frame exists
        """
        return self._backend.get_frame_by_id(self._error_frame_id)

    @api
    def group_by(self, group_by_columns, *aggregation_arguments):
        """
        Create summarized frame.

        Creates a new frame and returns a Frame object to access it.
        Takes a column or group of columns, finds the unique combination of
        values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        group_by_columns : [ str | list of str ]
            Column name or list of column names.

        aggregation_arguments
            Aggregation function based on entire row, and/or
            dictionaries (one or more) of { column name str : aggregation
            function(s) }.

        Returns
        -------
        Frame : frame reference
            A new object accessing a new aggregated frame.

        Notes
        -----
        *   The column names created by aggregation functions in the new frame
            are the original column name appended with the '_' character and
            the aggregation function.
            For example, if the original field is 'a' and the function is 'avg',
            the resultant column is named 'a_avg'.
        *   An aggregation argument of 'count' results in a column named
            'count'.

        Supported aggregation functions:

        ..  hlist::
            :columns: 5

            * avg
            * count
            * max
            * mean
            * min
            * stdev
            * sum
            * :term:`variance <Bias-variance tradeoff>`
            * distinct


        Examples
        --------
        For setup, we will use a Frame *my_frame* accessing a frame with a
        column *a*::

            my_frame.inspect()

             a:str
           /-------/
             cat
             apple
             bat
             cat
             bat
             cat

        Create a new frame, combining similar values of column *a*,
        and count how many of each value is in the original frame::

            new_frame = my_frame.group_by('a', agg.count)
            new_frame.inspect()

             a:str       count:int
           /-----------------------/
             cat             3
             apple           1
             bat             2

        In this example, 'my_frame' is accessing a frame with three columns,
        *a*, *b*, and *c*::

            my_frame.inspect()

             a:int   b:str   c:float
           /-------------------------/
             1       alpha     3.0
             1       bravo     5.0
             1       alpha     5.0
             2       bravo     8.0
             2       bravo    12.0

        Create a new frame from this data, grouping the rows by unique
        combinations of column *a* and *b*.
        Average the value in *c* for each group::

            new_frame = my_frame.group_by(['a', 'b'], {'c' : agg.avg})
            new_frame.inspect()

             a:int   b:str   c_avg:float
           /-----------------------------/
             1       alpha     4.0
             1       bravo     5.0
             2       bravo    10.0

        For this example, we use *my_frame* with columns *a*, *c*, *d*, and *e*::

            my_frame.inspect()

             a:str   c:int   d:float e:int
           /-------------------------------/
             ape     1       4.0     9
             ape     1       8.0     8
             big     1       5.0     7
             big     1       6.0     6
             big     1       8.0     5

        Create a new frame from this data, grouping the rows by unique
        combinations of column *a* and *c*.
        Count each group; for column *d* calculate the average, sum and minimum
        value.
        For column *e*, save the maximum value::

            new_frame = my_frame.group_by(['a', 'c'], agg.count, {'d':
                [agg.avg, agg.sum, agg.min], 'e': agg.max})

             a:str   c:int   count:int  d_avg:float  d_sum:float   d_min:float   e_max:int
           /-------------------------------------------------------------------------------/
             ape     1       2          6.0          12.0          4.0           9
             big     1       3          6.333333     19.0          5.0           7


        """
        # For further examples, see :ref:`example_frame.group_by`.
        return self._backend.group_by(self, group_by_columns, aggregation_arguments)


    @api
    def inspect(self, n=10, offset=0, columns=None):
        """
        Print the frame data in readable format.

        Parameters
        ----------
        n : int
            The number of rows to print.
            Default is 10.

        offset : int (optional)
            The number of rows to skip before printing.
            Default is 0.

        columns : [ str | iterable of str ] (optional)
            Specify the columns to be included in the result.
            Default is all.

        Returns
        -------
        data
            Formatted for ease of human inspection.

        Examples
        --------
        For this example, let's say we have a frame of data and a Frame to
        access it.
        Let's look at the first 10 rows of data::

            print my_frame.inspect()

            column defs ->  animal:str  name:str    age:int     weight:float
                          /--------------------------------------------------/
            frame data ->   lion        George        8            542.5
                            lion        Ursula        6            495.0
                            ape         Ape          41            400.0
                            elephant    Shep          5           8630.0



        """
        # For another example, see :ref:`example_frame.inspect`.
        return self._backend.inspect(self, n, offset, columns)

    @api
    def join(self, right, left_on, right_on=None, how='inner', name=None):
        """
        New frame from current frame and another frame.

        Create a new frame from a SQL JOIN operation with another frame.
        The frame on the 'left' is the currently active frame.
        The frame on the 'right' is another frame.
        This function takes a column in the left frame and matches it's values
        with a column in the right frame.
        Using the default 'how' option ['inner'] will only allow data in the
        resultant frame if both the left and right frames have the same value
        in the matching column.
        Using the 'left' 'how' option will allow any data in the resultant
        frame if it exists in the left frame, but will allow any data from the
        right frame if it has a value in it's column which matches the value in
        the left frame column.
        Using the 'right' option works similarly, except it keeps all the data
        from the right frame and only the data from the left frame when it
        matches.

        Parameters
        ----------
        right : Frame
            Another frame to join with.

        left_on : str
            Name of the column in the left frame used to match up the two
            frames.

        right_on : str (optional)
            Name of the column in the right frame used to match up the two
            frames.
            Default is the same as the left frame.

        how : str ['left' | 'right' | 'inner'] (optional)
            How to qualify the data to be joined together.
            Default is 'inner'.

        name : str (optional)
            Name for the resulting new joined frame
            Default is None.

        Returns
        -------
        Frame : combined frames
            A new object accessing a new joined frame.

        Notes
        -----
        When a column is named the same in both frames, it will result in two
        columns in the new frame.
        The column from the *left* frame (originally the current frame) will be
        copied and the column name will have the str "_L" added to it.
        The same thing will happen with the column from the *right* frame,
        except its name has the str "_R" appended.

        It is recommended that you rename the columns to meaningful terms prior
        to using the ``join`` method.
        Keep in mind that unicode in column names will likely cause the
        drop_frames() function (and others) to fail!

        Examples
        --------
        For this example, we will use a Frame *my_frame* accessing a frame with
        columns *a*, *b*, *c*, and a Frame *your_frame* accessing a frame with
        columns *a*, *d*, *e*.
        Join the two frames keeping only those rows having the same value in
        column *a*::

            my_frame = Frame(schema1)
            your_frame = Frame(schema2)
            joined_frame = my_frame.join(your_frame, 'a')

        Now, joined_frame is a Frame accessing a frame with the columns *a_L*,
        *a_R*, *b*, *c*, *d*, and *e*.
        The data in the new frame will be from the rows where column 'a' was
        the same in both frames.

        Now, using a single Frame *my_frame* accessing a frame with the columns
        *b* and *book*.
        Build a new frame, but remove any rows where the values in *b* and
        *book* do not match::

            joined_frame = your_frame.join(your_frame, left_on='b', right_on='book',
                how='inner')

        We end up with a new Frame *joined_frame* accessing a new frame with
        all the original columns, but only those rows where the data in the
        original frame in column *b* matched the data in column *book*.

        """
        # For further examples, see :ref:`example_frame.join`.
        return self._backend.join(self, right, left_on, right_on, how, name)

    @api
    def sort(self, columns, ascending=True):
        """
        Sort the data in a frame.

        Sort a frame by column values either ascending or descending.

        Parameters
        ----------
        columns : [ str | list of str | list of tuples ]
            Either a column name, a list of column names, or a list of tuples
            where each tuple is a name and an ascending bool value

        ascending: bool (optional)
            True for ascending, False for descending
            Default is True.

        Examples
        --------
        Sort a single column::

            frame.sort('column_name')

        Sort a single column ascending::

            frame.sort('column_name', True)

        Sort a single column descending::

            frame.sort('column_name', False)

        Sort multiple columns::

            frame.sort(['col1', 'col2'])

        Sort multiple columns ascending::

            frame.sort(['col1', 'col2'], True)

        Sort multiple columns descending::

            frame.sort(['col1', 'col2'], False)

        Sort multiple columns: 'col1' ascending and 'col2' descending::

            frame.sort([ ('col1', True), ('col2', False) ])

        """
        return self._backend.sort(self, columns, ascending)

    @api
    def take(self, n, offset=0, columns=None):
        """
        Get data subset.

        Take a subset of the currently active Frame.

        Parameters
        ----------
        n : int
            The number of rows to copy from the currently active Frame.

        offset : int (optional)
            The number of rows to skip before copying.
            Default is 0.

        columns : [ str | iterable of str ] (optional)
            Specify the columns to be included in the result.
            Default is None, meaning all columns are to be included.

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain
        number of rows, the rows obtained may be different every time the
        command is executed, even if the parameters do not change.

        Returns
        -------
        list : list of lists of row data
            A list composed of the data from the frame.
            Each item of the overall list is a list of the values of the
            columns for one row of the original frame.

        Examples
        --------
        Frame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 5000 rows::

            my_data_list = my_frame.take( 5000 )

        We now have a list of data from the original frame.

            print my_data_list

            [[ 1, "text", 3.1415962 ]
             [ 2, "bob", 25.0 ]
             [ 3, "weave", .001 ]
             ...]

        If we use the function with an offset like::

            my_data_list = my_frame.take( 5000, 1000 )

        We end up with a new list, but this time it has a copy of the data from
        rows 1001 to 5000 of the original frame.

        """
        result = self._backend.take(self, n, offset, columns)
        return result.data



@api
class Frame(DocStubsFrame, _BaseFrame):
    """
    Data handle.

    Class with information about a large row and columnar data store in a
    frame,
    Has information needed to modify data and table structure.

    Parameters
    ----------
    source : [ CsvFile | Frame | BigColumn(s) ]
        A source of initial data.

    name : str (optional)
        The name of the newly created frame.
        Default is None.

    Returns
    -------
    class | Frame object
        An object with access to the frame.

    Notes
    -----
    A frame with no name is subject to garbage collection.

    If a str in the CSV file starts and ends with a double-quote (")
    character, the character is stripped off of the data before it is put into
    the field.
    Anything, including delimiters, between the double-quote characters is
    considered part of the str.
    If the first character after the delimiter is anything other than a
    double-quote character, the str will be composed of all the characters
    between the delimiters, including double-quotes.
    If the first field type is str, leading spaces on each row are
    considered part of the str.
    If the last field type is str, trailing spaces on each row are
    considered part of the str.

    Examples
    --------
    Create a new frame based upon the data described in the CsvFile object
    *my_csv_schema*.
    Name the frame "my_frame".
    Create a Frame *g* to access the data::

        g = ia.Frame(my_csv_schema, "my_frame")

    A Frame object has been created and *g* is its proxy.
    It brought in the data described by *my_csv_schema*.
    It is named *my_frame*.

    Create an empty frame; name it "your_frame"::

        h = ia.Frame(name='your_frame')

    A frame has been created and Frame *h* is its proxy.
    It has no data yet, but it does have the name *your_frame*.

    """

    _entity_type = 'frame:'

    def __init__(self, source=None, name=None, _info=None):
        self._error_frame_id = None
        self._id = 0
        self._ia_uri = None
        with api_context(logger, 3, self.__init__, self, source, name, _info):
            check_api_is_loaded()
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create(self, source, name, _info)
            logger.info('Created new frame "%s"', new_frame_name)

    @api
    def append(self, data):
        """
        Adds more data to the current frame.

        Parameters
        ----------
        data : Frame
            A Frame accessing the data being added.

        Examples
        --------
        Given a frame with a single column *col_1* and a frame with two columns
        *col_1* and *col_2*.
        Column *col_1* means the same thing in both frames.
        The Frame *my_frame* points to the first frame and *your_frame* points
        to the second.
        Add the contents of *your_frame* to *my_frame*::

            my_frame.append(your_frame)

        Now the first frame has two columns, *col_1* and *col_2*.
        Column *col_1* has the data from *col_1* in both original frames.
        Column *col_2* has None (undefined) in all of the rows in the original
        first frame, and has the value of the second frame column *col_2* in
        the rows matching the new data in *col_1*.

        Breaking it down differently, the original rows referred to by
        *my_frame* have a new column *col_2* and this new column is filled with
        non-defined data.
        The frame referred to by *your_frame* is then added to the bottom.

        """
        self._backend.append(self, data)


@api
class VertexFrame(DocStubsVertexFrame, _BaseFrame):
    """
    A list of Vertices owned by a Graph..

    A VertexFrame is similar to a Frame but with a few important differences:

    -   VertexFrames are not instantiated directly by the user, instead they
        are created by defining a vertex type in a graph
    -   Each row of a VertexFrame represents a vertex in a graph
    -   VertexFrames have many of the same methods as Frames but not all (for
        example, flatten_column())
    -   VertexFrames have extra methods not found on Frames (for example,
        add_vertices())
    -   Removing a vertex (or row) from a VertexFrame also removes edges
        connected to that vertex from the graph
    -   VertexFrames have special system columns (_vid, _label) that are
        maintained automatically by the system and cannot be modified by the
        user
    -   VertexFrames have a special user defined id column whose value uniquely
        identifies the vertex
    -   "Columns" on a VertexFrame can also be thought of as "properties" on
        vertices

    Examples
    --------
    Given a data file, create a frame, move the data to graph and then define a
    new VertexFrame and add data to it::

        csv = ia.CsvFile("/movie.csv", schema= [('user_id', int32), \
                                            ('user_name', str),     \
                                            ('movie_id', int32),    \
                                            ('movie_title', str),   \
                                            ('rating', str)])
        my_frame = ia.Frame(csv)
        my_graph = ia.Graph()
        my_graph.define_vertex_type('users')
        my_vertex_frame = my_graph.vertices['users']
        my_vertex_frame.add_vertices(my_frame, 'user_id', ['user_name', 'age'])

    Retrieve a previously defined graph and retrieve a VertexFrame from it::

        my_graph = ia.get_graph("your_graph")
        my_vertex_frame = my_graph.vertices["your_label"]

    Calling methods on a VertexFrame::

        my_vertex_frame.vertices["your_label"].inspect(20)

    Convert a VertexFrame to a frame::

        new_Frame = my_vertex_frame.vertices["label"].copy()

    """
    # For other examples, see :ref:`example_frame.bigframe`.

    # TODO - Review Parameters, Examples

    _entity_type = 'frame:vertex'

    def __init__(self, source=None, graph=None, label=None, _info=None):
        try:
            check_api_is_loaded()
            self._error_frame_id = None
            self._id = 0
            self._ia_uri = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create_vertex_frame(self, source, label, graph, _info)
            logger.info('Created new vertex frame "%s"', new_frame_name)
        except:
            error = IaError(logger)
            raise error

    def drop_vertices(self, predicate):
        """
        Delete rows that qualify.

        Parameters
        ----------
        predicate : function
            Function or :term:`lambda` which takes a row argument and evaluates
            to a boolean value.

        Examples
        --------
        Given VertexFrame object *my_vertex_frame* accessing a graph with lots
        of data for the attributes of ``lions``, ``tigers``, and ``ligers``.
        Get rid of the ``lions`` and ``tigers``:

        .. only:: html

            ::

                my_vertex_frame.drop_vertices(lambda row: row.animal_type == "lion" or row.animal_type == "tiger")

        .. only:: latex

            ::

                my_vertex_frame.drop_vertices(lambda row:   \
                    row.animal_type == "lion" or            \
                    row.animal_type == "tiger")

        Now the frame only has information about ``ligers``.

        More information on row functions can be found at :doc:`ds_apir`

        """
        self._backend.filter_vertices(self, predicate, keep_matching_vertices=False)

    def filter(self, predicate):
        self._backend.filter_vertices(self, predicate)


@api
class EdgeFrame(DocStubsEdgeFrame, _BaseFrame):
    """
    A list of Edges owned by a Graph.

    An EdgeFrame is similar to a Frame but with a few important differences:

    -   EdgeFrames are not instantiated directly by the user, instead they are
        created by defining an edge type in a graph
    -   Each row of an EdgeFrame represents an edge in a graph
    -   EdgeFrames have many of the same methods as Frames but not all
    -   EdgeFrames have extra methods not found on Frames (e.g. add_edges())
    -   EdgeFrames have a dependency on one or two VertexFrames.
        Adding an edge to an EdgeFrame requires either vertices to be present
        or for the user to specify create_missing_vertices=True.
    -   EdgeFrames have special system columns (_eid, _label, _src_vid,
        _dest_vid) that are maintained automatically by the system and cannot
        be modified by the user
    -   "Columns" on an EdgeFrame can also be thought of as "properties" on
        Edges

    Examples
    --------
    Given a data file, create a frame, move the data to the graph, define a
    couple of vertex types, then define an edge type
    ``ratings`` as directed edges from ``user`` vertices to ``movie`` vertices::

        csv = ia.CsvFile("/movie.csv", schema= [('user_id', int32),
                                            ('user_name', str),
                                            ('movie_id', int32),
                                            ('movie_title', str),
                                            ('rating', str)])
        my_frame = ia.Frame(csv)
        my_graph = ia.Graph()
        my_graph.define_vertex_type('users')
        my_graph.define_vertex_type('movies')
        my_graph.define_edge_type('ratings','users','movies',directed=True)

    Add data to the graph::

        graph.vertices['users'].add_vertices(my_frame, 'user_id', ['user_name'])
        graph.vertices['movies].add_vertices(my_frame, 'movie_id', ['movie_title])
        my_edge_frame = graph.edges['ratings']
        my_edge_frame.add_edges(my_frame, 'user_id', 'movie_id', ['rating']

    Retrieve a previously defined graph and retrieve an EdgeFrame from it::

        g = ia.get_graph("your_graph")
        f = g.edges["your_label"]

    Calling methods on an EdgeFrame::

        g.edges["your_label"].inspect(20)

    Convert an EdgeFrame to a Frame using the copy method::

        f = g.edges["label"].copy()

    """
    # For other examples, see :ref:`example_frame.bigframe`.

    # TODO - Review Parameters, Examples

    _entity_type = 'frame:edge'

    def __init__(self, source=None, graph=None, label=None, src_vertex_label=None, dest_vertex_label=None, directed=None, _info=None):
        try:
            check_api_is_loaded()
            self._error_frame_id = None
            self._id = 0
            self._ia_uri = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            _BaseFrame.__init__(self)
            new_frame_name = self._backend.create_edge_frame(self, source, label, graph, src_vertex_label, dest_vertex_label, directed, _info)
            logger.info('Created new edge frame "%s"', new_frame_name)
        except:
            error = IaError(logger)
            raise error


