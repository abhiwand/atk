##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
"""
Frame
"""

import logging

logger = logging.getLogger(__name__)
from intelanalytics.core.api import get_api_decorator
api = get_api_decorator(logger)

from intelanalytics.core.userfunction import has_python_user_function_arg

from intelanalytics.core.column import BigColumn
from intelanalytics.core.errorhandle import IaError
from intelanalytics.core.namedobj import name_support
from intelanalytics.core.metaprog import CommandLoadable, doc_stubs_import

from intelanalytics.core.deprecate import deprecated, raise_deprecation_warning


def _get_backend():
    from intelanalytics.core.config import get_frame_backend
    return get_frame_backend()


# BaseFrame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsBaseFrame as CommandLoadableBaseFrame
    doc_stubs_import.success(logger, "DocStubsBaseFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsBaseFrame", e)
    CommandLoadableBaseFrame = CommandLoadable


class BaseFrame(CommandLoadableBaseFrame):
    _command_prefix = 'frame'

    def __init__(self):
        CommandLoadableBaseFrame.__init__(self)


# Frame
try:
    # boilerplate required here for static analysis to pick up the inheritance (the whole point of docstubs)
    from intelanalytics.core.docstubs import DocStubsFrame as CommandLoadableFrame
    doc_stubs_import.success(logger, "DocStubsFrame")
except Exception as e:
    doc_stubs_import.failure(logger, "DocStubsFrame", e)
    CommandLoadableFrame = BaseFrame

@api
@name_support('frame')
class Frame(CommandLoadableFrame):
    """
    Data handle.

    Class with information about a large 2D table of data.
    Has information needed to modify data and table structure.

    Parameters
    ----------
    source : CsvFile, BigFrame, or BigColumn(s)
        A source of initial data
    name : string
        The name of the newly created frame

    Returns
    -------
    BigFrame
        An object with access to the frame

    Notes
    -----
    If no name is provided for the BigFrame object, it will generate one.
    An automatically generated name will be the word "frame\_" followed by the uuid.uuid4().hex and
    if allowed, an "_" character then the name of the data source.
    For example, ``u'frame_e433e25751b6434bae13b6d1c8ab45c1_csv_file'``

    If a string in the csv file starts and ends with a double-quote (") character, the character is stripped
    off of the data before it is put into the field.
    Anything, including delimiters, between the double-quote characters is considered part of the string.
    If the first character after the delimiter is anything other than a double-quote character,
    the string will be composed of all the characters between the delimiters, including double-quotes.
    If the first field type is string, leading spaces on each row are considered part of the string.
    If the last field type is string, trailing spaces on each row are considered part of the string.

    Examples
    --------
    Create a new frame based upon the data described in the CsvFile object *my_csv_schema*.
    Name the frame "my_frame".
    Create a BigFrame *g* to access the data::

        g = ia.BigFrame(my_csv_schema, "my_frame")

    A BigFrame object has been created and *g* is its proxy.
    It brought in the data described by *my_csv_schema*.
    It is named *my_frame*.

    Create an empty frame; name it "your_frame"::

        h = ia.BigFrame(name='your_frame')

    A frame has been created and BigFrame *h* is its proxy.
    It has no data yet, but it does have the name *your_frame*.


    .. versionadded:: 0.8

    """
    # For other examples, see :ref:`example_frame.bigframe`.

    # TODO - Review Parameters, Examples

    _command_prefix = 'frame:'

    def __init__(self, source=None, name=None):
        try:
            self._error_frame_id = None
            self._id = 0
            self._ia_uri = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            CommandLoadableFrame.__init__(self)
            new_frame_name = self._backend.create(self, source, name)
            logger.info('Created new frame "%s"', new_frame_name)
        except:
            error = IaError(logger)


            raise error



    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name == '_backend':
            raise AttributeError('_backend')
        try:
            return super(Frame, self).__getattribute__(name)
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
                return [BigColumn(self, name, data_type_dict[name]) for name in column_name]
            return BigColumn(self, column_name, data_type_dict[column_name])
        except KeyError:
            raise error_type(error_msg % column_name)

    # We are not defining __setitem__.  Columns must be added explicitly

    # We are not defining __delitem__.  Columns must be deleted w/ drop_columns

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(Frame, self).__repr__() + " (Unable to collect metadata from server)"

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
        Iterator for BigFrame - frame iteration works on the columns, returns BigColumn objects
        (see BigFrame.__iter__)

        Parameters
        ----------
        frame : BigFrame
            A BigFrame object
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
                column = BigColumn(self.frame, self.schema[self.i][0], self.schema[self.i][1])
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
        Column names.

        The names of all the columns in the current BigFrame object.

        Returns
        -------
        list of string

        Examples
        --------
        Create a BigFrame object from the data described by schema *my_csv*; get the column names::

            my_frame = ia.BigFrame(source='my_csv')
            my_columns = my_frame.column_names
            print my_columns

        Now, assuming the schema *my_csv* described three columns *col1*, *col2*, and *col3*, our result is::

            ["col1", "col2", "col3"]

        .. versionadded:: 0.8

        """
        return [name for name, data_type in self._backend.get_schema(self)]

    @property
    @api
    def name(self):
        """
        Frame name.

        The name of the data frame.

        Returns
        -------
        str
            The name of the frame

        Examples
        --------
        Create a frame and give it the name "Flavor Recipes"; read the name back to check it::

            frame = ia.BigFrame(name="Flavor Recipes")
            given_name = frame.name
            print given_name

        The result given is::

            "Flavor Recipes"

        .. versionadded:: 0.8

        """
        return self._backend.get_name(self)

    @name.setter
    @api
    def name(self, value):
        """
        Set frame name.

        Assigns a name to a data frame.

        Examples
        --------
        Assign the name "movies" to the current frame::

            my_frame.name = "movies"

        .. versionadded:: 0.8

        """
        self._backend.rename_frame(self, value)

    @property
    @api
    def row_count(self):
        """
        Returns number of rows.

        Returns
        -------
        int
            The number of rows in the frame

        Examples
        --------
        Get the number of rows::

            my_frame.row_count

        The result given is::

            81734

        .. versionadded:: 0.8

        """
        return self._backend.get_row_count(self)

    @property
    @api
    def ia_uri(self):
        return self._backend.get_ia_uri(self)

    @property
    @api
    def schema(self):
        """
        Frame schema.

        The schema of the current frame is a list of column names and associated data types.
        It is retrieved as a list of tuples.
        Each tuple has the name and data type of one of the frame's columns.

        Returns
        -------
        list of tuples

        Examples
        --------
        Given that we have an existing data frame *my_data*, create a BigFrame, then show the frame schema::

            BF = ia.get_frame('my_data')
            print BF.schema

        The result is::

            [("col1", str), ("col1", numpy.int32)]

        .. versionadded:: 0.8

        """
        return self._backend.get_schema(self)

    @deprecated("Use classification_metrics().")
    def accuracy(self, label_column, pred_column):
        """
        Model accuracy.

        Computes the accuracy measure for a classification model.
        A column containing the correct labels for each instance and a column containing the predictions made by the classifier are specified.
        The accuracy of a classification model is the proportion of predictions that are correct.
        If we let :math:`T_{P}` denote the number of true positives, :math:`T_{N}` denote the number of true negatives, and :math:`K`
        denote the total number of classified instances, then the model accuracy is given by: :math:`\\frac{T_{P} + T_{N}}{K}`.

        This measure applies to binary and multi-class classifiers.

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance

        Returns
        -------
        float64
            the accuracy measure for the classifier

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            /-------------------------------------------------------/
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.accuracy('labels', 'predictions')

            0.75

        .. versionadded:: 0.8

        """
        try:
            cm = self.classification_metrics(label_column, pred_column, 1, 1)
            return cm.accuracy
        except:
            raise IaError(logger)

    @api
    @has_python_user_function_arg
    def add_columns(self, func, schema):
        """
        Add column.

        Adds one or more new columns to the frame by evaluating the given
        func on each row.

        Parameters
        ----------
        func : row function
            function which takes the values in the row and produces a value
            or collection of values for the new cell(s)
        schema : tuple or list of tuples
            the schema for the results of the functions, indicating the new
            column(s) to add.  Each tuple provides the column name and data
            type and is of the form (str, type).

        Notes
        -----
        The row function ('func') must return a value in the same format as specified by the schema.
        See :doc:`ds_apir`.

        Examples
        --------
        Given a BigFrame proxy *my_frame* identifying a data frame with two int32 columns *column1* and *column2*.
        Add a third column named "column3" as an int32 and fill it with the contents of *column1* and *column2*
        multiplied together::

            my_frame.add_columns(lambda row: row.column1*row.column2, ('column3', int32))

        The frame now has three columns, *column1*, *column2* and *column3*.
        The type of *column3* is an int32, and the value is the product of *column1* and *column2*.

        Add a string column *column4* that is empty::

            my_frame.add_columns(lambda row: '', ('column4', str))

        The BigFrame object *my_frame* now has four columns *column1*, *column2*, *column3*, and *column4*.
        The first three columns are int32 and the fourth column is string.  Column *column4* has an
        empty string ('') in every row.

        Multiple columns can be added at the same time.
        Add a column *a_times_b* and fill it with the contents of column *a* multiplied by the contents of column *b*.
        At the same time, add a column *a_plus_b* and fill it with the contents of column *a* plus
        the contents of column *b*::

            my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b], [("a_times_b",
                float32), ("a_plus_b", float32))

        Two new columns are created, "a_times_b" and "a_plus_b", with the appropriate contents.

        Given a frame of data and BigFrame *my_frame* points to it.
        In addition we have defined a function *func*.
        Run *func* on each row of the frame and put the result in a new integer column *calculated_a*::

            my_frame.add_columns( func, ("calculated_a", int))

        Now the frame has a column *calculated_a* which has been filled with the results of the function *func*.

        Functions must return their value in the same format as the column is defined.
        In most cases this is automatically the case, but sometimes it is less obvious.
        Given a function *function_b* which returns a value in a list, store the result in a new column *calculated_b*::

            my_frame.add_columns(function_b, ("calculated_b", float32))

        This would result in an error because function_b is returning a value as a single element list like [2.4], but our column is defined as
        a tuple.
        The column must be defined as a list::

            my_frame.add_columns(function_b, [("calculated_b", float32)])

        More information on row functions can be found at :doc:`ds_apir`



        .. versionadded:: 0.8

        """
        # For further examples, see :ref:`example_frame.add_columns`.
        self._backend.add_columns(self, func, schema)

    @api
    def append(self, data):
        """
        Add data.

        Adds more data (rows and/or columns) to the frame.

        Parameters
        ----------
            data : a BigFrame accessing the data being added

        Examples
        --------
        Given a frame with a single column *col_1* and a frame with two columns *col_1* and *col_2*.
        Column *col_1* means the same thing in both frames.
        BigFrame *my_frame* points to the first frame and *your_frame* points to the second.
        Add the contents of *your_frame* to *my_frame*::

            my_frame.append(your_frame)

        Now the first frame has two columns, *col_1* and *col_2*.
        Column *col_1* has the data from *col_1* in both original frames.
        Column *col_2* has None (undefined) in all of the rows in the original first frame, and has the value of the second frame column *col_2* in
        the rows matching the new data in *col_1*.

        Breaking it down differently, the original rows referred to by *my_frame* have a new column *col_2* and this new column is
        filled with non-defined data.
        The frame referred to by *your_frame* is then added to the bottom.


        .. versionadded:: 0.8

        """
        self._backend.append(self, data)

    @api
    def bin_column(self, column_name, num_bins, bin_type='equalwidth', bin_column_name='binned'):
        """
        Group by value.

        Two types of binning are provided: `equalwidth` and `equaldepth`.

        Equal width binning places column values into bins such that the values in each bin fall within the same
        interval and the interval width for each bin is equal.

        Equal depth binning attempts to place column values into bins such that each bin contains the same number of
        elements.  For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin number is determined by:

        .. math::

            ceiling \\left( n * \\frac {f(C)}{m} \\right)

        where :math:`f` is a tie-adjusted ranking function over values of :math:`C`.
        If there are multiples of the same value in :math:`C`, then their tie-adjusted rank is the average of their ordered rank values.

        The num_bins parameter is upper-bound on the number of bins since the data may justify fewer bins.
        With equal depth binning, for example, if the column to be binned has 10 elements with
        only 2 distinct values and num_bins > 2, then the number of actual bins will only be 2.
        This is due to a restriction that elements with an identical value must belong to the same bin.
        The type of the new column will be int32 and the bin numbers start at 1.

        Parameters
        ----------
        column_name : str
            The column whose values are to be binned
        num_bins : int
            The requested number of bins
        bin_type : str (optional)
            The binning algorithm to use
            ['equalwidth' | 'equaldepth']
        bin_column_name : str (optional)
            The name for the new binned column

        Returns
        -------
        BigFrame
            A BigFrame accessing a new frame, with a bin column appended to the original frame structure

        Examples
        --------
        For this example, we will use a frame with column *a* and a BigFrame *my_frame* accessing it::

            my_frame.inspect( n=11 )

              a:int32
            /---------/
              1
              1
              2
              3
              5
              8
             13
             21
             34
             55
             89

        Create a new frame with a column showing what bin the data is in.
        The data should be separated into a maximum of five bins and the bins should be *equalwidth*::

            binnedEW = my_frame.bin_column('a', 5, 'equalwidth', 'aEWBinned')
            binnedEW.inspect( n=11 )

              a:int32     aEWBinned:int32
            /-----------------------------/
              1                   1
              1                   1
              2                   1
              3                   1
              5                   1
              8                   1
             13                   1
             21                   2
             34                   2
             55                   4
             89                   5

        Create a new frame with a column showing what bin the data is in.
        The data should be separated into a maximum of five bins and the bins should be *equaldepth*::


            binnedED = my_frame.bin_column('a', 5, 'equaldepth', 'aEDBinned')
            binnedED.inspect( n=11 )

              a:int32     aEDBinned:int32
            /-----------------------------/
              1                   1
              1                   1
              2                   1
              3                   2
              5                   2
              8                   3
             13                   3
             21                   4
             34                   4
             55                   5
             89                   5

        .. versionadded:: 0.8

        """
        return self._backend.bin_column(self, column_name, num_bins, bin_type, bin_column_name)

    @deprecated("Use classification_metrics().")
    def confusion_matrix(self, label_column, pred_column, pos_label='1'):
        """
        Builds matrix.

        Outputs a :term:`confusion matrix` for a binary classifier

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance
        pos_label : int or str, (optional)
            the value to be interpreted as a positive instance

        Returns
        -------
        Formatted confusion matrix

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            /-------------------------------------------------------/
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            print(frame.confusion_matrix('labels', 'predictions'))

                            Predicted
                           _pos_ _neg__
             Actual   pos |  1     1
                      neg |  0     2

        .. versionadded:: 0.8

        """
        try:
            cm = self.classification_metrics(label_column, pred_column, pos_label, 1)
            return cm.confusion_matrix
        except:
            raise IaError(logger)

    def copy(self, columns=None):
        """
        Copy frame.

        Copy frame or certain frame columns entirely.

        Parameters
        ----------
        columns : str, list, or dict (optional)
            If not None, the copy will only include the columns specified.  If a dictionary is used, the string pairs
            represent a column renaming, {source_column_name: destination_column_name}

        Returns
        -------
        BigFrame
            A new frame object which is a copy of the currently active BigFrame

        Examples
        --------
        Build a BigFrame from a csv file with 5 million rows of data; call the frame "cust"::

            my_frame = ia.BigFrame(source="my_data.csv")
            my_frame.name("cust")

        At this point we have one frame of data, which is now called "cust".
        We will say it has columns *id*, *name*, *hair*, and *shoe*.
        Let's copy it to a new frame::

            your_frame = my_frame.copy()

        Now we have two frames of data, each with 5 million rows. Checking the names::

            print my_frame.name()
            print your_frame.name()

        Gives the results::

            "cust"
            "frame_75401b7435d7132f5470ba35..."

        Now, let's copy *some* of the columns from the original frame::

            our_frame = my_frame.copy(['id', 'hair'])

        Our new frame now has two columns, *id* and *hair*, and has 5 million rows.
        Let's try that again, but this time change the name of the *hair* column to *color*::

            last_frame = my_frame.copy(('id': 'id', 'hair': 'color'))

        .. versionchanged:: 0.8.5

        """
        if columns is None:
            column_names = self.column_names  # all columns
            new_names = None
        elif isinstance(columns, dict):
            column_names = columns.keys()
            new_names = columns.values()
        elif isinstance(columns, basestring):
            column_names = [columns]
            new_names = None
        else:
            raise ValueError("bad argument type %s passed to copy().  Must be string or dict" % type(columns))
        copied_frame = Frame()
        self._backend.project_columns(self, copied_frame, column_names, new_names)
        return copied_frame

    @api
    @has_python_user_function_arg
    def drop_rows(self, predicate):
        """
        Drop rows.

        Remove all rows from the frame which satisfy the predicate.

        Parameters
        ----------
        predicate : function
            function or :term:`lambda` which takes a row argument and evaluates to a boolean value

        Examples
        --------
        For this example, my_frame is a BigFrame object accessing a frame with lots of data for the attributes of *lions*, *tigers*, and *ligers*.
        Get rid of the *lions* and *tigers*::

            my_frame.drop_rows(lambda row: row.animal_type == "lion" or
                row.animal_type == "tiger")

        Now the frame only has information about *ligers*.

        More information on row functions can be found at :doc:`ds_apir`


        .. versionchanged:: 0.8.5

        """
        # For further examples, see :ref:`example_frame.drop_rows`
        self._backend.drop(self, predicate)

    @api
    def drop_duplicates(self, columns=None):
        """
        Remove duplicates.

        Remove duplicate rows, keeping only one row per uniqueness criteria match

        Parameters
        ----------
        columns : str OR list of str
            column name(s) to identify duplicates. If empty, will remove duplicates that have whole row data identical.

        Examples
        --------
        Remove any rows that have the same data in column *b* as a previously checked row::

            my_frame.drop_duplicates("b")

        The result is a frame with unique values in column *b*.

        Remove any rows that have the same data in columns *a* and *b* as a previously checked row::

            my_frame.drop_duplicates(["a", "b"])

        The result is a frame with unique values for the combination of columns *a* and *b*.

        Remove any rows that have the whole row identical::

            my_frame.drop_duplicates()

        The result is a frame where something is different in every row from every other row.
        Each row is unique.


        .. versionadded:: 0.8

        """
        # For further examples, see :ref:`example_frame.drop_duplicates`
        self._backend.drop_duplicates(self, columns)

    @api
    def ecdf(self, sample_col):
        """
        Empirical Cumulative Distribution.

        Generates the :term:`empirical cumulative distribution` for the input column.

        Parameters
        ----------
        sample_col : str
            the name of the column containing sample

        Returns
        -------
        list
            list of tuples containing each distinct value in the sample and its corresponding ecdf value

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32
            /---------------------/
              red               1
              blue              3
              blue              1
              green             0

            result = frame.ecdf('b')
            result.inspect()

              b:int32   b_ECDF:float64
            /--------------------------/
              1                    0.2
              2                    0.5
              3                    0.8
              4                    1.0


        .. versionadded:: 0.8

        """
        return self._backend.ecdf(self, sample_col)

    @api
    @has_python_user_function_arg
    def filter(self, predicate):
        """
        Select data.

        Select all rows which satisfy a predicate.

        Parameters
        ----------
        predicate : function
            function definition or lambda which takes a row argument and evaluates to a boolean value

        Examples
        --------
        For this example, my_frame is a BigFrame object with lots of data for the attributes of *lizards*,
        *frogs*, and *snakes*.
        Get rid of everything, except information about *lizards* and *frogs*::

            def my_filter(row):
                return row['animal_type'] == 'lizard' or row['animal_type'] == "frog"

            my_frame.filter(my_filter)

        The frame now only has data about lizards and frogs

        More information on row functions can be found at :doc:`ds_apir`



        .. versionadded:: 0.8

        """
        # For further examples, see :ref:`example_frame.filter`
        self._backend.filter(self, predicate)

    @api
    def flatten_column(self, column_name):
        """
        Spread out data.

        Search through the currently active BigFrame for multiple items in a single specified column.
        When it finds multiple values in the column, it replicates the row and separates the multiple items across the existing and new rows.
        Multiple items is defined in this case as being things separated by commas.

        Parameters
        ----------
        column_name : str
            The column to be flattened

        Returns
        -------
        BigFrame
            A BigFrame object proxy for the new flattened frame

        Examples
        --------
        Given that I have a frame accessed by BigFrame *my_frame* and the frame has two columns *a* and *b*.
        The "original_data"::

            1-"solo,mono,single"
            2-"duo,double"

        I run my commands to bring the data in where I can work on it::

            my_csv = CsvFile("original_data.csv", schema=[('a', int32), ('b', string)],
                delimiter='-')
            # The above command has been split for enhanced readability in some medias.
            my_frame = BigFrame(source=my_csv)

        I look at it and see::

            my_frame.inspect()

              a:int32   b:string
            /------------------------------/
                1       solo, mono, single
                2       duo, double

        Now, I want to spread out those sub-strings in column *b*::

            your_frame = my_frame.flatten_column('b')

        Now I check again and my result is::

            your_frame.inspect()

              a:int32   b:str
            /------------------/
                1       solo
                1       mono
                1       single
                2       duo
                2       double


        .. versionadded:: 0.8

        """
        return self._backend.flatten_column(self, column_name)

    @deprecated("Use classification_metrics().")
    def f_measure(self, label_column, pred_column, pos_label='1', beta=1):
        """
        Model :math:`F_{\\beta}` measure.

        Computes the :math:`F_{\\beta}` measure for a classification model.
        A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
        The :math:`F_{\\beta}` measure of a binary classification model is the harmonic mean of precision and recall.
        If we let:

        * beta :math:`\\equiv \\beta`,
        * :math:`T_{P}` denote the number of true positives,
        * :math:`F_{P}` denote the number of false positives, and
        * :math:`F_{N}` denote the number of false negatives,

        then:

        .. math::
            F_{\\beta} = \\left(1 + \\beta ^ 2\\right) * \\frac{\\frac{T_{P}}{T_{P} + F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * \\
            \\left(\\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}}\\right)}

        For multi-class classification, the :math:`F_{\\beta}` measure is computed as the weighted average of the :math:`F_{\\beta}` measure
        for each label, where the weight is the number of instance with each label in the labeled column.  The
        determination of binary vs. multi-class is automatically inferred from the data.

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance
        pos_label : int or str, (optional)
            the value to be interpreted as a positive instance (only for binary, ignored for multi-class)
        beta : float, (optional)
            beta value to use for :math:`F_{\\beta}` measure (default F1 measure is computed); must be greater than zero

        Returns
        -------
        float64
            the :math:`F_{\\beta}` measure for the classifier

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            /-------------------------------------------------------/
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.f_measure('labels', 'predictions')

            0.66666666666666663

            frame.f_measure('labels', 'predictions', beta=2)

            0.55555555555555558

            frame.f_measure('labels', 'predictions', pos_label=0)

            0.80000000000000004

        .. versionadded:: 0.8

        """
        try:
            cm = self.classification_metrics(label_column, pred_column, pos_label, beta)
            return cm.f_measure
        except:
            raise IaError(logger)

    @api
    def get_error_frame(self):
        """
        Frame with errors.

        When a frame is loaded, parse errors go into a separate data frame so they can be
        inspected.  No error frame is created if there were no parse errors.

        Returns
        -------
        BigFrame
            A new object accessing a frame that contains the parse errors of the currently active BigFrame
            or None if no error frame exists
        """
        return self._backend.get_frame_by_id(self._error_frame_id)

    @api
    def group_by(self, group_by_columns, *aggregation_arguments):
        """
        Create summarized frame.

        Creates a new frame and returns a BigFrame object to access it.
        Takes a column or group of columns, finds the unique combination of values,
        and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        group_by_columns : str
            column name or list of column names
        aggregation_arguments
            aggregation function based on entire row, and/or
            dictionaries (one or more) of { column name string : aggregation function(s) }

        Returns
        -------
        BigFrame
            A new object accessing a new aggregated frame

        Notes
        -----
        *   The column names created by aggregation functions in the new frame are the original column
            name appended with the '_' character and the aggregation function.
            For example, if the original field is 'a' and the function is 'avg',
            the resultant column is named 'a_avg'.
        *   An aggregation argument of 'count' results in a column named 'count'.

        Examples
        --------
        For setup, we will use a BigFrame *my_frame* accessing a frame with a column *a*::

            my_frame.inspect()

             a:str
           /-------/
             cat
             apple
             bat
             cat
             bat
             cat

        Create a new frame, combining similar values of column *a*, and count how many of each
        value is in the original frame::

            new_frame = my_frame.group_by('a', agg.count)
            new_frame.inspect()

             a:str       count:int
           /-----------------------/
             cat             3
             apple           1
             bat             2

        In this example, 'my_frame' is accessing a frame with three columns, *a*, *b*, and *c*::

            my_frame.inspect()

             a:int   b:str       c:float
           /-----------------------------/
             1       alpha     3.0
             1       bravo     5.0
             1       alpha     5.0
             2       bravo     8.0
             2       bravo    12.0

        Create a new frame from this data, grouping the rows by unique combinations of column *a* and *b*;
        average the value in *c* for each group::

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
             ape     1     4.0       9
             ape     1     8.0       8
             big     1     5.0       7
             big     1     6.0       6
             big     1     8.0       5

        Create a new frame from this data, grouping the rows by unique combinations of column *a* and *c*;
        count each group; for column *d* calculate the average, sum and minimum value; for column *e*,
        save the maximum value::

            new_frame = my_frame.group_by(['a', 'c'], agg.count, {'d':
                [agg.avg, agg.sum, agg.min], 'e': agg.max})

             a:str   c:int   count:int  d_avg:float  d_sum:float   d_min:float   e_max:int
           /-------------------------------------------------------------------------------/
             ape     1           2        6.0         12.0             4.0           9
             big     1           3        6.333333    19.0             5.0           7


        .. versionchanged:: 0.8.5

        """
        # For further examples, see :ref:`example_frame.group_by`.
        return self._backend.group_by(self, group_by_columns, aggregation_arguments)


    @api
    def inspect(self, n=10, offset=0, columns=None):
        """
        Print data.

        Print the frame data in readable format.

        Parameters
        ----------
        n : int
            The number of rows to print
        offset : int
            The number of rows to skip before printing
        columns : String or iterable of string
            Specify the columns to be included in the result. By default all the columns
            are to be included


        Returns
        -------
        data
            Formatted for ease of human inspection

        Examples
        --------
        For this example, let's say we have a frame of data and a BigFrame to access it. Let's look at the first 10 rows of data::

            print my_frame.inspect()

            column defs ->  animal:str  name:str    age:int     weight:float
                          /--------------------------------------------------/
            frame data ->   lion        George        8            542.5
                            lion        Ursula        6            495.0
                            ape         Ape          41            400.0
                            elephant    Shep          5           8630.0



        .. versionadded:: 0.8

        """
        # For another example, see :ref:`example_frame.inspect`.
        return self._backend.inspect(self, n, offset, columns)

    @api
    def join(self, right, left_on, right_on=None, how='inner'):
        """
        Combine frames.

        Create a new frame from a SQL JOIN operation with another frame.
        The frame on the 'left' is the currently active frame.
        The frame on the 'right' is another frame.
        This function takes a column in the left frame and matches it's values with a column in the right frame.
        Using the default 'how' option ['inner'] will only allow data in the resultant frame if both the left and right
        frames have the same value in the matching column.
        Using the 'left' 'how' option will allow any data in the resultant frame if it exists in the left frame, but
        will allow any data from the right frame if it has a value in it's column which matches the value in the left frame column.
        Using the 'right' option works similarly, except it keeps all the data from the right frame and only the
        data from the left frame when it matches.

        Parameters
        ----------
        right : BigFrame
            Another frame to join with
        left_on : str
            Name of the column in the left frame used to match up the two frames.
        right_on : str (optional)
            Name of the column in the right frame used to match up the two frames.
            If not provided, then the column name used must be the same in both frames.
        how : str (optional)
            ['left' | 'right' | 'inner']

        Returns
        -------
        BigFrame
            A new object accessing a new joined frame

        Notes
        -----
        When a column is named the same in both frames, it will result in two columns in the new frame.
        The column from the *left* frame will be copied and the column name will have the string "_L" added to it.
        The same thing will happen with the column from the *right* frame, except its name has the string "_R" appended.

        It is recommended that you rename the columns to meaningful terms prior to using the ``join`` method.
        Keep in mind that unicode characters should not be used in column names.

        Examples
        --------
        For this example, we will use a BigFrame *my_frame* accessing a frame with columns *a*, *b*, *c*, and a BigFrame *your_frame* accessing
        a frame with columns *a*, *d*, *e*.
        Join the two frames keeping only those rows having the same value in column *a*::

            my_frame = BigFrame(schema1)
            your_frame = BigFrame(schema2)
            joined_frame = my_frame.join(your_frame, 'a')

        Now, joined_frame is a BigFrame accessing a frame with the columns *a_L*, *a_R*, *b*, *c*, *d*, and *e*.
        The data in the new frame will be from the rows where column 'a' was the same in both frames.

        Now, using a single BigFrame *my_frame* accessing a frame with the columns *b* and *book*.
        Build a new frame, but remove any rows where the values in *b* and *book* do not match::

            joined_frame = your_frame.join(your_frame, left_on='b', right_on='book',
                how='inner')

        We end up with a new BigFrame *joined_frame* accessing a new frame with all the original columns, but only those rows where the data in the
        original frame in column *b* matched the data in column *book*.


        .. versionadded:: 0.8

        """
        # For further examples, see :ref:`example_frame.join`.
        return self._backend.join(self, right, left_on, right_on, how)

    @deprecated("Use classification_metrics().")
    def precision(self, label_column, pred_column, pos_label='1'):
        """
        Model precision.

        Computes the precision measure for a classification model.
        A column containing the correct labels for each instance and a column containing the predictions made by the
        model are specified.  The precision of a binary classification model is the proportion of predicted positive
        instances that are correct.  If we let :math:`T_{P}` denote the number of true positives and :math:`F_{P}` denote the number of false
        positives, then the model precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.

        For multi-class classification, the precision measure is computed as the weighted average of the precision
        for each label, where the weight is the number of instances with each label in the labelled column.  The
        determination of binary vs. multi-class is automatically inferred from the data.

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance
        pos_label : int or str, (optional, default=1)
            the value to be interpreted as a positive instance (only for binary, ignored for multi-class)

        Returns
        -------
        float64
            the precision measure for the classifier

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            /-------------------------------------------------------/
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.precision('labels', 'predictions')

            1.0

            frame.precision('labels', 'predictions', 0)

            0.66666666666666663

        .. versionadded:: 0.8

        """
        try:
            cm = self.classification_metrics(label_column, pred_column, pos_label, 1)
            return cm.precision
        except:
            raise IaError(logger)

    @deprecated("Use classification_metrics().")
    def recall(self, label_column, pred_column, pos_label='1'):
        """
        Model measure.

        Computes the recall measure for a classification model.
        A column containing the correct labels for each instance and a column containing the predictions made by the model are specified.
        The recall of a binary classification model is the proportion of positive instances that are correctly identified.
        If we let :math:`T_{P}` denote the number of true positives and :math:`F_{N}` denote the number of false
        negatives, then the model recall is given by: :math:`\\frac {T_{P}} {T_{P} + F_{N}}`.

        For multi-class classification, the recall measure is computed as the weighted average of the recall
        for each label, where the weight is the number of instance with each label in the labeled column.  The
        determination of binary vs. multi-class is automatically inferred from the data.

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance
        pos_label : int or str, (optional)
            the value to be interpreted as a positive instance (only for binary, ignored for multi-class)

        Returns
        -------
        float64
            the recall measure for the classifier

        Examples
        --------
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            /-------------------------------------------------------/
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.recall('labels', 'predictions')

            0.5

            frame.recall('labels', 'predictions', 0)

            1.0

        .. versionadded:: 0.8

        """
        try:
            cm = self.classification_metrics(label_column, pred_column, pos_label, 1)
            return cm.recall
        except:
            raise IaError(logger)

    @api
    def rename_columns(self, column_names):
        """
        Rename column.

        Renames columns in a frame.

        Parameters
        ----------
        column_names : dictionary of str pairs
            The name pair (existing name, new name)

        Notes
        -----
        Unicode characters should not be used in column names.

        Examples
        --------
        Start with a frame with columns *Wrong* and *Wong*.
        Rename the columns to *Right* and *Wite*::

            my_frame.rename_columns({"Wrong": "Right, "Wong": "Wite"})

        Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.


        .. versionchanged:: 0.8.5

        """
        # For further examples, see :ref:`example_frame.rename_columns`.
        self._backend.rename_columns(self, column_names)

    @api
    def take(self, n, offset=0, columns=None):
        """
        Get data subset.

        Take a subset of the currently active BigFrame.

        Parameters
        ----------
        n : int
            The number of rows to copy from the currently active BigFrame
        offset : int
            The number of rows to skip before copying
        columns : String or iterable of string
            Specify the columns to be included in the result.
            By default all the columns are to be included.

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain number of rows, the rows obtained may be
        different every time the command is executed, even if the parameters do not change.

        Returns
        -------
        BigFrame
            A new frame object accessing a new frame containing copies of a subset of the original frame

        Examples
        --------
        BigFrame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 5000 rows::

            your_frame = my_frame.take( 5000 )

        We now have a separate frame accessed by a BigFrame *your_frame* with a copy of the first 5000 rows of the original frame.

        If we use the function with an offset like::

            your_frame = my_frame.take( 5000, 1000 )

        We end up with a new frame accessed by the BigFrame *your_frame* again, but this time it has a copy of
        rows 1001 to 5000 of the original frame.

        .. versionadded:: 0.8

        """
        result = self._backend.take(self, n, offset, columns)
        return result.data


class BigFrame(Frame):
    def __init__(self, *args, **kwargs):
        raise_deprecation_warning('BigFrame', 'Use Frame()')
        super(BigFrame, self).__init__(*args, **kwargs)
