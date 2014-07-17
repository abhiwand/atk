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

from intelanalytics.core.orddict import OrderedDict
import json

import logging

import uuid, sys
logger = logging.getLogger(__name__)

from intelanalytics.core.iatypes import supported_types
from intelanalytics.core.aggregation import *
from intelanalytics.core.errorhandle import IaError
from intelanalytics.core.command import CommandSupport, doc_stub


def _get_backend():
    from intelanalytics.core.config import get_frame_backend
    return get_frame_backend()


def get_frame_names():
    """
    Summary
    -------
    BigFrame names

    .. versionadded:: 0.8

    Extended Summary
    ----------------
    Gets the names of BigFrame objects available for retrieval

    Raises
    ------
    IaError

    Returns
    -------
    list of strings
        Names of the all BigFrame objects

    Examples
    --------
    Create two BigFrame objects and get their names::

        frame1 = BigFrame(csv_schema_1, "BigFrame1")
        frame2 = BigFrame(csv_schema_2, "BigFrame2")
        frame_names = get_frame_names()
        print frame_names

    Result would be::

        ["BigFrame1", "BigFrame2"]
    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame_names()
    except:
        raise IaError(logger)


def get_frame(name):
    """
    Summary
    -------
    Get BigFrame

    .. versionadded:: 0.8

    Extended Summary
    ----------------
    Retrieves a BigFrame class object to allow access to the data frame

    Parameters
    ----------
    name : string
        String containing the name of the BigFrame object

    Raises
    ------
    IaError

    Returns
    -------
    class
        BigFrame class object

    Examples
    --------
    Create a frame *BF1*; create a BigFrame proxy for it; check that the new BigFrame is equivalent to the original::

        BF1a = BigFrame(my_csv, "BF1")
        BF1b = get_frame("BF1")
        print BF1a == BF1b

    Result would be::

        True

    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame(name)
    except:
        raise IaError(logger)


def delete_frame(name):
    """
    Summary
    -------
    Erases data

    .. versionadded:: 0.8

    Extended Summary
    ----------------
    Deletes the frame from backing store

    Parameters
    ----------
    name : string
        The name of the BigFrame object to delete.

    Raises
    ------
    IaError

    Returns
    -------
    string
        The name of the deleted frame

    Examples
    --------
    Create a new frame; delete it; print what gets returned from the function::

        my_frame = BigFrame(my_csv, 'BF1')
        deleted_frame = delete_frame('BF1')
        print deleted_frame

    The result would be::

        BF1
    """
    # TODO - Review examples and parameter
    try:
        return _get_backend().delete_frame(name)
    except:
        raise IaError(logger)


class BigFrame(CommandSupport):
    """
    Summary
    -------
    Data handle

    .. versionadded:: 0.8

    Extended Summary
    ----------------
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
    An automatically generated name will be the word "frame_" followed by the uuid.uuid4().hex and
    if allowed, an "_" character then the name of the data source.

    Examples
    --------
    Create a BigFrame object; name it "BF1"::

        g = BigFrame(my_csv_schema, "BF1")

    A BigFrame object has been created and *g* is its proxy. It brought in the data described by *my_csv_schema*. It is named *BF1*.

    Create an empty frame; name it "BF2"::

        h = BigFrame(name='BF2')

    A BigFrame object has been created and *h* is its proxy. It has no data yet, but it does have the name *BF2*.

    For other examples, see :ref:`example_frame.bigframe`.

    """
    # TODO - Review Parameters, Examples

    def __init__(self, source=None, name=None):
        try:
            self._columns = OrderedDict()  # self._columns must be the first attribute to be assigned (see __setattr__)
            self._id = 0
            self._uri = ""
            self._name = ""
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            self._backend.create(self, source, name)
            CommandSupport.__init__(self)
            logger.info('Created new frame "%s"', self._name)
        except:
            raise IaError(logger)


    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        try:
            if name != "_columns" and name in self._columns:
                return self[name]
            return super(BigFrame, self).__getattribute__(name)
        except:
            raise IaError(logger)


    # We are not defining __setattr__.  Columns must be added explicitly

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Slicing not supported")
            if isinstance(key, list):
                return [self._columns[k] for k in key]
            return self._columns[key]
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    # We are not defining __setitem__.  Columns must be added explicitly

    # We are not defining __delitem__.  Columns must be deleted w/ remove_columns

    def __repr__(self):
        try:
            return json.dumps({'uri': self.uri,
                               'name': self.name,
                               'schema': self._schema_as_json_obj()}, indent=2, separators=(', ', ': '))
        except:
            raise IaError(logger)

    def _schema_as_json_obj(self):
        return [(col.name, supported_types.get_type_string(col.data_type)) for col in self._columns.values()]

    def __len__(self):
        return len(self._columns)

    def __contains__(self, key):
        return self._columns.__contains__(key)

    def _validate_key(self, key):
        if key in dir(self) and key not in self._columns:
            raise KeyError("Invalid column name '%s'" % key)

    class _FrameIter(object):
        """
        (Private)
        Iterator for BigFrame - frame iteration works on the columns
        (see BigFrame.__iter__)

        Parameters
        ----------
        frame : BigFrame
            A BigFrame object
        """

        def __init__(self, frame):
            self.frame = frame
            self.i = 0

        def __iter__(self):
            return self

        def next(self):
            if self.i < len(self.frame):
                column = self.frame._columns.values()[self.i]
                self.i += 1
                return column
            raise StopIteration

    def __iter__(self):
            return BigFrame._FrameIter(self)

    def __eq__(self, other):
            if not isinstance(other, BigFrame):
                return False
            return self._id == other._id

    def __hash__(self):
        return hash(self._id)

    @property
    def column_names(self):
        """
        Summary
        -------
        Column names

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        The names of all the columns in the current BigFrame object.

        Returns
        -------
        list of string

        Examples
        --------
        Create a BigFrame object from the data described by schema *my_csv*; get the column names::

            my_frame = BigFrame(source='my_csv')
            my_columns = my_frame.column_names()
            print my_columns

        Now, assuming the schema *my_csv* described three columns *col1*, *col2*, and *col3*, our result is::

            ["col1", "col2", "col3"]

        """
        return self._columns.keys()

    @property
    def name(self):
        """
        Summary
        -------
        Frame name

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        The name of the data frame.

        Returns
        -------
        name : str
            The name of the frame

        Examples
        --------
        Create a frame and give it the name "Flavor Recipes"; read the name back to check it::

            frame = BigFrame(name="Flavor Recipes")
            given_name = frame.name()
            print given_name

        The result given is::

            "Flavor Recipes"

        """
        return self._name

    @name.setter
    def name(self, value):
        """
        Summary
        -------
        Set frame name

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Assigns a name to a data frame.

        Examples
        --------
        Assign the name "movies" to the current frame::

            my_frame.name("movies")

        """
        try:
            self._backend.rename_frame(self, value)
            self._name = value  # TODO - update from backend
        except:
            raise IaError(logger)

    @property
    def schema(self):
        """
        Summary
        -------
        BigFrame schema

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        The schema of the current BigFrame object.

        Returns
        -------
        schema : list of tuples
            The schema of the BigFrame, which is a list of tuples which
            represents and the name and data type of frame's columns

        Examples
        --------
        Given that we have an existing data frame *my_data*, get the BigFrame proxy then the frame schema::

            BF = get_frame('my_data')
            my_schema = BF.schema()
            print my_schema

        The result is::

            [("col1", str), ("col1", numpy.int32)]

        """
        return [(col.name, col.data_type) for col in self._columns.values()]

    @property
    def uri(self):
        """
        Summary
        -------
        Uniform Resource Identifier

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        The uniform resource identifier of the data frame.

        Returns
        -------
        uri : str
            The value of the uri

        Examples
        --------
        Given that we have an existing data frame *my_data*, get the BigFrame proxy then the frame uri::

            BF = get_frame('my_data')
            my_uri = BF.uri()
            print my_uri

        The result is::

            TBD

        """
        return self._uri

    def _as_json_obj(self):
        return self._backend._as_json_obj(self)

    def accuracy(self, label_column, pred_column):
        """
        Summary
        -------
        Model accuracy

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Computes the accuracy measure for a classification model
        A column containing the correct labels for each instance and a column containing the predictions made by the classifier are specified.
        The accuracy of a classification model is the proportion of predictions that are correct.
        If we let :math:`TP` denote the number of true positives, :math:`TN` denote the number of true negatives, and :math:`K`
        denote the total number of classified instances, then the model accuracy is given by: :math:`(TP + TN) / K`.

        This measure applies to binary and multi-class classifiers.

        Parameters
        ----------
        label_column : str
            the name of the column containing the correct label for each instance
        pred_column : str
            the name of the column containing the predicted label for each instance

        Returns
        ----------
        float64
            the accuracy measure for the classifier

        Examples
        ----------
        >>> acc = frame.accuracy('labels', 'predictions')

        """
        return self._backend.classification_metric(self, 'accuracy', label_column, pred_column, '1', 1)


    def add_columns(self, func, schema):
        """
        Summary
        -------
        Add column
        
        .. versionadded:: 0.8

        Extended Summary
        ----------------
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
        See :doc:ds_apir.
        
        Examples
        --------
        Given a BigFrame proxy *my_frame* identifying a data frame with two int32 columns *column1* and *column2*.
        Add a third column named "column3" as an int32 and fill it with the contents of *column1* and *column2* multiplied together::

            my_frame.add_columns(lambda row: row.column1*row.column2, ('column3', int32))

        The frame now has three columns, *column1*, *column2* and *column3*.
        The type of *column3* is an int32, and the value is the product of *column1* and *column2*.

        Add a string column *column4* that is empty::

            my_frame.add_columns(lambda row: '', ('column4', str))

        The BigFrame object *my_frame* now has four columns *column1*, *column2*, *column3*, and *column4*.
        The first three columns are int32 and the fourth column is string.  Column *column4* has an empty string ('') in every row.

        Multiple columns can be added at the same time.
        Add a column *a_times_b* and fill it with the contents of column *a* multiplied by the contents of column *b*.
        At the same time, add a column *a_plus_b* and fill it with the contents of column *a* plus the contents of column *b*::

            my_frame.add_columns(lambda row: [row.a * row.b, row.a + row.b], [("a_times_b", float32), ("a_plus_b", float32))

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

            my_frame.add_columns(function_b, (["calculated_b"], float32))
        
        For further examples, see :ref:`example_frame.add_columns`.

        """
        try:
            self._backend.add_columns(self, func, schema)
        except:
            raise IaError(logger)

    def append(self, data):
        """
        Summary
        -------
        Add data

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Adds more data (rows and/or columns) to the BigFrame object.

        Parameters
        ----------
            data : a BigFrame describing the data being added

        Examples
        --------
        Given a frame with a single column *col_1* and a frame with two columns *col_1* and *col_2*.
        Column *col_1* means the same thing in both frames.
        BigFrame *f_1* points to the first frame and *f_2* points to the second.
        Add the contents of the *f_2* to *f_1*::

            f_1.append(f_2)

        Now the first frame has two columns, *col_1* and *col_2*.
        Column *col_1* has the data from *col_1* in both frames.
        Column *col_2* has None (undefined) in all of the rows in the original first frame, and has the value of the second frame column *col_2* in
        the rows matching the new data in *col_1*.

        Breaking it down differently, the original rows refered to by *f_1* have a new column *col_2* and this new column is filled with non-defined
        data.
        The frame referred to by *f_2* is then added to the bottom.

        For further example, see :ref:`Data flow <example_frame.append>`.
        """
        # TODO - Review examples
        try:
            self._backend.append(self, data)
        except:
            raise IaError(logger)

    @doc_stub
    def assign_sample(self, name):
        """
        Summary
        -------
        Assign classes to rows

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Randomly assign classes to rows given a vector of percentages:
        The table receives an additional column that contains a random label generated by the probability distribution
        function specified by a list of floating point values. The labels are non-negative integers drawn from the range
        [ 0,  len(split_percentages) - 1].

        Parameters
        ----------
        sample_percentages : list of floating point values
            Entries are non-negative and sum to 1.
            If the *i*'th entry of the  list is *p*, then then each row receives label *i* with independent probability *p*.
        sample_labels : str (optional)
            Names to be used for the split classes.
            Defaults "TR", "TE", "VA" when there are three numbers given in split_percentages, defaults to Sample#0, Sample#1, ... otherwise.
        output_column : str (optional)
            Name of the new column which holds the labels generated by the Optional. Defaults to "sample_bin".
            column name OR list of column names to be removed from the frame
        random_seed : int (optional)
            Random seed used to generate the labels. Defaults to 0.

        Examples
        --------
        For this example, my_frame is a BigFrame object accessing a frame with data.
        Append a new column *sample_bin* to the frame;
        Assign the value in the new column to "train", "test", or "validate"::

            my_frame.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])

        Now the frame accessed by BigFrame *my_frame* has a new column named "sample_bin" and each row contains one of the values "train",
        "test", or "validate".
        Values in the other columns are unaffected.

        """
        pass

    def bin_column(self, column_name, num_bins, bin_type='equalwidth', bin_column_name='binned'):
        """
        Summary
        -------
        Column values into bins

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Two types of binning are provided: `equalwidth` and `equaldepth`.

        Equal width binning places column values into bins such that the values in each bin fall within the same
        interval and the interval width for each bin is equal.

        Equal depth binning attempts to place column values into bins such that each bin contains the same number of
        elements.  For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin number is determined by:

        .. math::

            ceiling(n * f(C) / m)

        where :math:`f` is a tie-adjusted ranking function over values of :math:`C`.
        If there are multiple of the same value in :math:`C`, then their tie-adjusted rank is the average of their ordered rank values.

        The num_bins parameter is upper-bound on the number of bins since the data may justify fewer bins.
        With :term:`equal depth binning`, for example, if the column to be binned has 10 elements with
        only 2 distinct values and num_bins > 2, then the number of actual bins will only be 2.
        This is due to a restriction that elements with an identical value must belong to the same bin.

        Parameters
        ----------
        column_name : str
            The column whose values are to be binned
        num_bins : int
            The requested number of bins
        bin_type : str (optional)
            The binning algorithm to use
            [':term:`equalwidth`' | ':term:`equaldepth`']
        bin_column_name : str, (optional)
            The name for the new binned column

        Returns
        -------
        BigFrame
            A BigFrame accessing a new frame with binned column appended to original frame

        Examples
        --------
        ::

            binnedEW = frame.bin_column('a', 5, 'equalwidth', 'aEWBinned')
            binnedED = frame.bin_column('a', 5, 'equaldepth', 'aEDBinned')

        """
        return self._backend.bin_column(self, column_name, num_bins, bin_type, bin_column_name)

    def confusion_matrix(self, label_column, pred_column, pos_label=1):
        """
        Summary
        -------
        Builds matrix

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Outputs a confusion matrix for a binary classifier

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
        ::

            print(frame.confusion_matrix('labels', 'predictions'))

        The resultant output is::

                             Predicted
                           __pos__ _neg___
             Actual   pos | 1     | 4
                      neg | 3     | 2

        """

        return self._backend.confusion_matrix(self, label_column, pred_column, pos_label)

    def copy(self):
        """
        Summary
        -------
        Copy frame

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Creates a full copy of the current frame.

        Raises
        ------
        IaError

        Returns
        -------
        frame : BigFrame
            A new frame object which is a copy of the currently active BigFrame

        Examples
        --------
        Build a BigFrame from a csv file with 5 million rows of data; call the frame "cust".
        Copy the BigFrame; name the frame "serv".
        Copy the frame; check its name::

            BF1 = BigFrame(source="my_data.csv")
            BF1.name("cust")

        At this point we have one frame of data, which is now called "cust".
        ::

            BF2 = BF1

        We still have one frame of data called "cust".
        ::

            BF2.name("serv")

        We now have one frame of data called "serv". We probably wanted a second frame named "serv", with the first named "cust".
        ::

            BF2 = BF2.copy()

        Now we have two frames of data, each with 5 million rows. Checking the names::

            print BF1.name()
            print BF2.name()

        Gives the results::

            "cust"
            (a very long computer-generated name)


        """
        try:
            copied_frame = BigFrame()
            self._backend.project_columns(self, copied_frame, self.column_names)
            return copied_frame
        except:
            raise IaError(logger)

    def count(self):
        """
        Summary
        -------
        Row count

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Count the number of rows that exist in this object.

        Raises
        ------
        IaError

        Returns
        -------
        int32
            The number of rows in the frame

        Examples
        --------
        Build a frame from a huge CSV file; report the number of rows of data::

            my_frame = BigFrame(source="my_csv")
            num_rows = my_frame.count()
            print num_rows

        The result could be::

            298376527

        """
        try:
            return self._backend.count(self)
        except:
            raise IaError(logger)

    def drop(self, predicate):
        """
        Summary
        -------
        Drop rows

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Remove all rows from the frame which satisfy the predicate.

        Parameters
        ----------
        predicate : function
            function or :term:`lambda` which takes a row argument and evaluates to a boolean value

        Raises
        ------
        IaError

        Examples
        --------
        For this example, my_frame is a BigFrame object accessing a frame with lots of data for the attributes of *lions*, *tigers*, and *ligers*.
        Get rid of the *lions* and *tigers*::

            my_frame.drop(lambda row: row.animal_type == "lion" or row.animal_type == "tiger")

        Now the frame only has information about *ligers*.

        For further examples, see :ref:`example_frame.drop`

        """
        # TODO - Review docstring
        try:
            self._backend.drop(self, predicate)
        except:
            raise IaError(logger)

    def drop_duplicates(self, columns=[]):
        """
        Summary
        -------
        Remove duplicates

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Remove duplicate rows, keeping only one row per uniqueness criteria match

        Parameters
        ----------
        columns : str OR list of str
            column name(s) to identify duplicates. If empty, will remove duplicates that have whole row data identical.
    
        Raises
        ------
        IaError

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

        For further examples, see :ref:`example_frame.drop_duplicates`

        """
        try:
            self._backend.drop_duplicates(self, columns)
        except:
            raise IaError(logger)

    def filter(self, predicate):
        """
        Summary
        -------
        Select data

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Select all rows which satisfy a predicate.

        Parameters
        ----------
        predicate: function
            function definition or lambda which takes a row argument and evaluates to a boolean value

        Raises
        ------
        IaError

        Examples
        --------
        For this example, my_frame is a BigFrame object with lots of data for the attributes of *lizards*, *frogs*, and *snakes*.
        Get rid of everything, except information about *lizards* and *frogs*::

            my_frame.filter(animal_type == "lizard" or animal_type == "frog")

        The frame now only has data about lizards and frogs

        For further examples, see :ref:`example_frame.filter`

        """
        # TODO - Review docstring
        try:
            self._backend.filter(self, predicate)
        except:
            raise IaError(logger)

    def flatten_column(self, column_name):
        """
        Summary
        -------
        Spread out data

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Search through the currently active BigFrame for multiple items in a single specified column.
        When it finds multiple values in the column, it replicates the row and separates the multiple items across the existing and new rows.
        Multiple items is defined in this case as being things separated by commas.

        Parameters
        ----------
        column_name : str
            The column to be flattened

        Raises
        ------
        IaError

        Returns
        -------
        BigFrame
            A BigFrame object proxy for the new flattened frame

        Examples
        --------
        See :ref:`example_frame.flatten_column`.

        """

        try:
            return self._backend.flatten_column(self, column_name)
        except:
            raise IaError(logger)

    def fmeasure(self, label_column, pred_column, pos_label=1, beta=1):
        """
        Summary
        -------
        Model :math:`F_{\\beta}` measure

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Computes the :math:`F_{\\beta}` measure for a classification model.
        A column containing the correct labels for each instance and a column containing the predictions made by the
        model are specified.
        The :math:`F_{\\beta}` measure of a binary classification model is the harmonic mean of precision and
        recall.
        If we let:
        
        * beta :math:`\\equiv \\beta`,
        * :math:`T_{P}` denote the number of true positives,
        * :math:`F_{P}` denote the number of false positives, and
        * :math:`F_{N}` denote the number of false negatives,
            
        then:
        
        .. math::
            F_{\\beta} = (1 + \\beta ^ 2) * \\frac{\\frac{T_{P}}{T_{P} + F_{P}} * \\frac{T_{P}}{T_{P} + F_{N}}}{\\beta ^ 2 * (\\frac{T_{P}}{T_{P} + F_{P}} + \\frac{T_{P}}{T_{P} + F_{N}})}

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
            beta value to use for :math:`F_{\\beta}` measure (default F1 measure is computed); must be greater than 0

        Returns
        ----------
        float64
            the :math:`F_{\\beta}` measure for the classifier

        Examples
        ----------
        ::

            f1 = frame.fmeasure('labels', 'predictions')
            f2 = frame.fmeasure('labels', 'predictions', beta=2)
            f1_binary = frame.fmeasure('labels', 'predictions', pos_label='good')

        """
        return self._backend.classification_metric(self, 'fmeasure', label_column, pred_column, pos_label, beta)

    @doc_stub
    def column_summary_statistics(self, data_column, weights_column_name = None):
        """
        Calculate summary statistics of a column.

        Parameters
        ----------
        data_column : str
            The column to be statistically summarized. Must contain numerical data.

        weights_column : str
            Optional. The column that provides weights (frequencies) for the data being summarized.
            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
                parameter is not provided.

        Returns
        -------
        summary : Dict
            Dictionary containing summary statistics in the following entries:
                 mean : Arithmetic mean of the data.
                 geometric_mean : Geometric mean of the data.
                 variance : Variance of the data where weighted sum of squared distance from the mean is divided by
                  count - 1
                 standard_deviation : Standard deviation of the data.
                 mode : A mode of the data; that is, an item with the greatest weight (largest frequency).
                  Ties are resolved arbitrarily.
                 minimum : Minimum value in the data.
                 maximum : Maximum value in the data.
                 count : The number of entries - not necessarily distinct. Equivalently, the number of rows in the input
                  table.

        Examples
        --------
        >>> mean = frame.column_summary_statistics('data column', 'weight column')
        """
        pass

    @doc_stub
    def column_mode (self, data_column, weights_column = None):
        """
        Calculate the mode of a column.

        Parameters
        ----------
        data_column : str
            The column whose mode is to be calculated

        weights_column : str
            Optional. The column that provides weights (frequencies) for the mode calculation.
            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
                parameter is not provided.

        Returns
        -------
        mode : Dict
            Dictionary containing summary statistics in the following entries:
                mode : Mode of the data. (Ties resolved arbitrarily.
                weight_of_mode : Weight of the mode.
                total_weight : Sum of all weights in the weight column. (This will be the row count if no weights are
                    given.)

        Examples
        --------
        >>> mode = frame.column_mode('interesting column')
        """
        pass

    @doc_stub
    def column_median(self, data_column, weights_column = None):
        """
        Calculate the median of a column.

        Parameters
        ----------
        data_column : str
            The column whose median is to be calculated

        weights_column : str
            Optional. The column that provides weights (frequencies) for the median calculation.
            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
                parameter is not provided.

        Returns
        -------
        median : Double
            The median of the values.

        Examples
        --------
        >>> median = frame.column_median('interesting column')
        """
        pass



    def groupby(self, groupby_columns, *aggregation_arguments):
        """
        Summary
        -------
        Create summarized frame

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Creates a new frame and returns a BigFrame object to access it.
        Takes a column or group of columns, finds the unique combination of values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        groupby_columns: column name or list of column names (or function TODO)
            columns or result of a function will be used to create grouping
        aggregation: one or more aggregation functions or dictionaries of
            (column,aggregation function) pairs

        Raises
        ------
        IaError

        Returns
        -------
        frame
            new aggregated frame

        Notes
        -----
        * The column names created by aggregation functions in the returned frame are the original column name appended with the '_' character and the aggregation function. For example, if the original field is 'a' and the function is 'avg', the resultant column is named 'a_avg'.
        * An aggregation argument of 'count' results in a column named 'count'

        Examples
        --------
        For setup, we will use a BigFrame *my_frame* accessing a frame with a column *a*.
        Column *a* has the values 'cat', 'apple', 'bat', 'cat', 'bat', 'cat'.
        Create a new frame, combining similar values of column *a*, and count how many of each value is in the original frame::

            column_a = my_frame.a
            new_frame = my_frame.groupBy(column_a, count)

        The new BigFrame *new_frame* can access a new frame with two columns *a* and *count*.
        The data, in no particular order, is::

            Column *a* is 'apple' and column *count* is 1.
            Column *a* is 'bat' and column *count* is 2.
            Column *a* is 'cat' and column *count* is 3.

        In this example, 'my_frame' is accessing a frame with three columns, *a*, *b*, and *c*.
        The data in these columns is::

            *a* is 1, *b* is 'alpha', *c* is 3.0
            *a* is 1, *b* is 'bravo', *c* is 5.0
            *a* is 1, *b* is 'alpha', *c* is 5.0
            *a* is 2, *b* is 'bravo', *c* is 8.0
            *a* is 2, *b* is 'bravo', *c* is 12.0

        Break out the three columns for clarity; create a new frame from this data, grouping the rows by unique combinations of column *a* and *b*;
        average the value in *c* for each group::

            column_a = my_frame.a
            column_b = my_frame.b
            column_c = my_frame.c
            new_frame = my_frame.groupBy([column_a, column_b], {column_c: avg})

        The new frame accessed by the BigFrame *new_frame* has three columns named *a*, *b*, and *c_avg*.
        The resultant data is::

            *a* is 1, *b* is 'alpha', *c_avg* is 4.0
            *a* is 1, *b* is 'bravo', *c_avg* is 5.0
            *a* is 2, *b* is 'bravo', *c_avg* is 10.0

        For this example, we use *my_frame* with columns *a*, *c*, *d*, and *e*.
        Column types will be str, int, float, int.
        The data is::

            *a* is 'ape', *c* is 1, *d* is 4.0, *e* is 9
            *a* is 'ape', *c* is 1, *d* is 8.0, *e* is 8
            *a* is 'big', *c* is 1, *d* is 5.0, *e* is 7
            *a* is 'big', *c* is 1, *d* is 6.0, *e* is 6
            *a* is 'big', *c* is 1, *d* is 8.0, *e* is 5

        Break out the columns for clarity; create a new frame from this data, grouping the rows by unique combinations of column *a* and *c*;
        count each group; for column *d* calculate the average, sum and minimum value; for column *e*, save the maximum value::

            column_d = my_frame.d
            column_e = my_frame.e
            new_frame = my_frame.groupBy(my_frame[['a', 'c']], count, {column_d: [avg, sum, min], column_e: [max]})

        The new frame accessed by BigFrame *new_frame* has columns *a*, *c*, *count*, *d_avg*, *d_sum*, *d_min*, and *e_max*.
        The column types are (respectively): str, int, int, float, float, float, int.
        The data is::

            *a* is 'ape', *c* is 1, *count* is 2, *d_avg* is 6.0, *d_sum* is 12.0, *d_min* is 4.0, *e_max* is 9
            *a* is 'big', *c* is 1, *count* is 3, *d_avg* is 6.333333, *d_sum* is 19.0, *d_min* is 5.0, *e_max* is 7

        For further examples, see :ref:`example_frame.groupby`.

        """
        try:
            return self._backend.groupby(self, groupby_columns, aggregation_arguments)
        except:
            raise IaError(logger)


    def inspect(self, n=10, offset=0):
        """
        Summary
        -------
        Print data

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Print the data in readable format.

        Parameters
        ----------
        n : int
            The number of rows to print
        offset : int
            The number of rows to skip before printing
            
        Raises
        ------
        IaError

        Returns
        -------
        data
            Formatted for ease of human inspection
            
        Examples
        --------
        For an example, see :ref:`example_frame.inspect`
        
        """
        # TODO - Review docstring
        try:
            return self._backend.inspect(self, n, offset)
        except:
            raise IaError(logger)

    def join(self, right, left_on, right_on=None, how='inner'):
        """
        Summary
        -------
        Combine frames

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Create a new BigFrame from a SQL JOIN operation with another BigFrame.
        The BigFrame on the 'left' is the currently active frame.
        The BigFrame on the 'right' is another frame.
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

        Raises
        ------
        IaError

        Returns
        -------
        BigFrame
            BigFrame accessing a new joined frame

        Notes
        -----
        When a column is named the same in both frames, it will result in two columns in the new frame.
        The column from the *left* frame will be copied and the column name will have the string "_L" added to it.
        The same thing will happen with the column from the *right* frame, except its name has the string "_R" appended.

        It is recommended that you rename the columns to meaningful terms prior to using the ``join`` method.

        Examples
        --------
        For this example, we will use a BigFrame *frame1* accessing a frame with columns *a*, *b*, *c*, and a BigFrame *frame2* accessing
        a frame with columns *a*, *d*, *e*.
        Join the two frames keeping only those rows having the same value in column *a*::

            frame1 = BigFrame(schema1)
            frame2 = BigFrame(schema2)
            joined_frame = frame1.join(frame2, 'a')

        Now, joined_frame is a BigFrame accessing a frame with the columns *a*, *b*, *c*, *d*, and *e*.
        The data in the new frame will be from the rows where column 'a' was the same in both frames.

        Now, using a single BigFrame *my_frame* accessing a frame with the columns *b* and *book*.
        Build a new frame, but remove any rows where the values in *b* and *book* do not match::

            joined_frame = frame2.join(frame2, left_on='b', right_on='book', how='inner')

        We end up with a new BigFrame *joined_frame* accessing a new frame with all the original columns, but only those rows where the data in the
        original frame in column *b* matched the data in column *book*.

        For further examples, see :ref:`example_frame.join`.

        """
        try:
            return self._backend.join(self, right, left_on, right_on, how)
        except:
            raise IaError(logger)

    def precision(self, label_column, pred_column, pos_label=1):
        """
        Summary
        -------
        Model precision

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Computes the precision measure for a classification model
        A column containing the correct labels for each instance and a column containing the predictions made by the
        model are specified.  The precision of a binary classification model is the proportion of predicted positive
        instances that are correct.  If we let :math:`TP` denote the number of true positives and :math:`FP` denote the number of false
        positives, then the model precision is given by: :math:`TP / (TP + FP)`.

        For multi-class classification, the precision measure is computed as the weighted average of the precision
        for each label, where the weight is the number of instances with each label in the labeled column.  The
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
        ::
        
            prec = frame.precision('labels', 'predictions')
            prec2 = frame.precision('labels', 'predictions', 'yes')

        """
        return self._backend.classification_metric(self, 'precision', label_column, pred_column, pos_label, 1)


    def project_columns(self, column_names, new_names=None):
        """
        Summary
        -------
        Create frame from columns

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Copies specified columns into a new BigFrame object, optionally renaming them.

        Parameters
        ----------

        column_names : str OR list of str
            column name OR list of column names to be copied from the currently active frame
        new_names : str OR list of str
            The new name(s) for the column(s)

        Notes
        -----
        If new column names are specified, the quantity of column names must match the quantity of new names,
        though if you are only using a single column, it does not matter whether that column is declared in string
        fashion, or as a single string in a list.

        Raises
        ------
        ValueError
            number of columns specified in column_names does not match the number of columns specified in new_names
        Raises
        ------
        IaError


        Returns
        -------
        frame : BigFrame
            A new frame object containing copies of the specified columns

        Examples
        --------
        Given a BigFrame *frame1*, accessing a frame with columns named *a*, *b*, *c*, *d*.
        Create a new frame with three columns *apple*, *boat*, and *frog*, where for each row of the original frame, the data from column *a* is copied
        to the new column *apple*, the data from column *b* is copied to the column *boat*, and the data from column *c* is copied
        to the column *frog*::

            new_frame = frame1.project_columns( ['a', 'b', 'c'], ['apple', 'boat', 'frog'])

        And the result is a new BigFrame named 'new_name' accessing a new frame with columns *apple*, *boat*, *frog*, and the data from *frame1*,
        column *a* is now copied in column *apple*, the data from column *b* is now copied in column *boat* and the data from column *c* is now
        copied in column *frog*.

        Continuing::

            frog_frame = new_frame.project_columns('frog')

        And the new BigFrame *frog_frame* is accessing a frame with a single column *frog* which has a copy of all the data from the original
        column *c* in *frame1*.

        """
        # TODO - need example in docstring
        try:
            projected_frame = BigFrame()
            self._backend.project_columns(self, projected_frame, column_names, new_names)
            return projected_frame
        except:
            raise IaError(logger)

    def recall(self, label_column, pred_column, pos_label=1):
        """
        Summary
        -------
        Model measure

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Computes the recall measure for a classification model
        A column containing the correct labels for each instance and a column containing the predictions made by the
        model are specified.  The recall of a binary classification model is the proportion of positive instances that
        are correctly identified.  If we let :math:`TP` denote the number of true positives and :math:`FN` denote the number of false
        negatives, then the model recall is given by: :math:`TP / (TP + FN)`.

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
        ----------
        float64
            the recall measure for the classifier

        Examples
        ----------
        ::

            rec = frame.recall('labels', 'predictions')
            rec2 = frame.recall('labels', 'predictions', 'pos')

        """
        return self._backend.classification_metric(self, 'recall', label_column, pred_column, pos_label, 1)

    @doc_stub
    def remove_columns(self, name):
        """
        Summary
        -------
        Delete columns

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Remove columns from the BigFrame object.

        Parameters
        ----------
        name : str OR list of str
            column name OR list of column names to be removed from the frame

        Raises
        ------
        KeyError
            Attempting deletion of non-existant column
            Check column name spelling
        IaError

        Notes
        -----
        Deleting the last column in a frame leaves the frame empty.

        Examples
        --------
        For this example, BigFrame object *my_frame* accesses a frame with columns *column_a*, *column_b*, *column_c* and *column_d*.
        Eliminate columns *column_b* and *column_d*::

            my_frame.remove_columns([ column_b, column_d ])

        Now the frame only has the columns *column_a* and *column_c*.

        For further examples, see :ref:`example_frame.remove_columns`
        """
        pass
        # TODO - Review examples
        #try:
        #    self._backend.remove_columns(self, name)
        #except:
        #    raise IaError(logger)

    def rename_columns(self, column_names, new_names):
        """
        Summary
        -------
        Rename column

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Renames columns in a frame.

        Parameters
        ----------
        column_names : str or list of str
            The name(s) of the existing column(s).
        new_names : str
            The new name(s) for the column(s). Must not already exist.

        Examples
        --------
        Start with a frame with columns *Wrong* and *Wong*.
        Rename the columns to *Right* and *Wite*::

            frame.rename_columns(["Wrong", "Wong"], ["Right", "Wite"])

        Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

        For further examples, see :ref:`example_frame.rename_columns`
        """
        try:
            self._backend.rename_columns(self, column_names, new_names)
        except:
            raise IaError(logger)

    def take(self, n, offset=0):
        """
        Summary
        -------
        Get data subset

        .. versionadded:: 0.8

        Extended Summary
        ----------------
        Take a subset of the currently active BigFrame.

        Parameters
        ----------
        n : int
            The number of rows to copy from the currently active BigFrame
        offset : int
            The number of rows to skip before copying

        Raises
        ------
        IaError

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain number of rows, the rows obtained may be different every time the
        command is executed, even if the parameters do not change.

        Examples
        --------
        BigFrame *my_frame* accesses a frame with millions of rows of data.
        Get a sample of 5000 rows::

            r = my_frame.take( 5000 )

        We now have a separate frame accessed by a BigFrame *r* with a copy of the first 5000 rows of the original frame.

        If we use the function with an offset like::

            r = my_frame.take( 5000, 1000 )

        We end up with a new frame accessed by the BigFrame *r* again, but this time it has a copy of rows 1001 to 5000 of the original frame.
        
        """
        # TODO - Review and complete docstring
        try:
            return self._backend.take(self, n, offset)
        except:
            raise IaError(logger)


