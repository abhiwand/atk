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

import logging

logger = logging.getLogger(__name__)

from intelanalytics.core.column import BigColumn
from intelanalytics.core.errorhandle import IaError
from intelanalytics.core.command import CommandSupport, doc_stub


def _get_backend():
    from intelanalytics.core.config import get_frame_backend
    return get_frame_backend()


def get_frame_names():
    """
    BigFrame names.

    Gets the names of BigFrame objects available for retrieval

    Returns
    -------
    list of strings
        Names of the all BigFrame objects

    Examples
    --------
    Create two BigFrame objects and get their names::

        my_frame = BigFrame(csv_schema_1, "BigFrame1")
        your_frame = BigFrame(csv_schema_2, "BigFrame2")
        frame_names = get_frame_names()
        print frame_names

    Result would be::

        ["BigFrame1", "BigFrame2"]

    .. versionadded:: 0.8

    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame_names()
    except:
        raise IaError(logger)


def get_frame(name):
    """
    Get BigFrame.

    Retrieves a BigFrame class object to allow access to the data frame

    Parameters
    ----------
    name : string
        String containing the name of the BigFrame object

    Returns
    -------
    class
        BigFrame class object

    Examples
    --------
    Create a frame *my_frame*; create a BigFrame proxy for it; check that the new BigFrame is equivalent to the original::

        my_frame = BigFrame(my_csv, "my_frame")
        your_frame = get_frame("my_frame")
        print my_frame == your_frame

    Result would be::

        True

    .. versionadded:: 0.8

    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame(name)
    except:
        raise IaError(logger)

def delete_frame(frame):
    """
    Erases data.

    Deletes the frame from backing store

    Parameters
    ----------
    frame : string or BigFrame
        Either the name of the BigFrame object to delete or the BigFrame object itself

    Returns
    -------
    string
        The name of the deleted frame

    Examples
    --------
    Create a new frame; delete it; print what gets returned from the function::

        my_frame = BigFrame(my_csv, 'my_frame')
        deleted_frame = delete_frame('my_frame')
        print deleted_frame

    The result would be::

        "my_frame"

    .. versionadded:: 0.8

    """
    try:
        return _get_backend().delete_frame(frame)
    except:
        raise IaError(logger)


class BigFrame(CommandSupport):
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
    An automatically generated name will be the word "frame_" followed by the uuid.uuid4().hex and
    if allowed, an "_" character then the name of the data source.
    For example, ``frame_b21a3475a2175f165ba7...``

    Examples
    --------
    Create a BigFrame object; name it "my_frame"::

        g = BigFrame(my_csv_schema, "my_frame")

    A BigFrame object has been created and *g* is its proxy. It brought in the data described by *my_csv_schema*. It is named *my_frame*.

    Create an empty frame; name it "your_frame"::

        h = BigFrame(name='your_frame')

    A BigFrame object has been created and *h* is its proxy. It has no data yet, but it does have the name *your_frame*.

    For other examples, see :ref:`example_frame.bigframe`.

    .. versionadded:: 0.8

    """
    # TODO - Review Parameters, Examples

    def __init__(self, source=None, name=None):
        try:
            self._id = 0
            self._error_frame_id = None
            if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
                self._backend = _get_backend()
            new_frame_name = self._backend.create(self, source, name)
            CommandSupport.__init__(self)
            logger.info('Created new frame "%s"', new_frame_name)
        except:
            raise IaError(logger)

    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name == '_backend':
            raise AttributeError('_backend')
        try:
            return super(BigFrame, self).__getattribute__(name)
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

    # We are not defining __delitem__.  Columns must be deleted w/ remove_columns

    def __repr__(self):
        try:
            return self._backend.get_repr(self)
        except:
            return super(BigFrame, self).__repr__() + " (Unable to collect metadata from server)"

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
        Column names.

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

        .. versionadded:: 0.8

        """
        try:
            return [name for name, data_type in self._backend.get_schema(self)]
        except:
            raise IaError(logger)

    @property
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

            frame = BigFrame(name="Flavor Recipes")
            given_name = frame.name
            print given_name

        The result given is::

            "Flavor Recipes"

        .. versionadded:: 0.8

        """
        try:
            return self._backend.get_name(self)
        except:
            IaError(logger)

    @name.setter
    def name(self, value):
        """
        Set frame name.

        Assigns a name to a data frame.

        Examples
        --------
        Assign the name "movies" to the current frame::

            my_frame.name("movies")

        .. versionadded:: 0.8

        """
        try:
            self._backend.rename_frame(self, value)
        except:
            raise IaError(logger)

    @property
    def row_count(self):
        """
        Count the rows.

        Returns
        -------
        The number of rows in the frame.

        .. versionadded:: 0.8
        """
        try:
            return self._backend.get_row_count(self)
        except:
            raise IaError(logger)

    @property
    def schema(self):
        """
        BigFrame schema.

        The schema of the current BigFrame object.

        Returns
        -------
        schema
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

        .. versionadded:: 0.8

        """
        try:
            return self._backend.get_schema(self)
        except:
            raise IaError(logger)

    def accuracy(self, label_column, pred_column):
        """
        Model accuracy.

        Computes the accuracy measure for a classification model
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
            |-------------------------------------------------------|
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.accuracy('labels', 'predictions')

            0.75

        .. versionadded:: 0.8

        """
        try:
            return self._backend.classification_metric(self, 'accuracy', label_column, pred_column, '1', 1)
        except:
            raise IaError(logger)


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

            my_frame.add_columns(function_b, [("calculated_b", float32)])
        
        More information on row functions can be found at :doc:`ds_apir`.
        
        For further examples, see :ref:`example_frame.add_columns`.

        .. versionadded:: 0.8

        """
        try:
            self._backend.add_columns(self, func, schema)
        except:
            raise IaError(logger)

    def append(self, data):
        """
        Add data.

        Adds more data (rows and/or columns) to the BigFrame object.

        Parameters
        ----------
            data : a BigFrame describing the data being added

        Examples
        --------
        Given a frame with a single column *col_1* and a frame with two columns *col_1* and *col_2*.
        Column *col_1* means the same thing in both frames.
        BigFrame *my_frame* points to the first frame and *your_frame* points to the second.
        Add the contents of the *your_frame* to *my_frame*::

            my_frame.append(your_frame)

        Now the first frame has two columns, *col_1* and *col_2*.
        Column *col_1* has the data from *col_1* in both frames.
        Column *col_2* has None (undefined) in all of the rows in the original first frame, and has the value of the second frame column *col_2* in
        the rows matching the new data in *col_1*.

        Breaking it down differently, the original rows refered to by *my_frame* have a new column *col_2* and this new column is
        filled with non-defined data.
        The frame referred to by *your_frame* is then added to the bottom.

        For further example, see :ref:`Data flow <example_frame.append>`.

        .. versionadded:: 0.8

        """
        # TODO - Review examples
        try:
            self._backend.append(self, data)
        except:
            raise IaError(logger)

    @doc_stub
    def assign_sample(self, sample_percentages, sample_labels = ["TR", "TE", "VA"], output_column = "sample_bin", random_seed = 0):
        """
        Assign classes to rows.

        Randomly assign classes to rows given a vector of percentages.
         The table receives an additional column that contains a random label generated by the probability distribution
         function specified by a list of floating point values.
         The labels are non-negative integers drawn from the range [ 0,  len(split_percentages) - 1].
         Optionally, the user can specify a list of strings to be used as the labels. If the number of labels is 3,
         the labels will default to "TR", "TE" and "VA".

        Parameters
        ----------
        sample_percentages : list of floating point values
            Entries are non-negative and sum to 1.
            If the *i*'th entry of the  list is *p*,
            then then each row receives label *i* with independent probability *p*.
        sample_labels : str (optional)
            Names to be used for the split classes.
            Defaults "TR", "TE", "VA" when there are three numbers given in split_percentages,
            defaults to Sample#0, Sample#1, ... otherwise.
        output_column : str (optional)
            Name of the new column which holds the labels generated by the function
        random_seed : int (optional)
            Random seed used to generate the labels. Defaults to 0.

        Examples
        --------
        For this example, my_frame is a BigFrame object accessing a frame with data.
        Append a new column *sample_bin* to the frame;
        Assign the value in the new column to "train", "test", or "validate"::

            my_frame.assign_sample([0.3, 0.3, 0.4], ["train", "test", "validate"])

        Now the frame accessed by BigFrame *my_frame* has a new column named "sample_bin" and each row contains one of the values "train",
        "test", or "validate".  Values in the other columns are unaffected.

        .. versionadded:: 0.8

        """
        pass

    def bin_column(self, column_name, num_bins, bin_type='equalwidth', bin_column_name='binned'):
        """
        Column values into bins.

        Two types of binning are provided: `equalwidth` and `equaldepth`.

        Equal width binning places column values into bins such that the values in each bin fall within the same
        interval and the interval width for each bin is equal.

        Equal depth binning attempts to place column values into bins such that each bin contains the same number of
        elements.  For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin number is determined by:

        .. math::

            ceiling \\left( n * \\frac {f(C)}{m} \\right)

        where :math:`f` is a tie-adjusted ranking function over values of :math:`C`.
        If there are multiple of the same value in :math:`C`, then their tie-adjusted rank is the average of their ordered rank values.

        The num_bins parameter is upper-bound on the number of bins since the data may justify fewer bins.
        With :term:`equal depth binning`, for example, if the column to be binned has 10 elements with
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
            [':term:`equalwidth`' | ':term:`equaldepth`']
        bin_column_name : str (optional)
            The name for the new binned column

        Returns
        -------
        BigFrame
            A BigFrame accessing a new frame with binned column appended to original frame

        Examples
        --------
        For this example, we will use a frame with column *a* and a BigFrame *my_frame* accessing it::

            my_frame.inspect( n=11 )

              a int32
            |--------|
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

              a int32     aEWBinned int32
            |----------------------------|
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

              a int32     aEDBinned int32
            |----------------------------|
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
        try:
            return self._backend.bin_column(self, column_name, num_bins, bin_type, bin_column_name)
        except:
            raise IaError(logger)

    def calculate_percentiles(self, column_name, percentiles):
        """
        Calculate percentiles on given column.

        Parameters
        ----------
        column_name : str
            The column to calculate percentile
        percentiles : int OR list of int. If float is provided, it will be rounded to int

        Returns
        -------
        dictionary

        Examples
        --------
        ::

            my_frame.calculate_percentiles('final_sale_price', [10, 50, 100])

        .. versionadded:: 0.8

        """
        try:
            percentiles_result = self._backend.calculate_percentiles(self, column_name, percentiles).result.get('percentiles')
            result_dict = {}
            for p in percentiles_result:
                result_dict[p.get("percentile")] = p.get("value")

            return result_dict
        except:
            raise IaError(logger)

    @doc_stub
    def column_mode (self, data_column, weights_column = None, max_modes_returned = None):
        """
        Calculate modes of a column.  A mode is a data element of maximum weight. All data elements of weight <= 0
        are excluded from the calculation, as are all data elements whose weight is NaN or infinite.
        If there are no data elements of finite weight > 0, no mode is returned.

        Because data distributions often have mutliple modes, it is possible for a set of modes to be returned. By
         default, only one is returned, but my setting the optional parameter max_number_of_modes_returned, a larger
         number of modes can be returned.

        Parameters
        ----------
        data_column : str
            The column whose mode is to be calculated

        weights_column : str
            Optional. The column that provides weights (frequencies) for the mode calculation.
            Must contain numerical data. Uniform weights of 1 for all items will be used for the calculation if this
                parameter is not provided.

        max_modes_returned : int
            Optional. Maximum number of modes returned. If this parameter is not provided, it defaults to 1

        Returns
        -------
        mode : Dict
            Dictionary containing summary statistics in the following entries:
                mode : A mode is a data element of maximum net weight. A set of modes is returned.
                 The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
                 maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
                 > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
                 (per a canonical ordering) are returned.
                weight_of_mode : Weight of a mode. If there are no data elements of finite weight > 0,
                 the weight of the mode is 0. If no weights column is given, this is the number of appearances of
                 each mode.
                total_weight : Sum of all weights in the weight column. This is the row count if no weights
                 are given. If no weights column is given, this is the number of rows in the table with non-zero weight.
                mode_count : The number of distinct modes in the data. In the case that the data is very multimodal,
                 this number may well exceed max_number_of_modes_returned.

        Example
        -------
        >>> mode = frame.column_mode('modum columpne')
        """
        pass

    @doc_stub
    def column_summary_statistics(self, data_column, weights_column_name = None):
        """
        Calculate summary statistics of a column.

        Parameters
        ----------
        data_column : str
            The column to be statistically summarized.
            Must contain numerical data; all NaNs and infinite values are excluded from the calculation.
        weights_column_name : str (optional)
            Name of column holding weights of column values

        Returns
        -------
        summary : Dict
            Dictionary containing summary statistics in the following entries:

            | mean:
                  Arithmetic mean of the data.

            | geometric_mean:
                  Geometric mean of the data. None when there is a data element <= 0, 1.0 when there are no data elements.

            | variance:
                  Variance of the data where  sum of squared distance from the mean is divided by count - 1.
                  None when there are <= 1 many data elements.

            | standard_deviation:
                  Standard deviation of the data. None when there are <= 1 many data elements.

            | valid_data_count:
                  The count of all data elements that are finite numbers.
                  (In other words, after excluding NaNs and infinite values.)

            | minimum:
                  Minimum value in the data. None when there are no data elements.

            | maximum:
                  Maximum value in the data. None when there are no data elements.

            | mean_confidence_lower:
                  Lower limit of the 95% confidence interval about the mean.
                  Assumes a Gaussian distribution. None when there are 0 or 1 data elements.

            | mean_confidence_upper:
                  Upper limit of the 95% confidence interval about the mean.
                  Assumes a Gaussian distribution. None when there are 0 or 1 data elements.

        Notes
        -----
        Return Types
            | valid_data_count returns a Long.
            | All other values are returned as Doubles or None.

        Variance
            Variance is computed by the following formula:

        .. math::

            \\left( \\frac{1}{n - 1} \\right) * sum_{i}  \\left(x_{i} - M \\right) ^{2}

        where :math:`n` is the number of valid elements of positive weight, and :math:`M` is the mean.

        Standard Deviation
            The square root of the variance.

        Examples
        --------
        ::

            stats = frame.column_summary_statistics('data column', 'weight column')

        .. versionadded:: 0.8

        """
        pass

    def confusion_matrix(self, label_column, pred_column, pos_label=1):
        """
        Builds matrix.

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
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::
        
            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            |-------------------------------------------------------|
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

        return self._backend.confusion_matrix(self, label_column, pred_column, pos_label)

    def copy(self):
        """
        Copy frame.

        Creates a full copy of the current frame.

        Returns
        -------
        BigFrame
            A new frame object which is a copy of the currently active BigFrame

        Examples
        --------
        Build a BigFrame from a csv file with 5 million rows of data; call the frame "cust"::

            my_frame = BigFrame(source="my_data.csv")
            my_frame.name("cust")

        At this point we have one frame of data, which is now called "cust".
        Let's copy it to a new frame::

            your_frame = my_frame.copy()

        Now we have two frames of data, each with 5 million rows. Checking the names::

            print my_frame.name()
            print your_frame.name()

        Gives the results::

            "cust"
            "frame_75401b7435d7132f5470ba35..."

        .. versionadded:: 0.8

        """
        try:
            copied_frame = BigFrame()
            self._backend.project_columns(self, copied_frame, self.column_names)
            return copied_frame
        except:
            raise IaError(logger)

    def cumulative_count(self, sample_col, count_value):
        """
        Compute a cumulative count.
        
        A cumulative count is computed by sequentially stepping through the column values and keeping track of the
        the number of times the specified *count_value* has been seen up to the current value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative count
        count_value : any
            The column value to be used for the counts

        Returns
        -------
        BigFrame
            A new object accessing a new frame containing the original columns appended with a column containing the cumulative counts

        Examples
        --------
        Consider BigFrame *my_frame*, which accesses a frame that contains a single column *obs*::

            my_frame.inspect()

             obs int32
            |---------|
               0
               1
               2
               0
               1
               2
              
        The cumulative count for column *obs* using *count_value = 1* is obtained by::

            cc_frame = my_frame.cumulative_count('obs', 1)

        The BigFrame *cc_frame* accesses a frame which contains two columns *obs* and *obsCumulativeCount*.
        Column *obs* still has the same data and *obsCumulativeCount* contains the cumulative counts::

            cc_frame.inspect()

             obs int32   obsCumulativeCount int32
            |------------------------------------|
               0                          0
               1                          1
               2                          1
               0                          1
               1                          2
               2                          2

        .. versionadded:: 0.8

        """
        try:
            return self._backend.cumulative_dist(self, sample_col, 'cumulative_count', count_value)
        except:
            raise IaError(logger)

    def cumulative_percent_sum(self, sample_col):
        """
        Compute a cumulative percent sum.

        A cumulative percent sum is computed by sequentially stepping through the column values and keeping track of the
        current percentage of the total sum accounted for at the current value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative percent sum

        Returns
        -------
        BigFrame
            A new object accessing a new frame containing the original columns appended with a column containing the cumulative percent sums

        Notes
        -----
        This function applies only to columns containing numerical data.

        Examples
        --------
        Consider BigFrame *my_frame* accessing a frame that contains a single column named *obs*::

            my_frame.inspect()

             obs int32
            |---------|
               0
               1
               2
               0
               1
               2
 
        The cumulative percent sum for column *obs* is obtained by::

            cps_frame = my_frame.cumulative_percent_sum('obs')

        The new frame accessed by BigFrame *cps_frame* contains two columns *obs* and *obsCumulativePercentSum*.
        They contain the original data and the cumulative percent sum, respectively::

            cps_frame.inspect()

             obs int32   obsCumulativePercentSum float64
            |-------------------------------------------|
               0                          0.0
               1                          0.16666666
               2                          0.5
               0                          0.5
               1                          0.66666666
               2                          1.0
        
        .. versionadded:: 0.8

        """
        try:
            return self._backend.cumulative_dist(self, sample_col, 'cumulative_percent_sum')
        except:
            raise IaError(logger)

    def cumulative_percent_count(self, sample_col, count_value):
        """
        Compute a cumulative percent count.

        A cumulative percent count is computed by sequentially stepping through the column values and keeping track of
        the current percentage of the total number of times the specified *count_value* has been seen up to the current
        value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative sum
        count_value : any
            The column value to be used for the counts

        Returns
        -------
        BigFrame
            A new object accessing a new frame containing the original columns appended with a column containing the cumulative percent counts

        Examples
        --------
        Consider BigFrame *my_frame*, which accesses a frame that contains a single column named *obs*::

            my_frame.inspect()

             obs int32
            |---------|
               0
               1
               2
               0
               1
               2

        The cumulative percent count for column *obs* is obtained by::

            cpc_frame = my_frame.cumulative_percent_count('obs', 1)

        The BigFrame *cpc_frame* accesses a new frame that contains two columns, *obs* that contains the original column values, and
        *obsCumulativePercentCount* that contains the cumulative percent count::

            cpc_frame.inspect()

             obs int32   obsCumulativePercentCount float64
            |---------------------------------------------|
               0                          0.0
               1                          0.5
               2                          0.5
               0                          0.5
               1                          1.0
               2                          1.0
         
        .. versionadded:: 0.8

        """
        try:
            return self._backend.cumulative_dist(self, sample_col, 'cumulative_percent_count', count_value)
        except:
            raise IaError(logger)

    def cumulative_sum(self, sample_col):
        """
        Compute a cumulative sum.

        A cumulative sum is computed by sequentially stepping through the column values and keeping track of the current
        cumulative sum for each value.

        Parameters
        ----------
        sample_col : string
            The name of the column from which to compute the cumulative sum

        Returns
        -------
        BigFrame
            A new object accessing a frame containing the original columns appended with a column containing the cumulative sums

        Notes
        -----
        This function applies only to columns containing numerical data.

        Examples
        --------
        Consider BigFrame *my_frame*, which accesses a frame that contains a single column named *obs*::

            my_frame.inspect()

             obs int32
            |---------|
               0
               1
               2
               0
               1
               2

        The cumulative percent count for column *obs* is obtained by::

            cs_frame = my_frame.cumulative_percent_count('obs', 1)

        The BigFrame *cs_frame* accesses a new frame that contains two columns, *obs* that contains the original column values, and
        *obsCumulativeSum* that contains the cumulative percent count::

            cs_frame.inspect()

             obs int32   obsCumulativeSum int32
            |----------------------------------|
               0                     0
               1                     1
               2                     3
               0                     3
               1                     4
               2                     6

        .. versionadded:: 0.8

        """
        try:
            return self._backend.cumulative_dist(self, sample_col, 'cumulative_sum')
        except:
            raise IaError(logger)

    def drop(self, predicate):
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

            my_frame.drop(lambda row: row.animal_type == "lion" or row.animal_type == "tiger")

        Now the frame only has information about *ligers*.

        More information on row functions can be found at :doc:`ds_apir`.
        
        For further examples, see :ref:`example_frame.drop`

        .. versionadded:: 0.8

        """
        # TODO - Review docstring
        try:
            self._backend.drop(self, predicate)
        except:
            raise IaError(logger)

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

        For further examples, see :ref:`example_frame.drop_duplicates`

        .. versionadded:: 0.8

        """
        try:
            self._backend.drop_duplicates(self, columns)
        except:
            raise IaError(logger)

    def ecdf(self, sample_col):
        """
        Empirical Cumulative Distribution.

        Generates the empirical cumulative distribution for the input column.

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
            |---------------------|
              red               1
              blue              3
              blue              1
              green             0

            result = frame.ecdf('b')
            result.inspect()

              b:int32   b_ECDF:float64
            |--------------------------|
              1                    0.2
              2                    0.5
              3                    0.8
              4                    1.0


        .. versionadded:: 0.8

        """
        try:
            return self._backend.ecdf(self, sample_col)
        except:
            raise IaError(logger)

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
        For this example, my_frame is a BigFrame object with lots of data for the attributes of *lizards*, *frogs*, and *snakes*.
        Get rid of everything, except information about *lizards* and *frogs*::

            my_frame.filter(animal_type == "lizard" or animal_type == "frog")

        The frame now only has data about lizards and frogs

        More information on row functions can be found at :doc:`ds_apir`.
        
        For further examples, see :ref:`example_frame.filter`

        .. versionadded:: 0.8

        """
        # TODO - Review docstring
        try:
            self._backend.filter(self, predicate)
        except:
            raise IaError(logger)

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
        See :ref:`example_frame.flatten_column`.

        .. versionadded:: 0.8

        """

        try:
            return self._backend.flatten_column(self, column_name)
        except:
            raise IaError(logger)

    def fmeasure(self, label_column, pred_column, pos_label=1, beta=1):
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
            |-------------------------------------------------------|
              red               1              0                  0
              blue              3              1                  0
              blue              1              0                  0
              green             0              1                  1

            frame.fmeasure('labels', 'predictions')

            0.66666666666666663

            frame.fmeasure('labels', 'predictions', beta=2)

            0.55555555555555558

            frame.fmeasure('labels', 'predictions', pos_label=0)

            0.80000000000000004

        .. versionadded:: 0.8

        """
        return self._backend.classification_metric(self, 'fmeasure', label_column, pred_column, pos_label, beta)

    @doc_stub
    def column_summary_statistics(self, data_column, weights_column_name = None):
        """
        Calculate summary statistics of a column.

        Parameters
        ----------
        data_column : str
            The column to be statistically summarized. Must contain numerical data; all rows containing
             NaNs and infinite values in the data column are excluded from the statistical calculations and
             logged to counters. (See bad_row_count and good_row_count below.)

        weights_column : str
            Optional. The column that provides weights (frequencies) for the data being summarized.
            Must contain numerical data. All rows containing NaNs and infinite values in the weights column are
             excluded from the statistical calculations and  logged to counters. (See bad_row_count and good_row_count
             below.)
             Uniform weights of 1 for all items will be used for the calculation if this
                parameter is not provided. Data elements with weights <= 0 are excluded from the calculation
                but the count is logged (see positive_weight_count and non_positive_weight_count below).

        Returns
        -------
        summary : Dict
            Dictionary containing summary statistics in the following entries:
                 mean : Weighted arithmetic mean of the data.
                 geometric_mean : Weighted geometric mean of the data. None when there is a data element <= 0,
                   1.0 when there are no data elements.
                 variance : Weighted variance of the data where weighted sum of squared distance from the mean is
                  divided by the number of valid entries - 1. None when there are <= 1 many data elements.
                 standard_deviation : Standard deviation of the data. None when there are <= 1 many data elements.
                 total_weight The sum of all weights over from valid input rows.
                  (Ie. neither data nor weight is NaN, or infinity, and weight is > 0.)
                 minimum : Minimum value in the data. None when there are no data elements.
                 maximum : Maximum value in the data. None when there are no data elements.
                 mean_confidence_lower : Lower limit of the 95% confidence interval about the mean.
                   Assumes a Gaussian distribution. None when there are 0 or 1 data elements.
                 mean_confidence_upper: Upper limit of the 95% confidence interval about the mean.
                  Assumes a Gaussian distribution. None when there are 0 or 1 data elements.
                 bad_row_count : The number of rows containing a NaN or infinite value in either the data
                  or weights column.
                 good_row_count : The number of rows not containing a NaN or infinite value in either the data
                  or weights column.
                 positive_weight_count : The number of valid data elements with weight > 0.
                   This is the number of entries used in the statistical calculation.
                 non_positive_weight_count : The number valid data elements with finite weight <= 0.

        Return Types:
            | good_row_count, bad_row_count, positive_weight_count and non_positive_weight_count return Longs.
            | All other values are returned as Doubles or None.

        Variance:
            Variance is computed by the following formula:

        .. math::

            \\left( \\frac{1}{n - 1} \\right) * sum_{i} w_{i} * \\left(x_{i} - M \\right)^_{2}

        where :math:`n` is the number of valid elements of positive weight, and :math:`M` is the  weighted mean

        Standard Deviation:
            The square root of the variance.

        Logging Invalid Data
        --------------------

        A row is bad when it contains a NaN or infinite value in either its data or weights column.  In this case, it
         contributes to bad_row_count; otherwise it contributes to good row count.

        A good row can be skipped because the value in its weight column is <=0. In this case, it contributes to
         non_positive_weight_count, otherwise (when the weight is > 0) it contributes to valid_data_weight_pair_count.

            Equations
            ---------
            bad_row_count + good_row_count = # rows in the frame
            positive_weight_count + non_positive_weight_count = good_row_count

        In particular, when no weights column is provided and all weights are 1.0, non_positive_weight_count = 0 and
         positive_weight_count = good_row_count

        Examples
        --------
        ::

            stats = frame.column_summary_statistics('data column', 'weight column')

        .. versionadded:: 0.8

        """
        pass



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
        try:
            return self._backend.get_frame_by_id(self._error_frame_id)
        except:
            raise IaError(logger)

    def groupby(self, groupby_columns, *aggregation_arguments):
        """
        Create summarized frame.

        Creates a new frame and returns a BigFrame object to access it.
        Takes a column or group of columns, finds the unique combination of values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        groupby_columns : str
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
        * The column names created by aggregation functions in the new frame are the original column name appended with the '_' character
          and the aggregation function.
          For example, if the original field is 'a' and the function is 'avg', the resultant column is named 'a_avg'.
        * An aggregation argument of 'count' results in a column named 'count'.

        Examples
        --------
        For setup, we will use a BigFrame *my_frame* accessing a frame with a column *a*::

            my_frame.inspect()

             a str
            |-----|
             cat
             apple
             bat
             cat
             bat
             cat

        Create a new frame, combining similar values of column *a*, and count how many of each value is in the original frame::

            new_frame = my_frame.groupBy('a', count)
            new_frame.inspect()

             a str       count int
            |---------------------|
             cat             3
             apple           1
             bat             2

        In this example, 'my_frame' is accessing a frame with three columns, *a*, *b*, and *c*::

            my_frame.inspect()

             a int   b str       c float
            |---------------------------|
             1       alpha     3.0
             1       bravo     5.0
             1       alpha     5.0
             2       bravo     8.0
             2       bravo    12.0

        Create a new frame from this data, grouping the rows by unique combinations of column *a* and *b*;
        average the value in *c* for each group::

            new_frame = my_frame.groupBy(['a', 'b'], {'c' : avg})
            new_frame.inspect()

             a int   b str   c_avg float
            |---------------------------|
             1       alpha     4.0
             1       bravo     5.0
             2       bravo    10.0

        For this example, we use *my_frame* with columns *a*, *c*, *d*, and *e*::

            my_frame.inspect()

             a str   c int   d float e int
            |-----------------------------|
             ape     1     4.0       9
             ape     1     8.0       8
             big     1     5.0       7
             big     1     6.0       6
             big     1     8.0       5

        Create a new frame from this data, grouping the rows by unique combinations of column *a* and *c*;
        count each group; for column *d* calculate the average, sum and minimum value; for column *e*, save the maximum value::

            new_frame = my_frame.groupBy(['a', 'c'], agg.count, {'d': [agg.avg, agg.sum, agg.min], 'e': agg.max})

             a str   c int   count int  d_avg float  d_sum float     d_min float e_max int
            |-----------------------------------------------------------------------------|
             ape     1           2        6.0         12.0             4.0           9
             big     1           3        6.333333    19.0             5.0           7

        For further examples, see :ref:`example_frame.groupby`.

        .. versionadded:: 0.8

        """
        try:
            return self._backend.groupby(self, groupby_columns, aggregation_arguments)
        except:
            raise IaError(logger)


    def inspect(self, n=10, offset=0):
        """
        Print data.

        Print the frame data in readable format.

        Parameters
        ----------
        n : int
            The number of rows to print
        offset : int
            The number of rows to skip before printing
            
        Returns
        -------
        data
            Formatted for ease of human inspection
            
        Examples
        --------
        For an example, see :ref:`example_frame.inspect`

        .. versionadded:: 0.8

        """
        # TODO - Review docstring
        try:
            return self._backend.inspect(self, n, offset)
        except:
            raise IaError(logger)

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

            joined_frame = your_frame.join(your_frame, left_on='b', right_on='book', how='inner')

        We end up with a new BigFrame *joined_frame* accessing a new frame with all the original columns, but only those rows where the data in the
        original frame in column *b* matched the data in column *book*.

        For further examples, see :ref:`example_frame.join`.

        .. versionadded:: 0.8

        """
        try:
            return self._backend.join(self, right, left_on, right_on, how)
        except:
            raise IaError(logger)

    def precision(self, label_column, pred_column, pos_label=1):
        """
        Model precision.

        Computes the precision measure for a classification model
        A column containing the correct labels for each instance and a column containing the predictions made by the
        model are specified.  The precision of a binary classification model is the proportion of predicted positive
        instances that are correct.  If we let :math:`T_{P}` denote the number of true positives and :math:`F_{P}` denote the number of false
        positives, then the model precision is given by: :math:`\\frac {T_{P}} {T_{P} + F_{P}}`.

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
        Consider the following sample data set in *frame* with actual data labels specified in the *labels* column and
        the predicted labels in the *predictions* column::

            frame.inspect()

              a:unicode   b:int32   labels:int32  predictions:int32
            |-------------------------------------------------------|
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
        return self._backend.classification_metric(self, 'precision', label_column, pred_column, pos_label, 1)


    def project_columns(self, column_names, new_names=None):
        """
        Create frame from columns.

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

        Returns
        -------
        BigFrame
            A new frame object accessing a new frame containing copies of the specified columns

        Examples
        --------
        Given a BigFrame *my_frame*, accessing a frame with columns named *a*, *b*, *c*, *d*.
        Create a new frame with three columns *apple*, *boat*, and *frog*, where for each row of the original frame, the data from column *a* is copied
        to the new column *apple*, the data from column *b* is copied to the column *boat*, and the data from column *c* is copied
        to the column *frog*::

            new_frame = my_frame.project_columns( ['a', 'b', 'c'], ['apple', 'boat', 'frog'])

        And the result is a new BigFrame named 'new_name' accessing a new frame with columns *apple*, *boat*, *frog*, and the data from *my_frame*,
        column *a* is now copied in column *apple*, the data from column *b* is now copied in column *boat* and the data from column *c* is now
        copied in column *frog*.

        Continuing::

            frog_frame = new_frame.project_columns('frog')

        And the new BigFrame *frog_frame* is accessing a frame with a single column *frog* which has a copy of all the data from the original
        column *c* in *my_frame*.

        .. versionadded:: 0.8

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
            |-------------------------------------------------------|
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
        return self._backend.classification_metric(self, 'recall', label_column, pred_column, pos_label, 1)

    @doc_stub
    def remove_columns(self, name):
        """
        Delete columns.

        Remove columns from the BigFrame object.

        Parameters
        ----------
        name : str OR list of str
            column name OR list of column names to be removed from the frame

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

        .. versionadded:: 0.8

        """
        pass
        # TODO - Review examples
        #try:
        #    self._backend.remove_columns(self, name)
        #except:
        #    raise IaError(logger)

    def rename_columns(self, column_names, new_names):
        """
        Rename column.

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

            my_frame.rename_columns(["Wrong", "Wong"], ["Right", "Wite"])

        Now, what was *Wrong* is now *Right* and what was *Wong* is now *Wite*.

        For further examples, see :ref:`example_frame.rename_columns`

        .. versionadded:: 0.8

        """
        try:
            self._backend.rename_columns(self, column_names, new_names)
        except:
            raise IaError(logger)

    def take(self, n, offset=0):
        """
        Get data subset.

        Take a subset of the currently active BigFrame.

        Parameters
        ----------
        n : int
            The number of rows to copy from the currently active BigFrame
        offset : int
            The number of rows to skip before copying

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain number of rows, the rows obtained may be different every time the
        command is executed, even if the parameters do not change.

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

        We end up with a new frame accessed by the BigFrame *your_frame* again, but this time it has a copy of rows 1001 to 5000 of the original frame.

        .. versionadded:: 0.8

        """
        # TODO - Review and complete docstring
        try:
            result = self._backend.take(self, n, offset)
            return result.data
        except:
            raise IaError(logger)


