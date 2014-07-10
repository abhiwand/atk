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

from ordereddict import OrderedDict
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
    Gets the names of BigFrame objects available for retrieval.

    Returns
    -------
    list of strings
        Names of the all BigFrame objects

    Examples
    --------
    >>> get_frame_names()
    ["my_frame_1", "my_frame_2", "my_frame_3"] # a list of names of BigFrame objects
    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame_names()
    except:
        raise IaError(logger)

def get_frame(name):
    """
    Retrieves the named BigFrame object.

    Parameters
    ----------
    name : string
        String containing the name of the BigFrame object

    Returns
    -------
    BigFrame
        Named object

    Examples
    --------
    >>> my_frame = get_frame( "my_frame_1" )
    my_frame is now a proxy of the BigFrame object

    """
    # TODO - Review docstring
    try:
        return _get_backend().get_frame(name)
    except:
        raise IaError(logger)

def delete_frame(name):
    """
    Deletes the frame from backing store.

    Parameters
    ----------
    name : string
        The name of the BigFrame object to delete.

    Returns
    -------
    string
        The name of the deleted frame

    Examples
    --------
    >>> my_frame = BigFrame("raw_data.csv", my_csv)
    >>> deleted_frame = delete_frame( my_frame )
    deleted_frame is now a string with the value of the name of the frame which was deleted

    """
    # TODO - Review examples and parameter
    try:
        return _get_backend().delete_frame(name)
    except:
        raise IaError(logger)

class BigFrame(CommandSupport):
    """
    Proxy for a large 2D container to work with table data at scale.

    Parameters
    ----------
    source : source
        A source of initial data, like a CsvFile or another BigFrame
    name : string
        The name of the newly created BigFrame object

    Notes
    -----
    If no name is provided for the BigFrame object, it will generate one.
    If a data source *X* was specified, it will try to generate the frame name as *_X*.
    If for some reason *_X* would be illegal

    Examples
    --------
    >>> g = BigFrame(None, "my_data")
    A BigFrame object has been created and 'g' is its proxy. It has no data, but it has the name "my_data".

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
        The names of all the columns in the current BigFrame object.

        Returns
        -------
        list of string

        Examples
        --------
        >>> frame.column_names()
        ["col1", "col2", "col3"]

        """
        return self._columns.keys()

    @property
    def data_type(self):
        """
        The type of object this is.
        
        Returns
        -------
        type : type
            The type of object this is
        """
        # TODO - what to do about data_type on BigFrame?  is it really necessary?
        # TODO - Review Docstring
        return type(self)

    #@property
    # TODO - should we expose the frame ID publically?
    #def frame_id(self):
    #    return self._id

    @property
    def name(self):
        """
        The name of the BigFrame.
        
        Returns
        -------
        name : str
            The name of the BigFrame

        Examples
        --------
        >>> csv = CsvFile("my_data.csv", csv_schema)
        >>> frame = BigFrame(csv, "my_frame")
        >>> frame.name
        "my_frame"
        """
        return self._name

    @name.setter
    def name(self, value):
        try:
            self._backend.rename_frame(self, value)
            self._name = value  # TODO - update from backend
        except:
            raise IaError(logger)

    @property
    def schema(self):
        """
        The schema of the current object.

        Returns
        -------
        schema : list of tuples
            The schema of the BigFrame, which is a list of tuples which
            represents and the name and data type of frame's columns

        Examples
        --------
        >>> frame.schema
        [("col1", str), ("col1", numpy.int32)]
        """
        return [(col.name, col.data_type) for col in self._columns.values()]

    @property
    def uri(self):
        """
        The uniform resource identifier of the current object.

        Returns
        -------
        uri : str
            The value of the uri
        """
        return self._uri

    def _as_json_obj(self):
        return self._backend._as_json_obj(self)

    def add_columns(self, func, schema):
        """
        Add column(s).
        
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
        The row function ('func') must return a value in the same format as specified by the data type ('types').
        See examples below and glossary.
        
        Examples
        --------
        >>> my_frame = BigFrame(data)
        For this example my_frame is a BigFrame object with two int32 columns named "column1" and "column2".
        We want to add a third column named "column3" as an int32 and fill it with the contents of column1 and column2 multiplied together
        >>> my_frame.add_columns(lambda row: row.column1*row.column2, ('column3', int32))
        The variable my_frame now has three columns, named "column1", "column2" and "column3". The type of column3 is an int32, and the value is the product of column1 and column2.
        <BLANKLINE>
        Now, we want to add another column that is empty
        >>> my_frame.add_columns(lambda row: '', ('empty', str))
        The BigFrame object 'my_frame' now has four columns named "column1", "column2", "column3", and "new0". The first three columns are int32 and the fourth column is string.  Column "new0" has an empty string ('') in every row.
        >>> my_frame.add_columns(lambda row: (row.a * row.b, row.a + row.b), [("a_times_b", float32), ("a_plus_b", float32))
        # Two new columns are created, "a_times_b" and "a_plus_b"
        <BLANKLINE>
        Now, let us start with a BigFrame with some existing rows of data.
        >>> my_frame.add_columns( function_a, ("calculated_a", int))
        It is our responsibility to insure that function_a returns an int value.
        >>> my_columns = [("calculated_b", float), ("calculated_c", str)]
        >>> my_frame.add_columns(function_b, my_columns)
        In this case function_b would have to return an array of two elements so the program can figure out which result goes into which column.
        It is not obvious though that if you have an array with a single tuple describing the column, your function must provide the result as an array with a single value matching the column type.
        This would cause an error:
        >>> def function_c: return 12
        >>> my_frame.add_columns(function_c, [('column_C', int)])
        However, this would work fine:
        >>> def function_c: return [12]
        >>> my_frame.add_columns(function_c, [('column_C', int)])
        And these would work fine:
        >>> def function_c: return 12
        >>> my_frame.add_columns(function_c, ('column_C, int))
        >>> my_frame.add_columns(function_c, int, 'column_C')

        """
        try:
            self._backend.add_columns(self, func, schema)
        except:
            raise IaError(logger)

    def append(self, data):
        """
        Adds more data to the BigFrame object.

        Parameters
        ----------
            data : a schema describing the data being added

        Examples
        --------
        >>> my_frame = BigFrame(my_csv)
        >>> my_other_frame = BigFrame(my_other_csv)
        >>> my_frame.append(my_other_frame)
        # my_frame now has the data from my_other_frame as well
        """
        # TODO - Review examples
        try:
            self._backend.append(self, data)
        except:
            raise IaError(logger)

    def copy(self):
        """
        Creates a full copy of the current frame.

        Returns
        -------
        frame : BigFrame
            A new frame object which is a copy of the currently active BigFrame
        """
        try:
            copied_frame = BigFrame()
            self._backend.project_columns(self, copied_frame, self.column_names)
            return copied_frame
        except:
            raise IaError(logger)

    def count(self):
        """
        Count the number of rows that exist in this object.

        Returns
        -------
        int32
            The number of rows in the currently active BigFrame

        Examples
        --------
        >>>
        # For this example, my_frame is a BigFrame object with lots of data
        >>> num_rows = my_frame.count()
        # num_rows is now the count of rows of data in my_frame

        """
        try:
            return self._backend.count(self)
        except:
            raise IaError(logger)

    def drop(self, predicate):
        """
        Remove all rows from the frame which satisfy the predicate.

        Parameters
        ----------
        predicate : function
        function or lambda which takes a row argument and evaluates to a boolean value

        Examples
        --------
        >>>
        # For this example, my_frame is a BigFrame object with lots of data and columns for the attributes of animals.
        # We want to get rid of the lions and tigers
        >>> my_frame.drop(lambda row: row.animal_type == "lion" or row.animal_type == "tiger")
        """
        # TODO - Review docstring
        try:
            self._backend.drop(self, predicate)
        except:
            raise IaError(logger)

    def filter(self, predicate):
        """
        Select all rows which satisfy a predicate.

        Parameters
        ----------
        predicate: function
            function definition or lambda which takes a row argument and evaluates to a boolean value

        Examples
        --------
        >>>
        For this example, my_frame is a BigFrame object with lots of data and columns for the attributes of animals.
        We do not want all this data, just the data for lizards and frogs, so ...
        >>> my_frame.filter(animal_type == "lizard" or animal_type == "frog")
        BigFrame 'my_frame' now only has data about lizards and frogs

        """
        # TODO - Review docstring
        try:
            self._backend.filter(self, predicate)
        except:
            raise IaError(logger)

    def flatten_column(self, column_name):
        """
        Flatten a column.

        Search through the currently active BigFrame for multiple items in a single specified column.
        When it finds multiple values in the column, it replicates the row and separates the multiple items across the existing and new rows.
        Multiple items is defined in this case as being things separated by commas.

        Parameters
        ----------
        column_name : str
            The column to be flattened
                                                  n
        Returns
        -------
        frame : BigFrame
            The new flattened frame

        Examples
        --------
        >>>
        For this example, we have a BigFrame called 'frame1' with a column called 'a' and a column called 'b'.
        Row 1, column 'a' is "me", column 'b' is "x"
        Row 2, column 'a' is "oh, oh, you", column 'b' is "y"
        Row 3, column 'a' is "my", column 'b' is "z, e, r, o"
        <BLANKLINE>
        >>> flattened_frame = frame1.flatten_column('a')
        <BLANKLINE>
        Row 1, column 'a' is "me", column 'b' is "x"
        Row 2, column 'a' is "oh", column 'b' is "y"
        Row 3, column 'a' is "my", column 'b' is "z, e, r, o"
        Row 4, column 'a' is "oh", column 'b' is "y"
        Row 5, column 'a' is "you", column 'b' is "y"
        """
        try:
            return self._backend.flatten_column(self, column_name)
        except:
            raise IaError(logger)

    def bin_column(self, column_name, num_bins, bin_type='equalwidth', bin_column_name='binned'):
        """
        Bin column values into equal width or equal depth bins.

        The numBins parameter is an upper-bound on the number of bins since the data
        may justify fewer bins.  With equal depth binning, for example, if the column to be binned has 10 elements with
        only 2 distinct values and numBins > 2, then the number of actual bins will only be 2.  This is due to a
        restriction that elements with an identical value must belong to the same bin.

        Parameters
        ----------
        column_name : str
            The column whose values are to be binned
        num_bins : int
            The requested number of bins
        bin_type : 'equalwidth' or 'equaldepth', (optional, default 'equalwidth')
            The binning algorithm to use
        bin_column_name : str, (optional, default 'binned')
            The name for the new binned column

        Returns
        -------
        frame : BigFrame
            A new frame with binned column appended to original frame

        Examples
        --------
        >>> binnedEW = frame.bin_column('a', 5, 'equalwidth', 'aEWBinned')
        >>> binnedED = frame.bin_column('a', 5, 'equaldepth', 'aEDBinned')
        """
        return self._backend.bin_column(self, column_name, num_bins, bin_type, bin_column_name)

    @docstub
    def mean_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the mean value of a column.

        Parameters
        ----------
        column_name : str
            The column whose mean value is to be calculated

        Returns
        -------
        mean : Double
            The mean of the values in the column

        Examples
        --------
        >>> mean = frame.mean_column('interesting column')
        """
        pass

    def mode_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the mode of a column.

        Parameters
        ----------
        column_name : str
            The column whose mode is to be calculated

        Returns
        -------
        mode : Double
            The mode of the values in the column

        Examples
        --------
        >>> mode = frame.mode_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'MODE')

    def median_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the median of a column.

        Parameters
        ----------
        column_name : str
            The column whose median is to be calculated

        Returns
        -------
        median : Double
            The median of the values in the column

        Examples
        --------
        >>> median = frame.median_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'MEDIAN')

    def sum_column(self, column_name, multiplier_column_name = None):
            """
            Calculate the sum of a column.

            Parameters
            ----------
            column_name : str
                The column whose values are to be summed

            Returns
            -------
            mean : Double
                The sum of the values in the column

            Examples
            --------
            >>> sum = frame.sum_column('interesting column')
            """
            return self._backend.column_statistic(self, column_name, multiplier_column_name, 'SUM')

    def max_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the maximum value of a column.

        Parameters
        ----------
        column_name : str
            The column whose maximum is to be found

        Returns
        -------
        max : Double
            The maximum value of the column

        Examples
        --------
        >>> max = frame.max_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'MAX')

    def min_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the minimum value of a column.

        Parameters
        ----------
        column_name : str
            The column whose minimum is to be found

        Returns
        -------
        min : Double
            The minimum value of the column

        Examples
        --------
        >>> min = frame.min_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'MIN')

    def variance_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the variance of a column.

        Parameters
        ----------
        column_name : str
            The column whose variance is to be calculated

        Returns
        -------
        variance : Double
            The variance of the values in the column

        Examples
        --------
        >>> variance = frame.variance_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'VARIANCE')

    def stdev_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the standard deviation of a column.

        Parameters
        ----------
        column_name : str
            The column whose standard deviation is to be calculated

        Returns
        -------
        stdev : Double
            The standard deviation of the values in the column

        Examples
        --------
        >>> stdev = frame.stdev_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'STDEV')

    def geomean_column(self, column_name, multiplier_column_name = None):
        """
        Calculate the geometric mean of a column.

        Parameters
        ----------
        column_name : str
            The column whose geometric mean is to be calculated

        Returns
        -------
        geomean : Double
            The geometric mean of the values in the column

        Examples
        --------
        >>> geomean = frame.geomean_column('interesting column')
        """
        return self._backend.column_statistic(self, column_name, multiplier_column_name, 'GEOMEAN')

    def drop(self, predicate):
        """
        Drop rows that match a requirement.

        Parameters
        ----------
        predicate : function
            The requirement that the rows must match

        Examples
        --------
        >>>
        For this example, my_frame is a BigFrame object with a boolean column called "unimportant".
        >>> my_frame.drop( unimportant == True )
        my_frame's data is now empty of any data with where the column "unimportant" was true.

        """
        # TODO - review docstring
        try:
            self._backend.drop(self, predicate)
        except:
            raise IaError(logger)

    def drop_duplicates(self, columns=[]):
        """
        Remove duplicate rows, keeping only one row per uniqueness criteria match
    
        Parameters
        ----------
        columns : str OR list of str
            column name(s) to identify duplicates. If empty, will remove duplicates that have whole row data identical.
    
        Examples
        --------
        >>>
        Remove duplicate rows that have same data on column b.
        >>> my_frame.drop_duplicates("b")
        <BLANKLINE>
        Remove duplicate rows that have same data on column a and b
        >>> my_frame.drop_duplicates(["a", "b"])
        <BLANKLINE>
        Remove duplicates that have whole row data identical
        >>> my_frame.drop_duplicates()
        """
        try:
            self._backend.drop_duplicates(self, columns)
        except:
            raise IaError(logger)

    def inspect(self, n=10, offset=0):
        """
        Print the data in readable format.
        
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
        >>>
        For this example, my_frame is a BigFrame object with two columns 'a' and 'b'.
        Column 'a' is float32 and 'b' is int64.
        >>> print my_frame.inspect()
        Output would be something like:
        <BLANKLINE>
         a float32          b int64
        -----------------------------
           12.3000              500
          195.1230           183954
        
        """
        # TODO - Review docstring
        try:
            return self._backend.inspect(self, n, offset)
        except:
            raise IaError(logger)

    def join(self, right, left_on, right_on=None, how='inner'):
        """
        Build a new BigFrame from two others.

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
        right_on : str
            Name of the column in the right frame used to match up the two frames.
            If not provided, then the column name used must be the same in both frames.
        how : str
            {'left', 'right', 'inner'}

        Returns
        -------
        frame : BigFrame
            The new joined frame

        Examples
        --------
        >>>
        For this example, we will used a frame called 'frame1' with columns 'a', 'b', 'c', and a frame called 'frame2' with columns 'a', 'd', 'e'.
        >>> frame1 = BigFrame(schema1)
        ... frame2 = BigFrame(schema2)
        ... joined_frame = frame1.join(frame2, 'a')
        Now, joined_frame is a BigFrame with the columns 'a', 'b', 'c', 'd', 'e'. The data in the new frame will be from the rows where column 'a' was the same in both 'frame1' and 'frame2'.
        <BLANKLINE>
        Now, using a single BigFrame called 'frame2' with the columns 'b' and 'book':
        >>> joined_frame = frame2.join(frame2, left_on='b', right_on='book', how='inner')
        We end up with a new BigFrame with the columns 'b' and 'book', but only those rows where the data in the original frame in column 'b' matched the data in column 'book'.  Note that there are still the original number of columns, with the original column names, but any rows of data where 'b' and 'book' did not match are now gone.

        """
        try:
            return self._backend.join(self, right, left_on, right_on, how)
        except:
            raise IaError(logger)

    def project_columns(self, column_names, new_names=None):
        """
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

        Returns
        -------
        frame : BigFrame
            A new frame object containing copies of the specified columns

        Examples
        --------
        >>>
        For this example, our original BigFrame called 'frame1' has columns named 'a', 'b', 'c', 'd'.
        >>> new_frame = frame1.project_columns( ['a', 'b', 'c'], ['apple', 'boat', 'frog'])
        And the result is a new BigFrame named 'new_name' with columns 'apple', 'boat', 'frog', and the data from 'frame1', column 'a' is now copied in column 'apple', the data from column 'b' is now copied in column 'boat' and the data from column 'c' is now copied in column 'frog'.
        Continuing:
        >>> frog_frame = new_frame.project_columns('frog')
        And the new BigFrame called 'frog_frame' has a single column called 'frog' with a copy of all the data from the original column 'c' in 'frame1'.

        """
        # TODO - need example in docstring
        try:
            projected_frame = BigFrame()
            self._backend.project_columns(self, projected_frame, column_names, new_names)
            return projected_frame
        except:
            raise IaError(logger)

    def groupby(self, groupby_columns, *aggregation_arguments):
        """
        Create a new BigFrame using an existing frame, compressed by groups.
        
        Creates a new BigFrame.
        Takes a column or group of columns, finds the unique combination of values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        groupby_columns: column name or list of column names (or function TODO)
            columns or result of a function will be used to create grouping
        aggregation: one or more aggregation functions or dictionaries of
            (column,aggregation function) pairs

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
        >>>
        For this example, we will use a BigFrame called 'my_frame', with a column called 'a'.
        Column 'a' has the values 'cat', 'apple', 'bat', 'cat', 'bat', 'cat'.
        >>> column_a = my_frame.a
        >>> new_frame = my_frame.groupBy(column_a, count)
        The new BigFrame 'new_frame' has two columns named 'a' and 'count'.
        In a row of 'new_frame', column 'a' is 'apple' and column 'count' is 1.
        In another row of 'new_frame', column 'a' is 'bat' and column 'count' is 2.
        In another row of 'new_frame', column 'a' is 'cat' and column 'count' is 3.
        <BLANKLINE>
        In this example, 'my_frame' has three columns, named 'a', 'b', and 'c'. The data in these columns is:
        'a' is 1, 'b' is 'alpha', 'c' is 3.0
        'a' is 1, 'b' is 'bravo', 'c' is 5.0
        'a' is 1, 'b' is 'alpha', 'c' is 5.0
        'a' is 2, 'b' is 'bravo', 'c' is 8.0
        'a' is 2, 'b' is 'bravo', 'c' is 12.0
        >>> column_a = my_frame.a
        >>> column_b = my_frame.b
        >>> column_c = my_frame.c
        >>> new_frame = my_frame.groupBy([column_a, column_b], {column_c: avg})
        The new BigFrame 'new_frame' has three columns named 'a', 'b', and 'c_avg'. The data is:
        'a' is 1, 'b' is 'alpha', 'c_avg' is 4.0
        'a' is 1, 'b' is 'bravo', 'c_avg' is 5.0
        'a' is 2, 'b' is 'bravo', 'c_avg' is 10.0
        <BLANKLINE>
        For this example, we use 'my_frame' with columns 'a', 'c', 'd', and 'e'. Column types will be str, int, float, int. The data is:
        'a' is 'ape', 'c' is 1, 'd' is 4.0, 'e' is 9
        'a' is 'ape', 'c' is 1, 'd' is 8.0, 'e' is 8
        'a' is 'big', 'c' is 1, 'd' is 5.0, 'e' is 7
        'a' is 'big', 'c' is 1, 'd' is 6.0, 'e' is 6
        'a' is 'big', 'c' is 1, 'd' is 8.0, 'e' is 5
        >>> column_d = my_frame.d
        >>> column_e = my_frame.e
        >>> new_frame = my_frame.groupBy(my_frame[['a', 'c']], count, {column_d: [avg, sum, min], column_e: [max]})
        The new BigFrame 'new_frame' has columns 'a', 'c', 'count', 'd_avg', 'd_sum', 'd_min', 'e_max'.
        The column types are (respectively): str, int, int, float, float, float, int.
        The data is:
        'a' is 'ape', 'c' is 1, 'count' is 2, 'd_avg' is 6.0, 'd_sum' is 12.0, 'd_min' is 4.0, 'e_max' is 9
        'a' is 'big', 'c' is 1, 'count' is 3, 'd_avg' is 6.333333, 'd_sum' is 19.0, 'd_min' is 5.0, 'e_max' is 7


        """

        try:
            return self._backend.groupby(self, groupby_columns, aggregation_arguments)
        except:
            raise IaError(logger)

    @doc_stub
    def remove_columns(self, name):
        """
        Remove columns from the BigFrame object.

        Parameters
        ----------
        name : str OR list of str
            column name OR list of column names to be removed from the frame

        Notes
        -----
        * Deleting a non-existant column raises a KeyError.
        * Deleting the last column in a frame leaves the frame empty.

        Examples
        --------
        >>>
        # For this example, my_frame is a BigFrame object with columns titled "column_a", "column_b", column_c and "column_d".
        >>> my_frame.remove_columns([ column_b, column_d ])
        # Now my_frame only has the columns named "column_a" and "column_c"

        """
        pass
        # TODO - Review examples
        #try:
        #    self._backend.remove_columns(self, name)
        #except:
        #    raise IaError(logger)

    def rename_columns(self, column_names, new_names):
        """
        Renames columns in a frame.

        Parameters
        ----------
        column_names : str or list of str
            The name(s) of the existing column(s).
        new_names : str
            The new name(s) for the column(s). Must not already exist.

        Examples
        --------
        If we start with a BigFrame with columns named "Wrong" and "Wong"
        >>> frame.rename_columns( [ "Wrong", "Wong" ], [ "Right", "Wite" ] )
        # now, what was Wrong is now Right and what was Wong is now Wite
        """
        try:
            self._backend.rename_columns(self, column_names, new_names)
        except:
            raise IaError(logger)

    def take(self, n, offset=0):
        """
        Take a subset of the currently active BigFrame.

        Parameters
        ----------
        n : int
            The number of rows to copy from the currently active BigFrame
        offset : int
            The number of rows to skip before copying

        Notes
        -----
        The data is considered 'unstructured', therefore taking a certain number of rows, the rows obtained 

        Examples
        --------
        >>>
        Let's say we have a frame called 'my_frame' with millions of rows of data
        >>> r = my_frame.take( 5000 )
        We now have a separate BigFrame called 'r' with a copy of the first 5000 rows of 'my_frame'
        <BLANKLINE>
        If we use the function like:
        >>> r = my_frame.take( 5000, 1000 )
        We end up with the BigFrame called 'r' again, but this time it has a copy of rows 1001 to 5000 of 'my_frame'
        
        """
        # TODO - Review and complete docstring
        try:
            return self._backend.take(self, n, offset)
        except:
            raise IaError(logger)
