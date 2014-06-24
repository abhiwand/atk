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

from collections import OrderedDict
import json


import logging
logger = logging.getLogger(__name__)

from intelanalytics.core.types import supported_types
from intelanalytics.core.aggregation import *


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
    return _get_backend().get_frame_names()


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
    return _get_backend().get_frame(name)


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
    return _get_backend().delete_frame(name)


class BigFrame(object):
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
        self._columns = OrderedDict()  # self._columns must be the first attribute to be assigned (see __setattr__)
        self._id = 0
        self._uri = ""
        self._name = ""
        if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
            self._backend = _get_backend()
        self._backend.create(self, source, name)
        logger.info('Created new frame "%s"', self._name)

    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name != "_columns" and name in self._columns:
            return self[name]
        return super(BigFrame, self).__getattribute__(name)

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
        return json.dumps({'uri': self.uri,
                           'name': self.name,
                           'schema': self._schema_as_json_obj()}, indent=2, separators=(', ', ': '))

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
        self._backend.rename_frame(self, value)
        self._name = value  # TODO - update from backend

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
        #return ['frame', {"name": self.name}]

    def add_columns(self, func, types=str, names=""):
        """
        Add column(s).
        
        Adds one or more new columns to the frame by evaluating the given
        func on each row.

        Parameters
        ----------
        func : row function
            function which takes the values in the row and produces a value
            or collection of values for the new cell(s)

        types : data type or list/tuple of types
            specifies the type(s) of the new column(s)

        names : string  or list/tuple of strings
            specifies the name(s) of the new column(s).  By default, the new
            column(s) name will be given a unique name "new#" where # is the
            lowest number (starting from 0) such that there is not already a
            column with that name.

        Notes
        -----
        The row function ('func') must return a value in the same format as specified by the data type ('types').
        See examples below and glossary.
        
        Examples
        --------
        >>> my_frame = BigFrame(data)
        For this example my_frame is a BigFrame object with two int32 columns named "column1" and "column2".
        We want to add a third column named "column3" as an int32 and fill it with the contents of column1 and column2 multiplied together
        >>> my_frame.add_columns(lambda row: row.column1*row.column2, int32, "column3")
        The variable my_frame now has three columns, named "column1", "column2" and "column3". The type of column3 is an int32, and the value is the product of column1 and column2.
        <BLANKLINE>
        Now, we want to add another column, we don't care what it is called and it is going to be an empty string (the default).
        >>> my_frame.add_columns(lambda row: '')
        The BigFrame object 'my_frame' now has four columns named "column1", "column2", "column3", and "new0". The first three columns are int32 and the fourth column is string.  Column "new0" has an empty string ('') in every row.
        >>> frame.add_columns(lambda row: (row.a * row.b, row.a + row.b), (float32, float32), ("a_times_b", "a_plus_b"))
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
        self._backend.add_columns(self, func, names, types)

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
        self._backend.append(self, data)

    def copy(self):
        """
        Creates a full copy of the current frame.

        Returns
        -------
        frame : BigFrame
            A new frame object which is a copy of the currently active BigFrame
        """
        copied_frame = BigFrame()
        self._backend.project_columns(self, copied_frame, self.column_names)
        return copied_frame

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
        return self._backend.count(self)

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
        self._backend.filter(self, predicate)

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
        # TODO - Review docstring
        return self._backend.flatten_column(self, column_name)

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
        self._backend.drop(self, predicate)

    def dropna(self, how=any, column_subset=None):
        """
        Drops all rows which have NA values.

        Parameters
        ----------
        how : any, all, or column name
            any: if any column has an NA value, drop row
            all: if all the columns have an NA value, drop row
            column name: if named column has an NA value, drop row


        column_subset : str OR list of str
            if not "None", only the given columns are considered

        Examples
        --------
        >>>
        For this example, my_frame is a BigFrame object with a columns called "column_1", "column_2", and "column_3" (amongst others)
        >>> my_frame.dropna( "column_1" )
        will eliminate any rows which do not have a value for column_1
        <BLANKLINE>
        If we used the form
        >>> my_frame.dropna( any, ["column_2", "column_3"])
        This erased any line that has no data for column_2 OR column_3
        <BLANKLINE>
        If we used the form
        >>> my_frame.dropna( all, ["column_1", "column_2", "column_3"])
        This erased any rows that had no data for column_1, column_2 AND column_3.
        
        """
        # TODO - Review examples
        self._backend.dropna(self, how, column_subset)

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
        return self._backend.inspect(self, n, offset)

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
        return self._backend.join(self, right, left_on, right_on, how)

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

        if isinstance(column_names, basestring):
            column_names = [column_names]
        if new_names is not None:
            if isinstance(new_names, basestring):
                new_names = [new_names]
            if len(column_names) != len(new_names):
                raise ValueError("new_names list argument must be the same length as the column_names")
        # TODO - create a general method to validate lists of column names, such that they exist, are all from the same frame, and not duplicated
        projected_frame = BigFrame()
        self._backend.project_columns(self, projected_frame, column_names, new_names)
        return projected_frame

    def groupBy(self, group_by_columns, *aggregation_arguments):
        """
        Create a new BigFrame using an existing frame, compressed by groups.
        
        Creates a new BigFrame.
        Takes a column or group of columns, finds the unique combination of values, and creates unique rows with these column values.
        The other columns are combined according to the aggregation argument(s).

        Parameters
        ----------
        group_by_columns: BigColumn or List of BigColumns or function
            columns, or virtual columns created by a function, will be used to create grouping
        aggregation_arguments: column and (count or dict)
            (column, aggregation function(s)) pairs

        Return
        ------
        frame: BigFrame
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

        # This section creates a list comprised only of the names of the columns used to group the data.
        groupByColumns = []
        if isinstance(group_by_columns, list):
            groupByColumns = [i.name for i in group_by_columns]
        elif group_by_columns:
            groupByColumns = [group_by_columns.name]

        # The primary column name will be the first column name passed on the command line if it exists,
        # otherwise it will be the first column name of the currently active BigFrame
        primaryColumn = groupByColumns[0] if groupByColumns else self.column_names[0]
        aggregation_list = []

        # Go through each of the aggregation arguments individually
        for i in aggregation_arguments:
            if i == count:
                aggregation_list.append((count, primaryColumn, "count"))
            else:
                for k,v in i.iteritems():
                    if isinstance(v, list):
                        for j in v:
                            aggregation_list.append((j, k.name, "%s_%s" % (k.name, j)))
                    else:
                        aggregation_list.append((v, k.name, "%s_%s" % (k.name, v)))

        # Send to backend to compute aggregation
        return self._backend.groupBy(self, groupByColumns, aggregation_list)

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
        # TODO - Review examples
        self._backend.remove_columns(self, name)

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
        self._backend.rename_columns(self, column_names, new_names)

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
        return self._backend.take(self, n, offset)



