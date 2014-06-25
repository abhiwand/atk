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

import uuid, sys
logger = logging.getLogger(__name__)

from intelanalytics.core.types import supported_types
from intelanalytics.core.aggregation import *
from intelanalytics.core.errorhandle import IaError

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

class BigFrame(object):
    """
    Proxy for a large 2D container to work with table data at scale.

    Parameters
    ----------
    source : source (optional)
        A source of initial data, like a CsvFile or another BigFrame
    name : string (optional)
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
        The type of object this is

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
        The name of the BigFrame

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
            The source of the data being added.

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
        Creates a full copy of the current frame

        Returns
        -------
        frame : BigFrame
            A new frame object which is a copy of this frame
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
            The number of rows

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
        # For this example, my_frame is a BigFrame object with lots of data and columns for the attributes of animals.
        # We do not want all this data, just the data for lizards and frogs, so ...
        >>> my_frame.filter(lambda row: row.animal_type == "lizard" or row.animal_type == "frog")
        # my_frame now only has data about lizards and frogs
        """
        # TODO - Review docstring
        try:
            self._backend.filter(self, predicate)
        except:
            raise IaError(logger)

    def flatten_column(self, column_name):
        """
        Flatten a column

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
        >>> flattened_frame = frame1.flatten_column('a')
        """
        try:
            return self._backend.flatten_column(self, column_name)
        except:
            raise IaError(logger)

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
        For this example, my_frame is a BigFrame object with a column called "unimportant" (amongst other)
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
        Remove duplicates that have whole row data identical
        <BLANKLINE
        >>> my_frame.drop_duplicates()
        """
        try:
            self._backend.drop_duplicates(self, columns)
        except:
            raise IaError(logger)

    def inspect(self, n=10, offset=0):
        """
        Check the data for validity.

        Parameters
        ----------

        n : int
            The number of something

        offset : int
            The number of something else

        Returns
        -------

        bool
            Whether the data is valid or not.

        Examples
        --------

        >>>
        Let us say that my_frame is a BigFrame object and the row should have types int32, str, int64, bool and the data for that row is "10", "20", "Bob's your uncle", "0"
        >>> my_check = my_frame.inspect()
        my_check would be false because "Bob's your uncle" is not an int64 type

        """
        # TODO - Review docstring
        try:
            return self._backend.inspect(self, n, offset)
        except:
            raise IaError(logger)

    def join(self, right, left_on, right_on=None, how='inner'):
        """
        Create a new BigFrame from a JOIN operation with another BigFrame.

        Parameters
        ----------
        right : BigFrame
            Another frame to join with
        left_on : str
            Name of the column for the join in this (left) frame
        right_on : str, optional
            Name of the column for the join in the right frame.  If not
            provided, then the value of left_on is used.
        how : str, optional
            {'left', 'right', 'inner'}

        Returns
        -------
        frame : BigFrame
            The new joined frame

        Examples
        --------
        >>> joined_frame = frame1.join(frame2, 'a')
        >>> joined_frame = frame2.join(frame2, left_on='b', right_on='book', how='inner')
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
            column name OR list of column names to be removed from the frame
        new_names : str OR list of str
            The new name(s) for the column(s)

        Returns
        -------

        frame : BigFrame
            A new frame object containing copies of the specified columns

        Examples
        --------

        >>>

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
        Group frame as per the criteria provided and compute aggregation on each group

        Parameters
        ----------
        groupby_columns: column name or list of column names (or function TODO)
            columns or result of a function will be used to create grouping
        aggregation: one or more aggregation functions or dictionaries of
            (column,aggregation function) pairs

        Return
        ------
        frame: BigFrame
            new aggregated frame

        Examples
        --------
        frame.groupby(frame.a, count)
        frame.groupby([frame.a, frame.b], {f.c: avg})
        frame.groupby(frame[['a', 'c']], count, {f.d: [avg, sum, min], f.e: [max]})
        """

        try:
            return self._backend.groupby(self, groupby_columns, aggregation_arguments)
        except:
            raise IaError(logger)

    def remove_columns(self, name):
        """
        Remove columns from the BigFrame object.

        Parameters
        ----------

        name : str OR list of str
            column name OR list of column names to be removed from the frame

        Notes
        -----

        Deleting a non-existant column raises a KeyError.
        Deleting the last column in a frame leaves the frame empty.

        Examples
        --------

        >>>
        # For this example, my_frame is a BigFrame object with columns titled "column_a", "column_b", column_c and "column_d".
        >>> my_frame.remove_columns([ column_b, column_d ])
        # Now my_frame only has the columns named "column_a" and "column_c"

        """
        # TODO - Review examples
        try:
            self._backend.remove_columns(self, name)
        except:
            raise IaError(logger)

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
        >>> frame.rename_columns( [ "Wrong", "Wong" ], [ "Right", "Wite" ] )
        # now, what was Wrong is now Right and what was Wong is now Wite
        """
        try:
            self._backend.rename_columns(self, column_names, new_names)
        except:
            raise IaError(logger)

    def take(self, n, offset=0):
        """
        .. TODO:: Add Docstring

        Parameters
        ----------

        n : int
            ?
        offset : int (optional)
            ?

        Examples
        --------

        >>> my_frame = BigFrame( p_raw_data, "my_data" )
        >>> r = my_frame.take( 5000 )

        """
        # TODO - Review and complete docstring
        try:
            return self._backend.take(self, n, offset)
        except:
            raise IaError(logger)
