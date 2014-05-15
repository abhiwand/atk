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
BigFrame object
"""
from collections import OrderedDict
import json

from intelanalytics.core.types import supported_types

import logging
logger = logging.getLogger(__name__)
import uuid


def _get_backend():
    from intelanalytics.core.config import get_frame_backend
    return get_frame_backend()


def get_frame_names():
    """
    Gets the names of BigFrame objects available for retrieval

    Parameters
    ----------

    Returns
    -------
    String : Name
        Name of the active BigFrame object

    Examples
    --------
    >>>

    """
    return _get_backend().get_frame_names()


def get_frame(name):
    """
    Retrieves the named BigFrame object
    
    Parameter
    ---------
    name : str
        String containing the name of the BigFrame object

    Returns
    -------
    Frame : BigFrame
        Named object

    Examples
    --------
    >>> 

    """
    return _get_backend().get_frame(name)


def delete_frame(name):
    """
    Deletes the frame from backing store
    
    Parameters
    ----------
    name
    
    Returns
    -------
    String : str
        The name of the deleted frame

    Examples
    --------
    >>> 
    """
    return _get_backend().delete_frame(name)


class BigFrame(object):
    """
    Proxy for a large 2D container to work with table data at scale.
    """

    def __init__(self, source=None, name=None):
        self._columns = OrderedDict()  # self._columns must be the first attribute to be assigned (see __setattr__)
        self._id = 0
        self._uri = ""
        if not hasattr(self, '_backend'):  # if a subclass has not already set the _backend
            self._backend = _get_backend()
        self._name = name or self._get_new_frame_name(source)

        # TODO: remove this schema hack for frame creation w/ current REST API
        self._original_source = source  # hold on to original source,

        self._backend.create(self)
        if source:
            self.append(source)
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

    # We are not defining __delitem__.  Columns must be deleted w/ remove_column

    def __repr__(self):
        return json.dumps({'_id': str(self._id),
                           'name': self.name,
                           'schema': repr(self.schema)})

    def __len__(self):
        return len(self._columns)

    def __contains__(self, key):
        return self._columns.__contains__(key)

    @staticmethod
    def _get_new_frame_name(source=None):
        try:
            annotation = "_" + source.annotation
        except:
            annotation = ''
        return "frame_" + uuid.uuid4().hex + annotation

    def _validate_key(self, key):
        if key in dir(self) and key not in self._columns:
            raise KeyError("Invalid column name '%s'" % key)

    class _FrameIter(object):
        """iterator for BigFrame - frame iteration works on the columns"""

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
        return self._columns.keys()

    @property
    def data_type(self):
        return type(self)

    #@property
    #def frame_id(self):
    #    return self._id

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return FrameSchema(zip(self._columns.keys(),
                               map(lambda c: c.data_type, self._columns.values())))

    @property
    def uri(self):
        return self._uri

    def _as_json_obj(self):
        return self._backend._as_json_obj(self)
        #return ['frame', {"name": self.name}]


    def add_column(self, func, type=str, name=""):
        """
        Adds a new column to the frame by evaluating the given func on each row

        Parameters
        ----------
        func: row function
            function which takes a single row and produces a value for the new cell

        type: data type
            specifies the type of the new column

        name: string
            specifies the name of the new column
        """
        # Generate a synthetic name
        if not name:
            for i in range(0,1000):
                if self._columns.get('res%d' % i, None) is None:
                    name = 'res%d' % i
                    break
        self._backend.add_column(self, func, name, type)

    def add_columns(self, func, names=None, ):
        """
        Adds new columns to the frame by evaluating the given func on each row

        Parameters
        ----------
        func: row function
            function which takes a single row and produces a tuple of new cell
            values

        names: list or tuple of strings or tuples of string, data type
            specifies the name and data type of the new columns
        """
        self._backend.add_columns(self, func, names)

    def append(self, *data):
        pass

    def filter(self, predicate):
        """
        Select all rows which satisfy a predicate

        Parameters
        ----------
        predicate : function
            A function which determines which rows are to be included

        Returns
        -------
        
        Examples
        --------
        >>> myframe.filter(lambda row: row.col1 + row.col2 > row.col3)
        >>> def custom_filter(row):
        >>>     return row['a'] * row['a'] > 30
        >>> myfram.filter(custom_filter)
        """
        self._backend.filter(self, predicate)

    def count(self):
        pass

    def remove_column(self, name):
        """
        Remove columns of data

        Parameters
        ----------
        name : str OR list of str
            column name OR list of column names to be removed from the frame

        Notes
        -----
        This function will retain a single column.

        Examples
        --------
        >>> 
        """
        self._backend.remove_column(self, name)
        if isinstance(name, basestring):
            name = [name]
        for victim in name:
            del self._columns[victim]

    def drop(self, predicate):
        pass

    def dropna(self, how=any, column_subset=None):
        """
        Drops all rows which have NA values

        Parameter
        ---------
        how : any (optional), all (optional)
            any: if any column has an NA value, drop row
            all: if all the columns have an NA value, drop row
            column name: if named column has an NA value, drop row
        column_subset : str OR list of str (optional)
            if not "None", only the given columns are considered

        Examples
        --------
        >>> 
        """
        self._backend.dropna(self, how, column_subset)

    def inspect(self, n=10, offset=0):
        return self._backend.inspect(self, n, offset)

    # def join(self,
    #          right=None,
    #          how='left',
    #          left_on=None,
    #          right_on=None,
    #          suffixes=None):
    #     """
    #     Perform SQL JOIN on BigDataFrame
    #
    #     Syntax is similar to pandas DataFrame.join.
    #
    #     Parameters
    #     ----------
    #     right   : BigDataFrame or list/tuple of BigDataFrame
    #         Frames to be joined with
    #     how     : Str
    #         {'left', 'right', 'outer', 'inner'}, default 'inner'
    #     left_on : Str
    #         Columns selected to bed joined on from left frame
    #     right_on: Str or list/tuple of Str
    #         Columns selected to bed joined on from right frame(s)
    #     suffixes: tuple of Str
    #         Suffixes to apply to columns on the output frame
    #
    #     Returns
    #     -------
    #     joined : BigFrame
    #         new BigFrame result
    #     """
    #     if not right:
    #         raise ValueError("A value for right must be specified")
    #     return operations.BigOperationBinary("join", {BigFrame: {bool: None}}, self, predicate)

    def add_column(self, column_name, func):
        """
        Add a column to a frame

        Parameters
        ----------
        column_name : str
            The name of the new column. Must not already exist.
        func : blob
            A blob defining what type of data the column will hold

        Returns
        -------
        
        Examples
        --------
        >>>
        """
        return self._backend.add_column(self, column_name, func)


    def rename_column(self, column_name, new_name):
        """
        Rename a column to a frame

        Parameters
        ----------
        column_name : str
            The name of the existing column.
        new_name : str
            The new name for the column. Must not already exist.

        Returns
        -------
        
        Examples
        --------
        >>>
        """
        if isinstance(column_name, basestring) and isinstance(new_name, basestring):
            column_name = [column_name]
            new_name = [new_name]
        if len(column_name) != len(new_name):
            raise ValueError("rename requires name lists of equal length")
        current_names = self._columns.keys()
        for nn in new_name:
            if nn in current_names:
                raise ValueError("Cannot use rename to '{0}' because another column already exists with that name".format(nn))
        name_pairs = zip(column_name, new_name)

        self._backend.rename_columns(self, name_pairs)
        # rename on python side, here in the frame's local columns:
        values = self._columns.values()  # must preserve order in OrderedDict
        for p in name_pairs:
            self._columns[p[0]].name = p[1]
        self._columns = OrderedDict([(v.name, v) for v in values])

    def save(self, name=None):
        """
        Saves all current data in the frame to disk

        Parameters
        ----------
        name : str (optional)
            The name of a new file where the frame will be saved

        Examples
        --------
        >>>
        """
        self._backend.save(self, name)

    def take(self, n, offset=0):
        """
        ?

        Parameters
        ----------
        n : int
            ?
        offset : int (optional)
            ?

        Examples
        --------
        >>>
        """
        return self._backend.take(self, n, offset)


class FrameSchema(OrderedDict):
    """
    Ordered key-value pairs of column name -> data type
    """

    def __init__(self, source=None):
        super(FrameSchema, self).__init__()
        if isinstance(source, basestring):
            self._init_from_string(source)
        else:
            self._init_from_tuples(source)

    def __repr__(self,  _repr_running=None):
        return json.dumps(self._as_json_obj())

    def _as_json_obj(self):
        return zip(self.get_column_names(), self.get_column_data_type_strings())

    def _init_from_tuples(self, tuples):
        self.clear()
        for name, dtype in tuples:
            if isinstance(dtype, basestring):
                self[name] = supported_types.get_type_from_string(dtype)
            elif dtype not in supported_types:
                raise ValueError("Unsupported data type in schema " + str(dtype))
            else:
                self[name] = dtype

    def _init_from_string(self, schema_string):
        logger.debug("FrameSchema init from string: {0}".format(schema_string))
        self._init_from_tuples(json.loads(schema_string))

    def get_column_names(self):
        return self.keys()

    def get_column_data_types(self):
        return self.values()

    def get_column_data_type_strings(self):
        return map(lambda v: supported_types.get_type_string(v), self.values())

    def drop(self, victim_columns):
        """
        Get rid of particular columns

        Parameters
        ----------
        victim_columns : str OR list of str
            Name(s) of the columns to drop

        Examples
        --------
        >>> 
        """
        if isinstance(victim_columns, basestring):
            victim_columns = [victim_columns]
        for v in victim_columns:
            del self[v]

    def append(self, new_columns):
        """
        Add new columns

        Parameters
        ----------
        new_columns : structure
            The column(s) to add              Not optional
            .keys = str       
            .items = structure
            == int             The new column number
            == value           The new column value

        Examples
        --------
        >>> 

        Should the new column be named?
        """
        for f in new_columns.keys():
            if f in self:
                raise KeyError('Schema already contains column ' + f)
        for name, dtype in new_columns.items():
            self[name] = dtype

    def merge(self, schema):
        for name, dtype in schema.items():
            if name not in self:
                self[name] = dtype
            elif self[name] != dtype:
                raise ValueError('Schema merge collision: column being set to '
                                 'a different type')
