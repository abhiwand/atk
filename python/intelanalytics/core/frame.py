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

    def __init__(self, source=None):
        self._columns = OrderedDict()  # self._columns must be the first attribute to be assigned (see __setattr__)
        self._id = 0
        if not hasattr(self, '_backend'):
            self._backend = _get_backend()
        self._name = self._get_new_frame_name(source)
        self._original_source = source  # hack, hold on to original source
        self._backend.create(self)
        logger.info('Created new frame "%s"', self._name)
        if source is not None:
            self.append(source)

    def __getattr__(self, name):
        """After regular attribute access, try looking up the name of a column.
        This allows simpler access to columns for interactive use."""
        if name != "_columns" and name in self._columns:
            return self[name]
        return super(BigFrame, self).__getattribute__(name)

    # def __setattr__(self, name, value):
    #     """After regular attribute access, try looking up the name of a column.
    #     This allows simpler access to columns for interactive use."""
    #     if name != "_columns" and name in self._columns or (name not in dir(self)
    #                                                         and isinstance(value, operations.BigExpression)):
    #         self.__setitem__(name, value)
    #     else:
    #         super(BigFrame, self).__setattr__(name, value)

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Slicing not supported")
            if isinstance(key, list):
                return [self._columns[k] for k in key]
            return self._columns[key]
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

#     def __setitem__(self, key, value):
#         #print "log: {0}.__setitem__({1},{2})".format(self.name,key, value)
#         if value is None:
#             raise Exception("cannot set column to None")
#         # ====================================================================
#         #  value \ key        |    str          |   list of str
#         # ====================================================================
#         # BigColumn           | A0:copy_column  | B0:TypeError
#         # --------------------+-----------------+-----------------------------
#         # list of Big Column  | A1:TypeError    | B1:copy_columns, if same len
#         # --------------------+-----------------+-----------------------------
#         # BigOperation        | A2:assign_column| B2:TypeError*
#         # --------------------+-----------------+-----------------------------
#         # list of BigOperation| A3:TypeError    | B3:NotImplementedError
#         #                     |                 | assign_columns, if same len
#         #                     |                 | (low ROI, + Views will
#         #                     |                 | alleviate desire)
#         # --------------------------------------------------------------------
#
#         if isinstance(key, slice):
#             if key.start is None and key.stop is None and key.step is None:
#                 self._assign_frame(value)
#             else:
#                 raise NotImplementedError("Slicing frame not supported, only [:]")
#         elif isinstance(key, list):
#             if not isinstance(value, list)\
#                     or len(key) != len(value) or\
#                     not all([isinstance(v, BigColumn) for v in value]):
#                 # B0, B2, B3
#                 raise TypeError("List of columns must be assigned from another "
#                                 "list of columns of the same length")
#             # B1
#             self._copy_columns(key, value)
# #        elif not isinstance(key, basestring):
# #            raise TypeError("Unsupported assignment key type "
# #                            + value.__class__.__name__)
#         elif isinstance(value, list):
#             # A1, A3
#             raise TypeError("List of values cannot be assigned to a single column")
#         elif isinstance(value, BigColumn):
#             # A0
#             self._copy_column(key, value)
#         elif isinstance(value, operations.BigOperation):
#             self._assign_column(key, value)
#         else:
#             raise TypeError("Unsupported assignment value type "
#                             + value.__class__.__name__)
#             # todo - consider supported immediate numbers, strings, lists?

    #def _attach_column_to_python_frame(self, key, column):
    #    column.frame = self
    #    self._columns[key] = column  # todo, improve Column creation and assignment to BigDF

    #def _copy_column(self, key, column):
    #    self._validate_key(key)
    #    self._backend.copy_columns([key], [column])  #(dst, src)

    #def _copy_columns(self, keys, columns):
    #    for key in keys:
    #        self._validate_key(key)
    #    self._backend.copy_columns(self, keys, columns)  #(dst, src)

    #def _assign_column(self, key, value):
    #    self._validate_key(key)
    #    try:
    #        dst = self._columns[key]
    #    except KeyError:
    #        dst = BigColumn(key)
    #        dst.frame = self
    #    self._backend.assign(dst, value)
    #def _assign_frame(self, value):
    #    self._backend.assign(self, value)

    def _get_new_frame_name(self, source=None):
        try:
            annotation = "_" + source.annotation
        except:
            annotation = ''
        return "frame_" + uuid.uuid4().hex + annotation

    def _validate_key(self, key):
        if key in dir(self) and key not in self._columns:
            raise KeyError("Invalid column name '%s'" % key)

    #def __delitem__(self, key):
    #    if isinstance(key, slice):
    #        keys = self._columns.keys()[key]
    #    elif isinstance(key, list):
    #        keys = key
    #    else:
    #        keys = [key]
    #    for k in keys:  # check for KeyError now before sending to backend
    #        dummy = self._columns[k]
    #    self._backend.drop_columns(self, keys)

    def __repr__(self):
        return json.dumps({'_id': str(self._id),
                           'name': self.name,
                           'schema': repr(self.schema)})
    def __len__(self):
        return len(self._columns)

    def __contains__(self, key):
        return self._columns.__contains__(key)

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

    @property
    def name(self):
        """
        To Do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        ?             ?                                                                       
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return self._name

    @property
    def data_type(self):
        """
        To Do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        ?             ?                                                                       
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return type(self)

    @property
    def column_names(self):
        """
        To Do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        ?             ?                                                                       
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return self._columns.keys()

    @property
    def schema(self):
        """
        To Do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        ?             ?                                                                       
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return FrameSchema(zip(self._columns.keys(),
                               map(lambda c: c.data_type, self._columns.values())))

    def _as_json_obj(self):
        return self._backend._as_json_obj(self)
        #return ['frame', {"name": self.name}]

    def append(self, *data):
        """
        To Do

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        *data         pointer     data to be appended                                    Not optional
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        pass

    def filter(self, predicate):
        """
        To Do

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        predicate     ?                                                                  Not optional
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        self._backend.filter(self, predicate)

    def count(self):
        """
        To Do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        ?             ?                                                                       
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """


    def remove_column(self, name):
        """
        Remove columns of data

        Parameters
        ----------
        name : str OR list of str

        Notes
        -----
        This function will retain a single column
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

        ============= =========== ========================================================== ==============
        Parameter     Data Type   Meaning                                                    Default Value
        ============= =========== ========================================================== ==============
        how           str         any: if any column has an NA value, drop row               any
                                  
                                  all: if all the columns have an NA value, drop row
                                  
                                  column name: if named column has an NA value, drop row
        ------------- ----------- ---------------------------------------------------------- --------------
        column_subset str or      if not "None", only the given columns are considered       None
                      list of str
        ============= =========== ========================================================== ==============

        **Examples**

        >>> 
        """
        self._backend.dropna(self, how, column_subset)

    def inspect(self, n=10):
        """
        To Do

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        n             int                                                                10
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """


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
        Add a column

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        column_name   str         The name of the new column                             Not optional
        func          ?           ?                                                      Not optional
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        return self._backend.add_column(self, column_name, func)


    def rename_column(self, column_name, new_name):
        """
        Rename a column

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        column_name   str         Existing column name                                   Not optional
        new_name      str         New name for column                                    Not optional
        ============= =========== ====================================================== ==============

        **Examples**

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
        Save the work

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        name          str         A new name to store the database                       None
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        self._backend.save(self, name)

    def take(self, n, offset=0):
        """
        To Do

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        n             ?                                                                  Not optional
        offset        int                                                                0
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        return self._backend.take(self, n, offset)



class FrameSchema(OrderedDict):
    # Right now, Frame Schema is only hold datatypes for the columns
    # Predict that this will become a FrameDescriptor or Columns
    # manager, where Columns will have more than just
    # their data type --they could have abstractions ("Views")
    # This needs to play with the FrameRegistry

    def __init__(self, source=None):
        super(FrameSchema, self).__init__()
        if isinstance(source, basestring):
            self._init_from_string(source)
        else:
            self._init_from_tuples(source)

    def __repr__(self,  _repr_running=None):
        return json.dumps(zip(self.get_column_names(),
                              self.get_column_data_type_strings()))
        #return '[%s]' % (','.join(['("%s", "%s")'
        #                           % (k, supported_types.get_type_string(v))
        #                           for k, v in self.items()]))

    def _init_from_tuples(self, tuples):
        self.clear()
        for k, v in tuples:
            if isinstance(v, basestring):
                self[k] = supported_types.get_type_from_string(v)
            elif v not in supported_types:
                raise ValueError("Unsupported data type in schema " + str(v))
            else:
                self[k] = v

    def _init_from_string(self, schema_string):
        logger.info("FrameSchema init from string: {0}".format(schema_string))
        self._init_from_tuples(json.loads(schema_string))

    def get_column_names(self):
        """
        To do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        None
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return self.keys()

    def get_column_data_types(self):
        """
        To do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        None
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return self.values()

    def get_column_data_type_strings(self):
        """
        To do

        ============= =========== =========================================================== ==============
        Parameter     Data Type   Meaning                                                     Default Value
        ============= =========== =========================================================== ==============
        None
        ============= =========== =========================================================== ==============

        **Examples**

        >>> 
        """
        return map(lambda v: supported_types.get_type_string(v), self.values())

    def drop(self, victim_columns):
        """
        Get rid of particular columns

        ============== =========== ====================================================== ==============
        Parameter      Data Type   Meaning                                                Default Value
        ============== =========== ====================================================== ==============
        victim_columns str or      Name(s) of the columns to drop                         Not optional
                       list of str
        ============== =========== ====================================================== ==============
        **Examples**

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
        for n, t in new_columns.items():
            self[n] = t

    def merge(self, schema):
        """
        Merge another schema into the current one

        ============= =========== ====================================================== ==============
        Parameter     Data Type   Meaning                                                Default Value
        ============= =========== ====================================================== ==============
        schema        structure                                                          Not optional
        ============= =========== ====================================================== ==============

        **Examples**

        >>> 
        """
        for k, v in schema.items():
            if k not in self:
                self[k] = v
            elif self[k] != v:
                raise ValueError('Schema merge collision: column being set to '
                                 'a different type')
