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

from intelanalytics.core.types import supported_types
from intelanalytics.core.column import BigColumn

import logging
logger = logging.getLogger(__name__)


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

    >>> my_names = get_frame_names()

    my_names is now ["my_frame_1", "my_frame_2", "my_frame_3"] where my_frame_1, my_frame_2, and my_frame_3 are BigFrame objects
    
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

    source : file descriptor such as a CsvFile object (optional)
        A source of initial data
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
    A BigFrame object has been created and 'g' is it's proxy. It has no data, but it has the name "my_data".

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

    # We are not defining __delitem__.  Columns must be deleted w/ remove_column

    def __repr__(self):
        return json.dumps({'_id': str(self._id),
                           'name': self.name,
                           'schema': repr(self.schema)})

    def __len__(self):
        return len(self._columns)

    def __contains__(self, key):
        return self._columns.__contains__(key)

    def _validate_key(self, key):
        if key in dir(self) and key not in self._columns:
            raise KeyError("Invalid column name '%s'" % key)

    class _FrameIter(object):
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

        >>>
        The raw data has three columns named col01, col02, and col03
        >>> my_frame = BigFrame(p_raw_data, my_csv)
        >>> my_columns = my_frame.column_names()
        my_columns is now ["col01", "col02", "col03"] 
        
        """
        # TODO - Review Docstring
        return self._columns.keys()

    @property
    def data_type(self):
        """
        The type of object this is
        
        Returns
        -------

        type
            The type of object this is
            
        Examples
        --------

        >>> my_frame = BigFrame(p_raw_data, my_csv)
        >>> my_type = my_frame.data_type
        my_type is "BigFrame"
        
        """
        # TODO - Review Docstring
        return type(self)

    #@property
    #def frame_id(self):
    #    return self._id

    @property
    def name(self):
        """
        The name of the current ojbect.
        
        Returns
        -------
        
        string
            The name of the current object.
            
        Examples
        --------

        >>> my_csv = CsvFile("my_data", csv_schema)
        >>> my_frame = BigFrame(p_raw_data , my_csv)
        my_frame is now a BigFrame object named "my_data"
        >>> my_name = my_frame.name
        my_name is now a string with the value "my_data"
        
        """
        # TODO - Review Docstring
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

        FrameSchema
        
        Examples
        --------

        >>> my_csv = CsvFile("my_data", csv_schema)
        >>> my_frame = BigFrame(p_raw_data, my_csv))
        >>> my_schema = my_frame.schema
        my_schema is now a FrameSchema matching the original csv_schema
        
        """
        # TODO - Review Docstring
        return FrameSchema(zip(self._columns.keys(),
                               map(lambda c: c.data_type, self._columns.values())))

    @property
    def uri(self):
        """
        The uniform resource identifier of the current object.
        
        Returns
        -------

        uri
            The value of the uri
        
        Examples
        --------

        >>> my_frame = BigFrame(p_raw_data, my_json)
        >>> my_uri = my_frame.uri
        my_uri is the uniform resource identifier for my_frame
        
        """
        # TODO - Revise Docstring
        return self._uri

    def _as_json_obj(self):
        return self._backend._as_json_obj(self)
        #return ['frame', {"name": self.name}]


    def add_column(self, func, type=str, name=""):
        """
        Adds a new column to the frame.
        
        This function accepts a *lambda* row function to figure out what should go into the new column.  It will also allow the selection of the type of variable and the name of the column.

        Parameters
        ----------

        func : row function
            function which takes the values in the row and produces a value for the new cell
        type : data type (optional)
            specifies the data type of the new column
        name : string (optional)
            specifies the name of the new column

        Notes
        -----

        If the name is left blank, the column name will be given a unique name "res#" where # is the lowest number from 0 through 1000, such that there is not already a column with that name.

        Examples
        --------

        >>> my_frame = BigFrame(data)
        For this example my_frame is a BigFrame object with two int32 columns named "column1" and "column2".
        We want to add a third column named "column3" as an int32 and fill it with the contents of column1 and column2 multiplied together
        >>> my_frame.add_column(lambda row: row.column1*row.column2, int32, "column3")
        The variable my_frame now has three columns, named "column1", "column2" and "column3". The type of column3 is an int32, and the value is the product of column1 and column2.
        <BLANKLINE>
        Now, we want to add another column, we don't care what it is called and it is going to be an empty string (the default).
        >>> my_frame.add_column(lambda row: '')
        The BigFrame object 'my_frame' now has four columns named "column1", "column2", "column3", and "res0000". The first three columns are int32 and the fourth column is string.  Column "res0000" has an empty string ('') in every row.
        

        """
        # TODO - Review examples
        # Generate a synthetic name
        if not name:
            for i in range(0,1000):
                if self._columns.get('res%d' % i, None) is None:
                    name = 'res%d' % i
                    break
        self._backend.add_column(self, func, name, type)

    """
    .. TODO:: add_columns

        def add_columns(self, func, names=None, ):
            # Not implemented yet
            self._backend.add_columns(self, func, names)
    """

    def append(self, data):
        """
        Adds more data to the BigFrame object.

        Parameters
        ----------
            The source of the data being added.

        Examples
        --------

        >>> my_frame = BigFrame( p_raw_data, my_csv )
        >>> my_other_frame = BigFrame( p_raw_data, my_other_csv )
        >>> my_frame.append( &my_other_frame )
        my_frame now has the data from my_other_frame as well
        """
        # TODO - Review examples
        self._backend.append(self, data)

    def copy(self):
        """
        Creates a full copy of the current frame

        Returns
        -------
        frame : BigFrame
            A new frame object which is a copy of this frame
        """
        copied_frame = BigFrame()
        self._backend.project_columns(self, copied_frame, self.column_names)
        return copied_frame

    def filter(self, predicate):
        """
        Select all rows which satisfy a predicate.

        Parameters
        ----------

        predicate: function
            function definition or lambda which evaluates to a boolean value
            
        Examples
        --------

        >>>
        For this example, my_frame is a BigFrame object with lots of data and columns for the attributes of animals.
        We do not want all this data, just the data for lizards and frogs, so ...
        >>> my_frame.filter( animal_type == "lizard" OR animal_type == "frog" )
        my_frame now only has data about lizards and frogs
        
        """
        # TODO - Review docstring
        self._backend.filter(self, predicate)

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
        For this example, my_frame is a BigFrame object with lots of data
        >>> num_rows = my_frame.count()
        num_rows is now the count of rows of data in my_frame
        
        """
        return self._backend.count(self)
        # TODO - Review docstring
        
    def remove_column(self, name):
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
        For this example, my_frame is a BigFrame object with columns titled "column_a", "column_b", column_c and "column_d".
        >>> my_frame.remove_column([ column_b, column_d ])
        Now my_frame only has the columns named "column_a" and "column_c"

        """
        # TODO - Review examples
        self._backend.remove_column(self, name)
        if isinstance(name, basestring):
            name = [name]
        for victim in name:
            del self._columns[victim]

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
        For this example, my_frame is a BigFrame object with a column called "unimportant" (amongst other)
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

        how : any, all, or column name, optional
            any: if any column has an NA value, drop row
            all: if all the columns have an NA value, drop row
            column name: if named column has an NA value, drop row
        column_subset : str OR list of str (optional)
            if not "None", only the given columns are considered

        Examples
        --------

        >>>
        For this example, my_frame is a BigFrame object with a columns called "column_1", "column_2", and "column_3" (amongst others)
        >>> my_frame.dropna( "column_1" ) will eliminate any rows which do not have a value for column_1
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
        return self._backend.inspect(self, n, offset)

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
        if right_on is None:
            right_on = left_on

        return self._backend.join(self, right, left_on, right_on, how)

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

    def rename_column(self, column_name, new_name):
        """
        Rename a column in a frame.

        Parameters
        ----------

        column_name : str
            The name of the existing column.
        new_name : str
            The new name for the column. Must not already exist.

        Examples
        --------

        Let's say my_frame is a BigFrame object with a column named "wrong" and another named "wong"
        >>> my_frame.rename_column([ "wrong", "wong" ], [ "right", "wite" ])
        Now, what was "wrong" is now "right" and what was "wong" is now "wite"

        """
        # TODO - Review docstring
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
        Saves all current data in the frame to disk.

        Parameters
        ----------

        name : str (optional)
            The name of a new file where the frame will be saved

        Examples
        --------

        >>> my_frame = BigFrame( p_data_source, my_xml)
        >>> my_frame.save
        The object my_frame is saved to disk, even those parts which were in memory waiting to do something

        """
        raise NotImplementedError()
        # TODO - Review docstring
        #self._backend.save(self, name)

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
        return self._backend.take(self, n, offset)


class FrameSchema(OrderedDict):
    """
    Ordered key-value pairs of column name -> data type.

    Parameters
    ----------

    source : str or tuple
        What to use as the pattern for the schema

    Raises
    ------

    ValueError
        "Unsupported data type in schema"

    Examples
    --------

    >>>

    .. TODO:: add example

    """
    # TODO - Review docstring
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
        """
        Extract the column names from a schema.
        
        Returns
        -------

        list of str
            A list containing strings with values of the column names
            
        Examples
        --------

        >>> my_schema = FrameSchema( original )
        >>> my_columns = my_schema.get_column_names()
        my_columns is now ["columnA", "columnB"] (for example)

        """        
        # TODO - Review docstring
        return self.keys()

    def get_column_data_types(self):
        """
        Extract the column types from a schema.
        
        Returns
        -------

        list of str
            A list containing strings with values of the column types
            
        Examples
        --------

        >>> my_schema = FrameSchema( original )
        >>> my_types = my_schema.get_column_data_types()
        my_types is now [string, int32] (for example)

        """        
        # TODO - Review docstring
        return self.values()

    def get_column_data_type_strings(self):
        """
        Extract the column types as strings from a schema.
        
        Returns
        -------

        list of str
            A list containing strings with values of the column types
            
        Examples
        --------

        >>> my_schema = FrameSchema( original )
        >>> my_types_string = my_schema.get_column_data_type_strings()
        my_types_string is now [ "str" , "int64" ] (for example)

        """        
        # TODO - Review docstring
        return map(lambda v: supported_types.get_type_string(v), self.values())

    def drop(self, victim_column_names):
        """
        Remove particular columns.

        Parameters
        ----------

        victim_column_names : str OR list of str
            Name(s) of the columns to drop

        Examples
        --------

        >>> my_schema = FrameSchema( original )
        >>> my_schema.drop([ "Column_43", "Colon_43" ])
        my_schema is now a FrameSchema object without Column_43 or Colon_43

        """
        # TODO - Review examples
        if isinstance(victim_column_names, basestring):
            victim_column_names = [victim_column_names]
        for v in victim_column_names:
            del self[v]

    def append(self, new_columns):
        """
        Add new columns.

        Parameters
        ----------

        new_columns : tuple (key : value)
            The column(s) to add
            key : string    The new column name
            value : type    The new column data type
            
        Raises
        ------

        KeyError

        Examples
        --------

        >>> my_schema = FrameSchema()
        >>> my_schema.append([ "Twelve", string ])
        my_schema now contains the schema for an additional string column called Twelve

        """
        # TODO - Review docstring
        for f in new_columns.keys():
            if f in self:
                raise KeyError('Schema already contains column ' + f)
        for name, dtype in new_columns.items():
            self[name] = dtype

    def merge(self, schema):
        """
        Merge two schemas.
        
        This function will merge two schemas. If both schemas have a common column with the same type, it is left as is in the active schems.
        If the column in the other schema is not in the active schema, it is added to the active one.  If both schemas have the same column
        but different types for that column, the function raises an error.

        Parameters
        ----------

        schema : FrameSchema


        Raises
        ------

        ValueError

        Examples
        --------

        >>> my_schema = FrameSchema()
        >>> my_other_schema = FrameSchema( a different schema )
        >>> my_schema.merge( my_other_schema )
        my_schema now contains the original schema, plus my_other_schema's schema

        """
        # TODO - Review docstring
        for name, dtype in schema.items():
            if name not in self:
                self[name] = dtype
            elif self[name] != dtype:
                raise ValueError('Schema merge collision: column being set to '
                                 'a different type')
