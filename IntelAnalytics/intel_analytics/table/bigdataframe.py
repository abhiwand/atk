##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
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
The common methods and class for buiding and operating with big data frames
"""
import sys
import abc
import traceback
from intel_analytics.config import global_config, dynamic_import, get_time_str

__all__ = ['get_frame_builder',
           'get_frame',
           'get_frame_names',
           'BigDataFrame',
           'FrameBuilder'
           ]


class FrameBuilderFactory(object):
    """
    An abstract class for the various frame builder factories (that is, one for Hbase).
    """
    __metaclass__ = abc.ABCMeta

    #todo: implement when builder discrimination is required
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_frame_builder(self):
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_frame(self, frame_name):
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_frame_names(self):
        raise Exception("Not overridden")


class FrameBuilder(object):
    """
    Builds BigDataFrame objects
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build_from_csv(self, frame_name, file_name, schema, skip_header=False):
        """
        Reads a CSV (comma-separated-value) file and loads it into a frame.

        Parameters
        ----------
        frame_name : String
            The name of the new frame
        file_name : String
            The path to the source CSV file.
        schema : String
            The schema of the source file.  A comma-separated list of ``key:value``
            pairs, where *key* is the name of the column and *value* is the data
            type of the column (`valid data types <http://pig.apache.org/docs/r0.7.0/piglatin_ref2.html#Data+Types>`_)
            ``'user:long,vertex_type:chararray,movie:long,rating:long,splits:chararray'``
        skip_header : Bool
            if True, skip the first line of the file

        Returns
        -------
        frame : BigDataFrame
            The new frame

        Examples
        --------
        >>> fb = get_frame_builder()
        >>> frame = fb.build_from_csv('my_frame', 'big_data.csv', schema='user:long,vertex_type:chararray,movie:long,rating:long,splits:chararray')

        """
        pass

    @abc.abstractmethod
    def append_from_csv(self, data_frame, file_name, skip_header=False):
        """
        Reads a CSV (comma-separated-value) file and append it into an existing data frame.

        Parameters
        ----------
        C{data_frame} : BigDataFrame
            An existing big data frame
        C{file_name} : String or list of strings
            File/Files to be imported
        """
        pass

    @abc.abstractmethod
    def build_from_json(self, frame_name, file_name):
        """
        Reads a JSON (www.json.org) file and loads it into a frame.

        Parameters
        ----------
        frame_name : String
            The name of the new frame
        file_name : String
            The path to the source CSV file.

        Returns
        -------
        frame : BigDataFrame
            The new frame
        """
        pass

    def append_from_json(self, data_frame, file_name):
        """
        Reads an XML file and loads it into a frame.
        Reads an JSON file and append it into an existing data frame

        Parameters
        ----------
        C{data_frame} : BigDataFrame
            An existing big data frame
        C{file_name} : String or list of strings
            File/Files to be imported:
        """
        pass

    @abc.abstractmethod
    def build_from_xml(self, frame_name, file_name, tag_name=None):
        """
        Reads an XML file and loads it into a table.

        Parameters
        ----------
        frame_name : String
            The name of the new frame
        file_name : String
            The path to the source CSV file.
        tag_name : String
            The XML tag name
        schema : String, optional
            The schema of the source file
        C{frame_name} : String
            The name for the data frame
        C{filename} : String or list of strings
            The path to the XML file/files
        C{tag_name} : String
            XML tag for record:

        TODO: Other parameters for the parser

        Returns
        -------
        frame : BigDataFrame
            The new frame
        """
        pass

    @abc.abstractmethod
    def append_from_xml(self, data_frame, file_name, tag_name):
        """
        Reads an XML file and append it into a existing data frame.

        Parameters
        ----------
        C{data_frame} : BigDataFrame
            An existing big data frame.
        C{filename} : String or list of strings
            The path to the XML file/files
        C{tag_name} : String
            XML tag for record.

        TODO: Other parameters for the parser

        Returns
        -------
        frame : C{BigDataFrame}
        """
        pass

    @abc.abstractmethod
    def append_from_data_frame(self, target_data_frame, source_data_frame):
        """
        Apped list of source data frames to target data frame.

        Parameters
        ----------
        C{target_data_frame} : BigDataFrame
            The data frame to append data to
        C{source_data_frame} : List
            List of data frame which data will be appended to the target data frame
        """
        pass

    @abc.abstractmethod
    def join_data_frame(self, left, right, how, left_on, right_on, suffixes, join_frame_name):
        """
        Perform SQL JOIN like operation on input BigDataFrames

        Parameters
        ----------
        left: BigDataFrame
            Left side of the join
        right: List
            List of BigDataFrame(s) on the right side of the join
        left_on: String
            String of columnes from left table, space or comma separated
            e.g., 'c1,c2' or 'b2 b3'
        right_on: List
            List of strings, each of which is in comma separated indicating
            columns to be joined corresponding to the list of tables as 
            the 'right', e.g., ['c1,c2', 'b2 b3']
        how: String
            The type of join, INNER, OUTER, LEFT, RIGHT
        suffixes: List
            List of strings, each of which is used as suffix to the column
            names from left and right of the join, e.g. ['_x', '_y1', '_y2'].
            Note the first one is always for the left
        join_frame_name: String
            Output BigDataFrame name

        Return
        ------
        BigDataFrame

        """
        pass

def get_frame_builder():
    """
    Returns a frame builder object which creates BigDataFrame objects
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame_builder()


def get_frame(frame_name):
    """
    Returns a previously created frame

    Parameters
    ----------
    frame_name : String
        Name of previously created frame

    Returns
    -------
    frame : BigDataFrame

    Examples
    --------
    >>> frame = get_frame("my_frame")
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame(frame_name)

def get_frame_names():
    """
    Returns the names of previously created frames that are available
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame_names()


# dynamically and lazily load the correct frame_builder factory,
# according to config
_frame_builder_factory = None


def _get_frame_builder_factory_class():
    global _frame_builder_factory
    if _frame_builder_factory is None:
        frame_builder_factory_class = dynamic_import(
            global_config['py_frame_builder_factory_class'])
        _frame_builder_factory = frame_builder_factory_class.get_instance()
    return _frame_builder_factory


class BigDataFrameException(Exception):
    pass


class BigDataFrame(object):
    """
    Proxy for a large 2D container to work with table data at scale.
    """

    def __init__(self, name, table):
        """Internal constructor; see FrameBuilder and get_frame_builder()"""
        #if not isinstance(table, Table):
        #    raise Exception("bad table given to Constructor")
        if name is None:
            raise BigDataFrameException("BigDataFrame Constructor requires non-None name")
        if table is None:
            raise BigDataFrameException("BigDataFrame Constructor requires non-None table")

        self.name = name
        self._table = table
        self.source_file = self._table.file_name
        """The name of the file from which this frame was originally created"""
        self._lineage=[]
        """history of table names"""
        self._lineage.append(self._table.table_name)

    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        return buf

    def get_schema(self):
        """
        Returns the list of column names/types
        """
        return self._table.get_schema()

    def inspect_as_html(self, nRows=10):
        """
        Get the nRows as an HTML table

        Parameters
        ----------
        nRows : int
            number of rows to retrieve as an HTML table
        """
        return self._table.inspect_as_html(nRows)


    #----------------------------------------------------------------------
    # Apply User-Defined Function (canned and UDF)
    #----------------------------------------------------------------------


    def transform(self, column_name, new_column_name, transformation, transformation_args=None):
        """
        Applies a built-in transformation function to the given column

        Parameters
        ----------
        column_name : String
            source column for the function
        new_column_name : String
            name for the new column that will be created as a result of applying the transformation
        transformation : :ref:`EvalFunctions <evalfunctions>` enumeration
            function to apply
        transformation_args: List
            arguments for the function

        Examples
        --------
        >>> frame.transform('rating', 'log_rating', EvalFunctions.Math.LOG)

        """
        try:
            self._table.transform(column_name, new_column_name, transformation, transformation_args)
            self._lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("transform exception " + str(e))


    def inspect(self, n=10):
        """
        Provides string representation of n sample lines of the table

        Parameters
        ----------
        n : int
            number of rows

        Returns
        -------
        output : String
        """
        # For IPython, consider dumping 2D array (NDarray) for pretty-print.

        try:
            self._table.inspect(n)
        except Exception, e:
            raise BigDataFrameException("head exception " + str(e))

    #----------------------------------------------------------------------
    # Cleaning
    #----------------------------------------------------------------------

    def drop(self, column_name, func):
        """
        Drops rows which meet given criteria

        Parameters
        ----------
        column_name : String
            name of column for the function

        func : function
            filter function evaluated at each cell in the given column; if
            result is true, row is dropped
        """
        raise BigDataFrameException("Not implemented")

    def dropna(self, how='any', column_name=None):
        """
        Drops all rows which have NA values

        Parameters
        ----------
        how : { 'any', 'all' }
            any : if any column has an NA value, drop row
            all : if all the columns have an NA value, drop row
        """
        # Currently we don't support threshold or subset so leave them out for the 0.5 release
        #         thresh : int
        #             require that many non-NA values, else drop row
        #         subset : array-like
        #             considers only the given columns in the check, None means all
        try:
            self._table.dropna(how, column_name)
            self._lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("dropna exception " + str(e))

    def fillna(self, column_name, value):
        """
        Fills in the NA with given value

        Parameters
        ----------
        column_name : String
            name of column for the function
        value : Imputation
            the fill value
        """

        try:
            self._table.fillna(column_name, value)
            self._lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("fillna exception "+ str(e))


    def impute(self, column_name, how):
        """
        Fills in NA values with imputation

        Parameters
        ----------
        column_name : String
            name of column for the function
        how : Imputation
            the imputation operation
        """
        # Imputation will be an enumeration of supported operations, like
        # Imputation.AVG or something

        try:
            self._table.impute(column_name, how)
            self._lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("impute exception "+ str(e))

    def drop_columns(self, column_names):
        """
        Drop columns from the data frame

        Parameters
        ----------
        column_names : String
            comma separated column names such as f1,f2,f3
        """
        self._table.drop_columns(column_names)

    def join(self,
             right=None,
             how='left',
             left_on=None,
             right_on=None,
             suffixes=None,
             join_frame_name=''):

        """
        Perform SQL JOIN on BigDataFrame

        Syntax is similar to pandas DataFrame.join.

        Parameters
        ----------
        right   : BigDataFrame or list/tuple of BigDataFrame
            Frames to be joined with
        how     : Str
            {'left', 'right', 'outer', 'inner'}, default 'inner'
        left_on : Str
            Columns selected to bed joined on from left frame
        right_on: Str or list/tuple of Str
            Columns selected to bed joined on from right frame(s)
        suffixes: tuple of Str
            Suffixes to apply to columns on the output frame
        join_frame_name: Str
            The name of the BigDataFrame that holds the result of join

        Notes
        -----
        1. Sorting
        2. Filtering out replicated columns

        Examples
        --------
        TODO

        Returns
        -------
        joined : BigDataFrame
        """

        def __tolist(v, v_type, v_default):
            """
            check if input is a v_type, or a list/tuple of v_type object
            and return a list of v_type
            """
            if not v:
                return v_default
            if isinstance(v, v_type):
                return [v]
            if isinstance(v, (list, tuple)) and all(isinstance(vv, v_type) for vv in v):
                return [x for x in v]
            return None

        def __iscolumn(frame, column):
            if not column:
                return False
            return column in frame.get_schema().keys()

        def __check_left(left, left_on):
            if not left_on:
                return ','.join(left.get_schema().keys())
            if not all(__iscolumn(left, x) for x in left_on.split(',')):
                return None
            return left_on

        def __check_right(right, rigth_on):
            right_on_default = [ ','.join(r.get_schema().keys()) for r in right ]
            ron = __tolist(right_on, str, right_on_default)
            if not all(all(__iscolumn(r, x) for x in c.split(',')) for r,c in zip(right, ron)):
                return None
            if len(right) != len(ron):
                return None
            return ron

        def __check_suffixes(suffixes, right):
            if not suffixes:
                suffixes = ['_x']
                suffixes.extend(['_y' + str(x) for x in range(1, len(right) + 1)])

            if len(suffixes) != (len(right) + 1):
                return None
            return suffixes

        # check input
        right = __tolist(right, BigDataFrame, [self])
        if not right:
            raise BigDataFrameException("Error! Input frame must be '%s'" \
                                        % self.__class__.__name__)
        if not how.lower() in ['inner', 'outer', 'left', 'right'] :
            raise BigDataFrameException("Error! Unsupported join type '%s'!" % how)

        left_on = __check_left(self, left_on)
        if not left_on:
            raise BigDataFrameException('Error! Invalid column names from left side.')

        right_on = __check_right(right, right_on)
        if not right_on:
            raise BigDataFrameException('Error! Invalid column names from right side.')

        if not all(len(left_on.split(',')) == len(r.split(',')) for r in right_on):
            raise BigDataFrameException("Error! The number of columns selected from left " \
                                        "does not match!")
        # left: _x, right: _y1, _y2, ...
        suffixes = __check_suffixes(suffixes, right)
        if not suffixes:
           raise BigDataFrameException("Error! The number of suffixes does not match "
                                        "total number of frames from left and right!")

        if not join_frame_name:
            join_frame_name = self.name + '_join' + get_time_str()

        if join_frame_name == self.name:
            raise BigDataFrameException('TODO: in-place join')

        try:
            join_frame = self._table.join(right=[x._table for x in right], \
                                          how=how.lower(),  \
                                          left_on=left_on,  \
                                          right_on=right_on,\
                                          suffixes=suffixes,\
                                          join_frame_name=join_frame_name)

            join_frame._lineage.append(join_frame._table.table_name)

        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("join exception "+ str(e))

        return join_frame
