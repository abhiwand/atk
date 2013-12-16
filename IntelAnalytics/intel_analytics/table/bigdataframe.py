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
BigDataFrame
"""
import sys
import abc
import traceback
from intel_analytics.config import global_config, dynamic_import

__all__ = ['get_frame_builder',
           'get_frame',
           'get_frame_names',
           'BigDataFrame'
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
   An abstract class for the various table builders to inherit.
   """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build_from_csv(self, frame_name, file_name, schema, skip_header=False):
        """
        Reads a CSV (comma-separated-value) file and loads it into a table.

        Parameters
        ----------
        C{filename} : String
            The path to the CSV file.
        C{schema} : String
            TODO:

        TODO: Other parameters for the csv parser.

        Returns
        -------
        C{frame} : C{BigDataFrame}
        """
        pass

    @abc.abstractmethod
    def build_from_json(self, frame_name, file_name):
        """
        Reads a JSON (www.json.org) file and loads it into a table.

        Parameters
        ----------
        C{filename} : String
            The path to the JSON file.

        TODO: Other parameters for the parser.

        Returns
        -------
        C{frame} : C{BigDataFrame}
        """
        pass

    @abc.abstractmethod
    def build_from_xml(self, frame_name, file_name, schema=None):
        """
        Reads an XML file and loads it into a table.

        Parameters
        ----------
        C{filename} : String
            The path to the XML file.
        C{schema} : String
            TODO:

        TODO: Other parameters for the parser.

        Returns
        -------
        frame : C{BigDataFrame}
        """
        pass


def get_frame_builder():
    """
    Returns a C{frame_builder} with which to create C{BigDataFrame} objects.
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame_builder()


def get_frame(frame_name):
    """
    Returns a previously created frame.
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame(frame_name)

def get_frame_names():
    """
    Returns a previously created frame.
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
    The C{BigDataFrame} is a proxy for a large 2D container to work with table data at scale.
    """

    def __init__(self, name, table):
        """
        (internal constructor)
        Parameters
        ----------
        C{table} : Table
        """
        #if not isinstance(table, Table):
        #    raise Exception("bad table given to Constructor")
        if name is None:
            raise BigDataFrameException("BigDataFrame Constructor requires non-None name")
        if table is None:
            raise BigDataFrameException("BigDataFrame Constructor requires non-None table")

        self.name = name
        self._table = table
        #This holds the original table from which we imported the data, will be used for understanding which features are derived or not.
        self._original_table_name = self._table.table_name
        self.source_file = self._table.file_name
        self.lineage=[]
        self.lineage.append(self._table.table_name)

    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        return buf

    def get_schema(self):
        """
        Returns the list of column names and types.
        """
        return self._table.get_schema()

    #----------------------------------------------------------------------
    # TODO - Save/Restore/Export
    #----------------------------------------------------------------------

    def to_csv(self, filename, include_header=True, include_schema=True):
        """
        Serializes the current state of the table to a CSV file.

        Parameters
        ----------
        C{filename} : String   
		    The name of the file.
        C{include_header} : Boolean
		    Whether to write the header as the first line.
        C{include_schema} : Boolean
		    Whether to also write a schema file with same name.
        """
        # We'll create an HDFS folder and shard into files.
        # We'll provide a separate, explicit "Download" mechanism to go from
        # HDFS (or S3) to client.
        raise BigDataFrameException("Not implemented")

    def to_json(self, filename, include_schema=True):
        """
        Serializes the current state of the table to an XML file.

        Parameters
        ----------
        C{file} : String
		    The name of the snapshot file.
        C{include_schema} : Boolean
		    Whether to also write a schema file with same name.
        """
        #TODO: embed the schema in same file, similar to header?
        raise BigDataFrameException("Not implemented")


    def to_xml(self, filename, include_schema=True):
        """
        Serializes the current state of the table to an XML file.

        Parameters
        ----------
        C{file} : String
		    The name of a snapshot file.
        C{include_schema} : Boolean
		    Whether to also write a schema file with same name.
        """
        #TODO: use a JSON schema (or XML XSD or DTD?) --embed in the same
        #      file, similar to header?
        raise BigDataFrameException("Not implemented")
    
    
    def to_html(self, nRows=10):
        """
        Gets the first C{nRows} as an HTML table.

        Parameters
        ----------
        C{nRows} : int
            The number of rows to retrieve in the HTML table.
        """
        return self._table.to_html(nRows)  


    #----------------------------------------------------------------------
    # Merging / Joining
    #----------------------------------------------------------------------
    def append(self, other, ignore_index=False, verify_integrity=False):
        """
        (pandas)
        Appends the columns of another frame to the end of this frame's columns and
        index, returning a new object. Columns not in this frame are added as new
        columns.

        Parameters
        ----------
        C{other} : C{DataFrame} or a list of Series/dict-like objects.
        C{ignore_index} : Boolean, default False.
		    If True, do not use the index labels. Useful for gluing together
            record arrays.
        C{verify_integrity} : Boolean, default False.
		    If True, raises Exception on creating index with duplicates.

        Notes
        -----
        If you pass a list of C{dict}, and the keys are all contained in the
        C{DataFrame}'s index, the order of the columns in the resulting C{DataFrame}
        will not hang.

        Returns
        -------
        C{appended} : DataFrame
        """
        raise BigDataFrameException("Not implemented")

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        """
        (pandas)
        Joins columns with another C{DataFrame}, either on the index or on a key column. 
		
		Efficiently Join multiple C{DataFrame} objects by index at once by passing a list.

        Parameters
        ----------
        C{other} : C{DataFrame}, Series with name field set, or list of C{DataFrame}s.
            The index should be similar to one of the columns when using this method.
            If you pass a Series, you must set its name attribute, which will be
            used as the column name in the resulting joined C{DataFrame}.
        C{on} : A column name, tuple or list of column names, or array-like.
            The column(s) to use for joining, otherwise join on the index. If you give 
			multiple columns, the passed C{DataFrame} must have a C{MultiIndex}. You can
            pass an array as the join key, if it is not already contained in the
            calling C{DataFrame}. This is similar to an Excel VLOOKUP operation.
        C{how} : {'left', 'right', 'outer', 'inner'}
            How to handle indexes of the two objects. Default: 'left'
            for joining on index, None, otherwise:
            * C{left} : Use calling frame's index.
            * C{right} : Use input frame's index.
            * C{outer} : Form union of indexes.
            * C{inner} : Use intersection of indexes.
        C{lsuffix} : String
		    The suffix to use from the left frame's overlapping columns.
        C{rsuffix} : String
		    The suffix to use from the right frame's overlapping columns.
        C{sort} : Boolean, default False.
            If True, orders the resulting DataFrame lexicographically by the join key. If False,
            preserves the index order of the calling (left) DataFrame.

        
		
		
	    Notes
        -----
        The on C{lsuffix} and C{rsuffix} options are not supported when passing a list
        of C{DataFrame} objects.

        Returns
        -------
        C{joined} : C{DataFrame}
        """
        raise BigDataFrameException("Not implemented")

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True):
        """
        (pandas)
        Merges C{DataFrame} objects by performing a database-style join operation by
        columns or indexes.

        If joining columns on columns, the C{DataFrame} indexes *will be
        ignored*. Otherwise if joining indexes on indexes or indexes on a
        column or columns, the index will be passed on.

        Parameters
        ----------
        C{right} : C{DataFrame}
        C{how} : {'left', 'right', 'outer', 'inner'}, default 'inner'
            * C{left} : Use only keys from left frame (SQL: left outer join)
            * C{right} : Use only keys from right frame (SQL: right outer join)
            * C{outer} : Use union of keys from both frames (SQL: full outer join)
            * C{inner} : Use intersection of keys from both frames (SQL: inner join)
        C{on} : Label or list.
            The field names to join on. Must be found in both C{DataFrames}. If 
            C{on} is None and not merging on indexes, then it merges on the 
            intersection of the columns by default.
        C{left_on} : Label or list, or array-like.
            The field names to join on in the left C{DataFrame}. Can be a vector or 
            list of vectors of the length of the C{DataFrame} to use a particular 
            vector as the join key instead of columns.
        C{right_on} : Label or list, or array-like.
            The field names to join on in the right C{DataFrame} or vector or list of 
            vectors per left_on docs.
        C{left_index} : Boolean, default False.
            Use the index from the left C{DataFrame} as the join key(s). If it is a
            C{MultiIndex), the number of keys in the other C{DataFrame} (either the
            index or a number of columns) must match the number of levels.
        C{right_index} : Boolean, default False
            Use the index from the right C{DataFrame} as the join key(s). If it is a
            C{MultiIndex}, the number of keys in the other DataFrame (either the
            index or a number of columns) must match the number of levels.
        C{sort} : Boolean, default False.
            Sort the join keys lexicographically in the resulting DataFrame.
        C{suffixes} : 2-length sequence (tuple, list, ...)
            The suffix to apply to overlapping column names in the left and right
            sides, respectively.
        C{copy} : Boolean, default True
            If False, do not copy data unnecessarily.

        Examples
        --------

        >>> A              >>> B
            lkey value         rkey value
        0   foo  1         0   foo  5
        1   bar  2         1   bar  6
        2   baz  3         2   qux  7
        3   foo  4         3   bar  8

        >>> merge(A, B, left_on='lkey', right_on='rkey', how='outer')
           lkey  value_x  rkey  value_y
        0  bar   2        bar   6
        1  bar   2        bar   8
        2  baz   3        NaN   NaN
        3  foo   1        foo   5
        4  foo   4        foo   5
        5  NaN   NaN      qux   7

        Returns
        -------
        merged : DataFrame
        """
        raise BigDataFrameException("Not implemented")

    #----------------------------------------------------------------------
    # Canned Transforms
    #  String Manipulation
    #  Math Functions
    #  Aggregate and Eval
    #  Date/Time
    #  Time Series, Windows, Categorical variables
    #----------------------------------------------------------------------
    # TODO - column crossing function

    # TODO - 0.5 add the methods we use from Pig HERE:


    #----------------------------------------------------------------------
    # Apply User-Defined Function (canned and UDF)
    #----------------------------------------------------------------------


    def transform(self, column_name, new_column_name, transformation, transformation_args=None):

        """
        Applies a built-in transformation function to the given column.

        Parameters
        ----------
        C{column_name} : String
            The source column for the function.
        C{new_column_name} : String
            The name for the new column that will be created as a result of applying the transformation.
        C{transformation} : enumeration
            The transformation to apply.
        C{keep_source_column} : Boolean
            Whether to keep the given column in the output of the transformation.
        C{transformation_args} : list
            The arguments for the transformation to apply.
        """
        try:
            self._table.transform(column_name, new_column_name, transformation, transformation_args)
            self.lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("transform exception " + str(e))

    #TODO - how to pass UDFs through to mapreduce
    def apply(self, column_name, func, output_type):

        """
        Applies a user-defined function (UDF) to the given column.

        Parameters
        ----------
        C{column_name} : String
            The source column for the function.
        C{func} : function
            The function to apply to the column.
        C{output_type} : DataType
            The data type of the output of the function.


        (pandas)::
    #def apply(self, func, axis=0, broadcast=False, raw=False, args=(), **kwds):
        Applies a function along the input axis of a DataFrame. The objects you 
		can pass to these functions are Series objects having either the 
		DataFrame's index (C{axis=0}) or the column's (C{axis=1}) as their index. \
		The return type depends on whether your passed function aggregates or not.

        Parameters
        ----------
        C{func} : function
            The function to apply to each column.
        C{axis} : C{{0, 1}}
            0 : Apply function to each column.
            1 : Apply function to each row.
        C{broadcast} : Boolean, default False
            For aggregation functions, return an object of same size with values
            propagated.
        C{raw} : Boolean, default False
            If False, convert each row or column into a Series. If True, the
            function you pass will receive C{ndarray} objects instead. If you 
			are just applying a NumPy reduction function, this will achieve much
            better performance.
        C{args} : tuple
            Positional arguments you can pass to the function in addition to the
            array or series.
        Pass additional keyword arguments as keywords to the function.

        Examples
        --------
        >>> df.apply(numpy.sqrt) # returns DataFrame
        >>> df.apply(numpy.sum, axis=0) # equiv to df.sum(0)
        >>> df.apply(numpy.sum, axis=1) # equiv to df.sum(1)

        See also
        --------
        C{DataFrame.applymap}: For elementwise operations.

        Returns
        -------
        C{applied} : Series or DataFrame.
        """
        raise BigDataFrameException("Not implemented")

    def applymap(self, column_name, func, output_type):
        """
        Applies a user-defined function (UDF) to each cell in the given column.

        Parameters
        ----------
        C{column_name} : String
            The source column for the function.
        C{func} : function
            The function to apply to the column.
        C{output_type} : C{DataType}
            The data type of the output of the function.

        (pandas frame.py)::
    #def applymap(self, func):
        Applies a function to a DataFrame that is intended to operate
        elementwise, in other words, like doing C{map(func, series)} for 
		each series in the C{DataFrame}.

        Parameters
        ----------
        C{func} : function
            A Python function, that returns a single value from a single value.

        Returns
        -------
        C{applied} : C{DataFrame}
        """
        raise BigDataFrameException("Not implemented")


    # TODO: groupby needs more study
    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False):
        """
        (from pandas/core/generic.py, NDFrame)

        Group series using a mapper (C{dict} or C{key} function, apply given 
		function to the group, return result as series) or by a series of columns.

        Parameters
        ----------
        C{by} : mapping function or list of functions, dict, Series, or tuple or list of column names.
            The function (or list of functions) that you pass is called on each element of the object 
			index to determine the groups. If you pass a dict or Series, the Series or dict VALUES 
			will be used to determine the groups.
        C{axis} : int, default 0
        C{level} : int, level name, or sequence of such, default None.
            If the axis is a MultiIndex (hierarchical), group by a particular
            level or levels.
        C{as_index} : Boolean, default True
            For aggregated output, return the object with group labels as the
            index. Only relevant for C{DataFrame} input. C{as_index=False} is
            effectively "SQL-style" grouped output.
        C{sort} : Boolean, default True
            Sort group keys. You will get better performance by turning this off.
        C{group_keys} : Boolean, default True
            When calling apply, add group keys to the index to identify the pieces.
        C{squeeze} : Boolean, default False
            Reduce the dimensionaility of the return type if possible, otherwise
            return a consistent type.

        Examples
        --------
        # DataFrame result
        >>> data.groupby(func, axis=0).mean()

        # DataFrame result
        >>> data.groupby(['col1', 'col2'])['col3'].mean()

        # DataFrame with hierarchical index
        >>> data.groupby(['col1', 'col2']).mean()

        Returns
        -------
        GroupBy object.
        (some context for future functions or another BDF)
        """
        raise BigDataFrameException("Not implemented")



    #----------------------------------------------------------------------
    # Indexing, Iteration
    #----------------------------------------------------------------------

    # def insert(self, loc, column, value, allow_duplicates=False):
    # Probably don't need --the transform functions will produce new columns
    # and optionally keeps or overwrites the src column
    # 2 options for new column:
    #     1) in-place transformation --> add new column to current table
    #     2) create a deep copy of table with new column included
    # Also, do I keep the src column?
    # these options do not require an explicity insert command

    # Hold off doing the BigColumn slicing, >0.5
    #def __getitem__(self, column_name):
    #    """
    #    Accesses a particular column
    #
    #    Parameters
    #    ----------
    #    column_name : String
    #        name of the column
    #
    #    Returns
    #    -------
    #    column : BigColumn
    #        proxy to the selected column
    #    """
    #    print "Not implemented"


    def head(self, n=10):
        """
        Provides a string representation of the first n lines of the table.

        Parameters
        ----------
        C{n} : int
            The number of rows.

        Returns
        -------
        C{head} : String
        """
        # For IPython, consider dumping 2D array (NDarray) for pretty-print.


        try:
            self._table.head(n)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("head exception " + str(e))

            # How do I manually create a row? (not doing)

            # Not doing __setitem__ for 0.5
            # instead we'll do:
            #
            #    bdf['x'].apply(sq, 'sqx')
            #
            #def __setitem__(self, column_name, value):
            #"""
            #sets/inserts the column name
            #"""
            ## bdf['sqx'] = bdf['x'].apply(sq)
            #print "Not implemented"

            #  Index-based lookup doesn't fit the HBase model unless we provide
            #  consequetive row key, which we shouldn't do anyway because of perf

            #def get_value(self, index, col):
            #"""
            #Quickly retrieve single value at passed column and index  (row key)
        #
    #Parameters
    #----------
    #index : row label
    #col : column label
    #
    #Returns
    #-------
    #value : scalar value
    #"""
    #print "Not implemented"

    #def set_value(self, index, col, value):
    #"""
    #Put single value at passed column and index
    #
    #Parameters
    #----------
    #index : row label
    #col : column label
    #value : scalar value
    #
    #Returns
    #-------
    #frame : DataFrame
    #If label pair is contained, will be reference to calling DataFrame,
    #otherwise a new object
    #"""
    #print "Not implemented"


    #----------------------------------------------------------------------
    # Cleaning
    #----------------------------------------------------------------------

    def drop(self, column_name, func):
        """
        Drops rows which meet given criteria.

        Parameters
        ----------
        C{column_name} : String
            The name of the column for the function.
        C{func} : function
            The filter function evaluated at each cell in the given column. If
            the result is true, the row is dropped.
        """
        raise BigDataFrameException("Not implemented")


    def dropna(self, how='any', column_name=None):
    #         def dropna(self, how='any', thresh=None, subset=None):
        """
        Drops all rows which have NA values.

        Parameters
        ----------
        C{how} : { 'any', 'all' }
            C{any} : If any column has an NA value, drop that row.
            C{all} : If all the columns have an NA value, drop that row.
        """
        # Currently we don't support threshold or subset so leave them out for the 0.5 release
        #         thresh : int
        #             require that many non-NA values, else drop row
        #         subset : array-like
        #             considers only the given columns in the check, None means all
        try:
            self._table.dropna(how, column_name)
            self.lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("dropna exception " + str(e))

    def fillna(self, column_name, value):
        """
        Fills in the NA with a given value.

        Parameters
        ----------
        C{column_name} : String
            The name of the column for the function.
        C{value} : Imputation
            The replacement value.
        """

        try:
            self._table.fillna(column_name, value)
            self.lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("fillna exception "+ str(e))


    def impute(self, column_name, how):
        """
        Fills in NA values with imputation.

        Parameters
        ----------
        C{column_name} : String
            The name of column for the function.
        C{how} : Imputation
            The imputation operation to perform.
        """
        # Imputation will be an enumeration of supported operations, like
        # Imputation.AVG or something

        try:
            self._table.impute(column_name, how)
            self.lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("impute exception "+ str(e))


            # just use our String/Math Manip functions for now
            #def replace(self, column_name, to_replace):
            #"""
            #Parameters
            #----------
            #column_name : String
            #    name of column for replace
            #how : Imputation
            #    the imputation operation
            #"""
            #print "Not implemented"
