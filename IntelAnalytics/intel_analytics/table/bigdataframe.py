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
import collections
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
    def join_data_frame(self, left, right, how, left_on, right_on, suffixes, join_frame_name, overwrite=False):
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
        overwrite: Boolean
            Wether to overwrite the output table if it already exists

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


class BigDataFilter(object):
    """
    Create a filter to be applied on a BigDataFrame
    BigDataFilter(filter)
	filter: filter condition as a boolean expression
    BigDataFilter(filter, column)
	filter: filter condition as a regex string
	column: name of the column to apply regex
    """

    def __init__(self, filter, column = ''):
        self.filter_condition = filter
        self.column_to_apply = column


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

    def aggregate(self, group_by_column_list, aggregation_list, aggregate_frame_name, overwrite=False):

        """
        Applies a built-in aggregation function to the given column

        Parameters
        ----------
        aggregate_frame_name: String
            aggregate frame name for the output of the aggregation
        group_by_column_list: List
            List of columns to group the data by before applying aggregation to each group
        aggregation_list: List of Tuples [(aggregation_Function, column_to_apply, new_column_name), ...]
            aggregation functions to apply on each group
	overwrite: Boolean
	    whether to overwrite the existing table with the same name

	Returns
	-------
	BigDataFrame
	    Aggregated frame
        """
        try:
            aggregate_table = self._table.aggregate(aggregate_frame_name, group_by_column_list, aggregation_list, overwrite)
	    return BigDataFrame(aggregate_frame_name, aggregate_table)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("Error during aggregation " + str(e))

    def aggregate_on_range(self, group_by_column, range, aggregation_list, aggregate_frame_name, overwrite=False):

        """
        Applies a built-in aggregation function to the given column given a range

        Parameters
        ----------
        aggregate_frame_name: String
            aggregate frame name for the output of the aggregation
        group_by_column: String
            Column to group the data by before applying aggregation to each group
	range: String
	    range of the group_by_column for applying the group
	    Supported formats - min:max:stepsize, comma separated values
        aggregation_list: List of Tuples [(aggregation_Function, column_to_apply, new_column_name), ...]
            aggregation functions to apply on each group
	overwrite: Boolean
	    whether to overwrite the existing table with the same name

	Returns
	-------
	BigDataFrame
	    Aggregated frame
        """
        try:
            aggregate_table = self._table.aggregate_on_range(aggregate_frame_name, group_by_column, range, aggregation_list, overwrite)
	    return BigDataFrame(aggregate_frame_name, aggregate_table)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("Error during aggregation on range " + str(e))

    def inspect(self, n=10):
        """
        Provides string representation of n sample lines of the table

        Parameters
        ----------
        n : int, optional
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


    def kfold_split(self,
                        k=10,
                        test_fold_id=0,
                        fold_id_column="fold_id",
                        split_name=["TE","TR"],
                        output_column='edge_type',
                        update=False,
                        overwrite=False):
        """
        Split user's ML data into Train and Test for k-fold cross-validation.

        Parameters
        ----------
        k : Integer, optional
            How many folds to split.
            The default value is 10.
        test_fold_id : Integer, optional
            Which fold to use for test.
            The valid value range is [1,k].
            The default value is 0.
        fold_id_column : String, optional
            The name of the column to store fold_id.
            The default value is "fold_id"
        split_name : List, optional
            Each value is the name for each split.
            The size of the list is 2.
            The default value is ["TE", "TR"]
        output_column : string, optional
            The name of the column to store split results.
            The default value is "splits"
        update : Boolean, optional
            whether to recalculate fold_id_column
            The default value is False
        overwrite : Boolean, optional
            whether to overwrite if output_column already exists
            The default value is False

        Examples
        --------
        It can be used to split data for K-fold cross validation.
        In the first iteration of k-fold cross validation, users can call
        >>> frame.kfold_split(test_fold_id=1, fold_id_column="new_id")
        If there is no existing "new_id" column, this method will firstly generate
        fold id into column "fold_id". And then label the data in the first fold as Test,
        and the rest as Train, save split labels into column "edge_type"

        Then in the x-th iterations, where x is no greater than k, users can call
        >>> frame.kfload_split(test_fold_id=x)
        This method will label the x-th fold as Test, and the rest as Train,
        by using of fold_id_column for the first iteration.

        If user has already randomized data by transform function, for example, by
        >>> frame.transform('rating','rand10', EvalFunctions.Math.RANDOM,[1,10])
        this method can be used together with existing fold_id_column to
        split ML data into Test/Train
        >>> frame.kfold_split(fold_id_column='rand10', test_fold_id=3)
        will label data in the third fold as Test, and the rest as Train.
        Save results in a column named "edge_type"
        """

        try:
            self._table.kfold_split(k, test_fold_id, fold_id_column, split_name, output_column, update, overwrite)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("kfold_split exception " + str(e))


    def percent_split(self,
                      randomization_column='rnd_id',
                      split_percent=[70,20,10],
                      split_name=["TR","VA","TE"],
                      output_column='edge_type',
                      update=False,
                      overwrite=False):

        """
        Split user's data into different buckets based on percentage distribution
        A good usage example is to segment ML data into Train and Test,
        or Train, Validate and Test.

        Parameters
        ----------
        randomization_column : String, optional
            Name of input column which contains randomization info.
            The default value is empty string.
        split_percent : List, optional
            Each value is the percentage for each split.
            The sum of all percentage values should be 100.
            The default value is [70,20,10]
        split_name : List, optional
            Each value is the name for each split.
            The default value is ["TR","VA","TE"]
        output_column : string, optional
            The name of the column to store split results.
            The default value is "split_label"
        update : Boolean, optional
            whether to recalculate fold_id_column
            The default value is False
        overwrite : boolean, optional
            whether to overwrite if output_column already exists
            The default value is False.

        Examples
        --------
        >>> frame.percent_split(randomization_column="new_id", split_percent=[60,30,20])
        If "new_id" does not exist, this method will firstly randomization data into [1,100] folds.
        Then label 60% of data as Train, 30% as Validate, 20% as Test, and save results in
        a column named "edge_type"

        If user has already randomized data by transform function, for example, by
        >>> frame.transform('rating','fold_id', EvalFunctions.Math.RANDOM,[1,100])
        this method can be also used together with existing randomization_column to split data
        >>> frame.autosplit(randomization_column="fold_id", split_percent=[75,15,10])
        will label 75% of data as Train, 15% as Validate, 10% as Test, and save results in
        a column named "edge_type"
        """

        try:
            self._table.percent_split(randomization_column, split_percent, split_name, output_column, update, overwrite)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("percent_split exception " + str(e))


    #----------------------------------------------------------------------
    # Cleaning
    #----------------------------------------------------------------------

    def drop(self, filter, frame_name=''):
        """
        Drops rows which meet given criteria

        Parameters
        ----------
        filter: BigDataFilter
            Filter to be applied to each row, either on specific column or the complete row
	    frame_name: String, optional
	    create a new frame for the remaining records if not deleting inplace
	
	Returns
	-------
        frame: BigDataFrame
        """
	
        try:
	    inplace = (frame_name.strip() == '')
	    isregex = (filter.column_to_apply.strip() != '')

            result_table = self._table.drop(filter.filter_condition, filter.column_to_apply, isregex, inplace, frame_name)

	    if inplace:
		frame = self
	    else:
	        frame = BigDataFrame(frame_name, result_table)
	    return frame
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("Unable to drop rows " + str(e))


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
             join_frame_name='',
             overwrite=False):

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
        overwrite: Boolean
            Wether to overwrite the output table if it already exists

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
            if isinstance(v, collections.Iterable) and all(isinstance(vv, v_type) for vv in v):
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
                                          join_frame_name=join_frame_name, \
                                          overwrite=overwrite)

            join_frame._lineage.append(join_frame._table.table_name)

        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("join exception "+ str(e))

        return join_frame
