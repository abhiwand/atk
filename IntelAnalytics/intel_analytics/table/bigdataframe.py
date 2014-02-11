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
    Returns a frame_builder with which to create BigDataFrame objects
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame_builder()


def get_frame(frame_name):
    """
    Returns a previously created frame
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame(frame_name)

def get_frame_names():
    """
    Returns a previously created frame
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
        transformation : enumeration
            transformation to apply
        transformation_args: list
            the arguments for the transformation to apply
        """
        try:
            self._table.transform(column_name, new_column_name, transformation, transformation_args)
            self.lineage.append(self._table.table_name)
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("transform exception " + str(e))

    def aggregate(self, aggregate_frame_name, group_by_column_list, aggregation_list, overwrite=False):

        """
        Applies a built-in aggregation function to the given column

        Parameters
        ----------
        aggregate_table_name: String
            aggregate table name for the output of the aggregation
        group_by_column_list: List
            List of columns to group the data by before applying aggregation to each group
        aggregation_list: List of Tuples [(aggregation_Function, column_to_apply, new_column_name), ...]
            aggregation functions to apply on each group
	overwrite: Boolean
	    whether to overwrite the existing table with the same name
        """
        try:
            aggregate_table = self._table.aggregate(aggregate_frame_name, group_by_column_list, aggregation_list, overwrite)
	    aggregate_frame = BigDataFrame(aggregate_frame_name, aggregate_table)
	    return aggregate_frame
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("transform exception " + str(e))

    def aggregate_on_range(self, aggregate_frame_name, group_by_column, range, aggregation_list, overwrite=False):

        """
        Applies a built-in aggregation function to the given column

        Parameters
        ----------
        aggregate_table_name: String
            aggregate table name for the output of the aggregation
        group_by_column: String
            Column to group the data by before applying aggregation to each group
	range: String
	    range of the group_by_column for applying the group  
        aggregation_list: List of Tuples [(aggregation_Function, column_to_apply, new_column_name), ...]
            aggregation functions to apply on each group
	overwrite: Boolean
	    whether to overwrite the existing table with the same name
        """
        try:
            aggregate_table = self._table.aggregate_on_range(aggregate_frame_name, group_by_column, range, aggregation_list, overwrite)
	    aggregate_frame = BigDataFrame(aggregate_frame_name, aggregate_table)
	    return aggregate_frame
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("transform exception " + str(e))

    def inspect(self, n=10):
        """
        Provides string representation of the n sample lines of the table

        Parameters
        ----------
        n : int
            number of rows
            Returns

        Returns
        -------
        C{head} : String
        """
        # For IPython, consider dumping 2D array (NDarray) for pretty-print.

        try:
            self._table.inspect(n)
        except Exception, e:
            raise BigDataFrameException("head exception " + str(e))

    #----------------------------------------------------------------------
    # Cleaning
    #----------------------------------------------------------------------

    def drop(self, filter, column='', isregex=False, inplace=True, frame_name=''):
        """
        Drops rows which meet given criteria

        Parameters
        ----------
        filter: String
            filter function evaluated at each row
            if result is true, row is dropped
	column: String
	    name of the column on which filter needs to be applied (required in case of regex filter)
	isregex: Boolean
	    if the filter is a regex expression
        inplace: Boolean
	    True:  drop the rows and update the frame
	    False: drop the rows and create a new frame with the remaining rows
	frame_name: String
	    name of the new frame to be created if inplace is False
	
	Returns
	-------
        frame : C{BigDataFrame}
        """
	
        try:
	    if (inplace == False and frame_name.strip() == ''):
		raise Exception('Invalid frame_name: Please input a valid frame_name')
	    if (isregex == True and column == ''):
		raise Exception('Invalid column: Please input a valid column for regex to be applied')

            result_table = self._table.drop(filter, column, isregex, inplace, frame_name)
	    if (inplace):
		frame = self
	    else:
	        frame = BigDataFrame(frame_name, result_table)
	    return frame
        except Exception, e:
            print traceback.format_exc()
            raise BigDataFrameException("Unable to drop rows " + str(e))


    def dropna(self, how='any', column_name=None):
    #         def dropna(self, how='any', thresh=None, subset=None):
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
            self.lineage.append(self._table.table_name)
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
            self.lineage.append(self._table.table_name)
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
            self.lineage.append(self._table.table_name)
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
