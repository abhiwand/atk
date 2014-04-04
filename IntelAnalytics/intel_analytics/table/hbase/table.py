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
import os
import re
import collections
from intel_analytics.table.pig import pig_helpers

import numpy
import math

from intel_analytics.config import Registry, \
    global_config as config, get_time_str, global_config
from intel_analytics.pig import get_pig_args, is_local_run
from intel_analytics.table.bigdataframe import BigDataFrame, FrameBuilder, StringSplitOptions
from intel_analytics.table.bigcolumn import BigColumn
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.pig.pig_script_builder import PigScriptBuilder, HBaseSource, HBaseLoadFunction, HBaseStoreFunction
from intel_analytics.table.pig.pig_flatten_script_builder import FlattenScriptBuilder

# import sys is needed here because test_hbase_table module relies
# on it to patch sys.stdout
import sys

from schema import ETLSchema, merge_schema
from range import ETLRange
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.logger import stdout_logger as logger
from intel_analytics.subproc import call
from intel_analytics.report import MapOnlyProgressReportStrategy, PigJobReportStrategy
from pydoop.hdfs.path import exists
from pydoop.hdfs import rmr
import hashlib
from intel_analytics.visualization import histogram

MAX_ROW_KEY = 'max_row_key'

try:
    from intel_analytics.pigprogressreportstrategy import PigProgressReportStrategy as etl_report_strategy#depends on ipython
except ImportError, e:
    from intel_analytics.report import PrintReportStrategy as etl_report_strategy


class DataAppendException(Exception):
    pass


class Imputation:
    """
    Imputation

    Imputation is replacing missing values in a dataset. See http://en.wikipedia.org/wiki/Imputation_%28statistics%29
    Currently the only supported imputation method is mean imputation, which replaces all missing values with
    the mean.
    """

    MEAN = 1

    @staticmethod
    def to_string(x):
        return {
            Imputation.MEAN: 'avg'
        }[x]


available_imputations = []#used for validation, does the user try to perform a valid imputation?
for key, val in Imputation.__dict__.items():
    if not isinstance(val, int):
        continue
    available_imputations.append(val)


class HBaseTableException(Exception):
    pass


class HBaseTable(object):
    """
    Table Implementation for HBase
    """

    def __init__(self, table_name, file_name):
        """
        (internal constructor)
        Parameters
        ----------
        table_name : String
            name of table in Hbase
        file_name : String
            name of file from which this table came
        """
        self.table_name = table_name
        self.file_name = file_name

    def __get_aggregation_list_and_schema(self, aggregation_arguments, etl_schema, new_schema_def):

	aggregation_list = []
	for i in aggregation_arguments:
	    function_name = column_to_apply = new_column_name = ""
	    if isinstance(i, tuple):
		function_name, column_to_apply, new_column_name = i[0], i[1], (i[1] if len(i) == 2 else i[2])
	    else:
		function_name = i
		if (function_name == EvalFunctions.Aggregation.COUNT):
		    column_to_apply, new_column_name = "*", "count"
		else:
                    raise HBaseTableException("Invalid aggregation: " + function_name)

	    try:
	        aggregation_list.append(EvalFunctions.to_string(function_name))
	    except:
		raise HBaseTableException('The specified aggregation function is invalid: ' + function_name)


	    if (column_to_apply != "*" and column_to_apply not in etl_schema.feature_names):
                raise HBaseTableException("Column %s does not exist" % column_to_apply)

	    aggregation_list.append(column_to_apply);
	    aggregation_list.append(new_column_name)


	    if (function_name in [EvalFunctions.Aggregation.COUNT, EvalFunctions.Aggregation.COUNT_DISTINCT]):
	        new_schema_def += ",%s:int" % (new_column_name)
	    elif (function_name in [EvalFunctions.Aggregation.DISTINCT]):
	        new_schema_def += ",%s:chararray" % (new_column_name)
	    else:
	        new_schema_def += ",%s:%s" % (new_column_name,
					          etl_schema.get_feature_type(column_to_apply))

	return aggregation_list, new_schema_def

    def aggregate(self,
		  aggregate_frame_name,
		  group_by_columns,
		  aggregation_arguments,
		  overwrite):

        #load schema info
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        # You should check if the group_by_columns are valid or not

        new_schema_def = ",".join(["%s:%s" % (col, etl_schema.get_feature_type(col)) for col in group_by_columns])

	aggregation_list, new_schema_def = self.__get_aggregation_list_and_schema(aggregation_arguments, etl_schema, new_schema_def)

        args = get_pig_args('pig_aggregation.py')

        new_table_name = _create_table_name(aggregate_frame_name, overwrite)
        hbase_table = HBaseTable(new_table_name, self.file_name)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])


        args.extend(['-i', self.table_name,
                 '-o', new_table_name,
		 '-a', " ".join(aggregation_list),
		 '-g', ",".join(group_by_columns),
                 '-u', feature_names_as_str, '-r', feature_types_as_str,
                ])

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')


        new_etl_schema = ETLSchema()
        new_etl_schema.populate_schema(new_schema_def)
        new_etl_schema.save_schema(new_table_name)

        hbase_registry.register(aggregate_frame_name, new_table_name, overwrite)
        return hbase_table

    def aggregate_on_range(self,
		  aggregate_frame_name,
		  group_by_column,
		  range,
		  aggregation_arguments,
		  overwrite):

        #load schema info
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

	# You should check if the group_by_columns are valid or not

	new_schema_def = "AggregateGroup:chararray"

        aggregation_list, new_schema_def = self.__get_aggregation_list_and_schema(aggregation_arguments, etl_schema, new_schema_def)

        args = get_pig_args('pig_range_aggregation.py')
        _range = ETLRange(range).toString()


	new_table_name = _create_table_name(aggregate_frame_name, overwrite)
        hbase_table = HBaseTable(new_table_name, self.file_name)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])


        args.extend(['-i', self.table_name,
                 '-o', new_table_name,
		 '-a', " ".join(aggregation_list),
		 '-g', group_by_column,
		 '-l', _range,
                 '-u', feature_names_as_str, '-r', feature_types_as_str,
                ])

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')


        new_etl_schema = ETLSchema()
        new_etl_schema.populate_schema(new_schema_def)
        new_etl_schema.save_schema(new_table_name)

        hbase_registry.register(aggregate_frame_name, new_table_name, overwrite)
        return hbase_table

    def transform(self,
                  column_name,
                  new_column_name,
                  transformation,
                  transformation_args=None):

        try:
            transformation_to_apply = EvalFunctions.to_string(transformation)
        except:
            raise HBaseTableException('The specified transformation function is invalid')

        #by default all transforms are now in-place
        keep_source_column=True#For in-place transformations the source/original feature has to be kept
        #load schema info
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        # input can't be null
        if not column_name:
            raise HBaseTableException("Input column is empty")

        # check input: can be a single column or an expression of multiple columns
        # accepted format exampe: transform('(a+b*(5+c))-d', ...
        if transformation == EvalFunctions.Math.ARITHMETIC:
            # basic syntax check of matching parentheses
            if column_name.count('(') != column_name.count(')'):
                raise HBaseTableException("Arithmetic expression syntax error!")
            cols = re.split('\+|-|\*|\/|\%', re.sub('[() ]', '', column_name))
            if len(cols) < 2:
                raise HBaseTableException("Arithmetic operations need more than 1 input")
            for col in cols:
                try:
                    float(col)
                except:
                    if col not in etl_schema.feature_names:
                        raise HBaseTableException("Column %s in expression %s does not exist" % (col, column_name))
        # check input: comma separated columns or single-quoted string literals
        # accepted format exampe: transform('(a,b,\'MyString\'', ...
        else:
            cols = column_name.split(',')

            if transformation == EvalFunctions.String.CONCAT and len(cols) < 2:
                raise HBaseTableException("Concatenation needs more than 1 input")

            for col in cols:
                if ((not ('\'' == col[0] and '\'' == col[len(col)-1])) and
                    col not in etl_schema.feature_names):
                    raise HBaseTableException("Column %s in expression %s does not exist" % (col, column_name))


        if not column_name:
            column_name = '' #some operations does not requires a column name.

        args = get_pig_args('pig_transform.py')

        args += ['-f', column_name, '-i', self.table_name,
                 '-o', self.table_name, '-t', transformation_to_apply,
                 '-u', feature_names_as_str, '-r', feature_types_as_str,
                 '-n', new_column_name]

        if transformation_args:  # we have some args that we need to pass to the transformation function
            args += ['-a', str(transformation_args)]

        if keep_source_column:
            args += ['-k']

        logger.debug(args)
        #print ' '.join(map(str,args))

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')

        type = "bytearray"
        #need to update schema here as it's difficult to pass the updated schema info from jython to python
        if (not keep_source_column) or column_name == new_column_name:
            idx =  etl_schema.feature_names.index(column_name)
            type = etl_schema.feature_types[idx]
            del etl_schema.feature_names[idx]
            del etl_schema.feature_types[idx]
        etl_schema.feature_names.append(new_column_name)

        #update the data type of return column based on type of transform we have
        if transformation in [
                              EvalFunctions.String.ENDS_WITH,
                              EvalFunctions.String.EQUALS_IGNORE_CASE,
                              EvalFunctions.String.STARTS_WITH,
                              ]:
            etl_schema.feature_types.append('boolean')
        elif transformation in [
                                EvalFunctions.String.INDEX_OF,
                                EvalFunctions.String.LAST_INDEX_OF,
                                EvalFunctions.String.LENGTH,
                                EvalFunctions.Math.FLOOR,
                                EvalFunctions.Math.CEIL,
                                EvalFunctions.Math.ROUND,
                                EvalFunctions.Math.MOD,
                               ]:
            etl_schema.feature_types.append('long')
        elif transformation in [
                                EvalFunctions.String.LOWER,
                                EvalFunctions.String.LTRIM,
                                EvalFunctions.String.REGEX_EXTRACT,
                                EvalFunctions.String.REGEX_EXTRACT_ALL,
                                EvalFunctions.String.REPLACE,
                                EvalFunctions.String.RTRIM,
                                EvalFunctions.String.STRSPLIT,
                                EvalFunctions.String.SUBSTRING,
                                EvalFunctions.String.TRIM,
                                EvalFunctions.String.UPPER,
                                EvalFunctions.String.CONCAT
                                ]:
            #print "here"
            etl_schema.feature_types.append('chararray')
        elif transformation == EvalFunctions.String.TOKENIZE:
            etl_schema.feature_types.append('chararray')
        #same as input column
        elif transformation in [
                                EvalFunctions.Math.ABS,
                                EvalFunctions.Math.LOG,
                                EvalFunctions.Math.LOG10,
                                EvalFunctions.Math.POW,
                                EvalFunctions.Math.EXP,
                                EvalFunctions.Math.STND,
                                EvalFunctions.Math.SQRT,
                                EvalFunctions.Math.DIV,
                                EvalFunctions.Math.RANDOM,
                                EvalFunctions.Math.ARITHMETIC
                               ]:
            etl_schema.feature_types.append('double')
        else:
            etl_schema.feature_types.append('bytearray')
        etl_schema.save_schema(self.table_name)

    def _update_schema_for_overwrite(self, etl_schema, output_column):
        idx =  etl_schema.feature_names.index(output_column)
        del etl_schema.feature_types[idx]
        del etl_schema.feature_names[idx]


    def kfold_split(self, k, test_fold_id, fold_id_column, split_name, output_column, update, overwrite):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        randomize = False
        if update or (fold_id_column not in etl_schema.feature_names):
            randomize = True

        if update:
            self._update_schema_for_overwrite(etl_schema, fold_id_column)

        if not isinstance(fold_id_column, basestring):
            raise TypeError("fold_id_column should be a string.")
        elif fold_id_column[0].isdigit():
            raise ValueError("fold_id_column %s starts with number.\n"
                             "It is not supported." % fold_id_column)

        if not isinstance(output_column, basestring):
            raise TypeError("output_column should be a string.")
        elif output_column[0].isdigit():
            raise ValueError("output_column %s starts with number.\n"
                             "It is not supported" % output_column)

        if output_column in etl_schema.feature_names:
            if not overwrite:
                raise ValueError("Column %s already existed and overwrite is False.\n"
                                 "please set overwrite=True if you meant to overwrite." % output_column)
            else:
                self._update_schema_for_overwrite(etl_schema, output_column)

        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        if not isinstance(test_fold_id, int):
            raise TypeError("test_fold_id should be an integer.")
        elif test_fold_id > k or test_fold_id < 1:
            raise ValueError("test_fold_id is %s. It should in the range of [1, %s]" % (test_fold_id, k))

        if not isinstance(split_name, list):
            raise TypeError("split_name should be a list.")
        elif len(split_name) != 2:
            raise ValueError("The size of split_name is %s. The supported size is 2." % len(split_name))


        args = get_pig_args('pig_kfold_split.py')

        args += ['-it', self.table_name,
                 '-ot', self.table_name,
                 '-k', str(k),
                 '-ic', fold_id_column,
                 '-f', str(test_fold_id),
                 '-r', str(randomize),
                 '-n', str(split_name),
                 '-oc', output_column,
                 '-fn', feature_names_as_str,
                 '-ft', feature_types_as_str,]

        #print ' '.join(map(str,args))
        logger.debug(args)
        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Failed to run kfold_split')

        if randomize:
            etl_schema.feature_names.append(fold_id_column)
            etl_schema.feature_types.append('float')
        etl_schema.feature_names.append(output_column)
        etl_schema.feature_types.append('chararray')
        etl_schema.save_schema(self.table_name)


    def percent_split(self, randomization_column, split_percent, split_name, output_column, update, overwrite):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        randomize = False
        if update or (randomization_column not in etl_schema.feature_names):
            randomize = True

        if update:
            self._update_schema_for_overwrite(etl_schema, randomization_column)

        if not isinstance(randomization_column, basestring):
            raise TypeError("randomization_column should be a string.")
        elif randomization_column[0].isdigit():
            raise ValueError("randomization_column %s starts with number.\n"
                             "It is not supported." % randomization_column)

        if not isinstance(output_column, basestring):
            raise TypeError("output_column should be a string.")
        elif output_column[0].isdigit():
            raise ValueError("output_column %s starts with number.\n"
                             "It is not supported." % output_column)

        if output_column in etl_schema.feature_names:
            if not overwrite:
                raise ValueError("Column %s already existed and overwrite is False.\n"
                                 "please set overwrite=True if you meant to overwrite." % output_column)
            else:
                self._update_schema_for_overwrite(etl_schema, output_column)

        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        if not isinstance(split_name, list):
            raise TypeError("split_name should be a list.")
        elif not isinstance(split_percent, list):
            raise TypeError("split_percent should be a list.")
        elif len(split_percent) != len(split_name):
            raise ValueError("The size of split_percent is %s. The size of split_name is %s. "
                             "Please make sure they are with the same size" %(len(split_percent), len(split_name) ))

        percent_sum = sum(split_percent)
        if sum(split_percent) != 100:
            raise ValueError("Sum of segement percentages is %s. It should be 100." % percent_sum)


        args = get_pig_args('pig_percent_split.py')

        args += ['-it', self.table_name,
                 '-ot', self.table_name,
                 '-ic', randomization_column,
                 '-r', str(randomize),
                 '-p', str(split_percent),
                 '-n', str(split_name),
                 '-oc', output_column,
                 '-fn', feature_names_as_str,
                 '-ft', feature_types_as_str,]

        #print ' '.join(map(str,args))
        logger.debug(args)
        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Failed to run percent_split')

        if randomize:
            etl_schema.feature_names.append(randomization_column)
            etl_schema.feature_types.append('float')
        etl_schema.feature_names.append(output_column)
        etl_schema.feature_types.append('chararray')
        etl_schema.save_schema(self.table_name)


    def copy(self, new_table_name, feature_names, feature_types):
        args = get_pig_args('pig_copy_table.py')

        args += ['-i', self.table_name, '-o', new_table_name,
                 '-n', feature_names, '-t', feature_types]

        return_code = call(args, report_strategy = etl_report_strategy())
        if return_code:
            raise HBaseTableException('Could not copy table')

        return HBaseTable(new_table_name, self.file_name)

    def project(self, new_table_name, features_to_project_names, features_to_project_types, renamed_feature_names):
        builder = PigScriptBuilder()
        relation = "project_relation"

        pig_schema = pig_helpers.get_pig_schema_string(','.join(features_to_project_names), ','.join(features_to_project_types))
        builder.add_load_statement(relation, HBaseSource(self.table_name), HBaseLoadFunction(features_to_project_names, True), 'key:chararray,' + pig_schema)
        builder.add_store_statement(relation, HBaseSource(new_table_name), HBaseStoreFunction(renamed_feature_names))

        args = get_pig_args('pig_execute.py')
        args += ['-s', builder.get_statements()]
        pig_report = PigJobReportStrategy()
        return_code = call(args, report_strategy = [etl_report_strategy(), pig_report])

        save_table_properties_from_pig_report(ETLSchema(),  pig_report, new_table_name)

        if return_code:
            raise HBaseTableException('Could not project table')

        return HBaseTable(new_table_name, self.file_name)

    def drop(self, filter, column, isregex, inplace, new_table_name):

        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        args = get_pig_args('pig_filter.py')

	if (inplace):
	    hbase_table_name = self.table_name
            hbase_table = HBaseTable(hbase_table_name, self.file_name)
	else:
             # need to delete/create output table so that we can write the remaining rows after filtering
             hbase_table_name = _create_table_name(new_table_name, True)
             hbase_table = HBaseTable(hbase_table_name, self.file_name)
             with ETLHBaseClient() as hbase_client:
                 hbase_client.drop_create_table(hbase_table_name,
                                           [config['hbase_column_family']])

        args.extend(['-i', self.table_name,
                 '-o', hbase_table_name,
		 '-n', feature_names_as_str,
                 '-t', feature_types_as_str,
		 '-p', 'True' if inplace else 'False',
		 '-c', column,
		 '-r', 'True' if isregex else 'False',
		 '-f', filter])

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not drop rows using the filter')

        if not inplace:
            new_etl_schema = ETLSchema()
            new_etl_schema.populate_schema(etl_schema.get_schema_as_str())
            new_etl_schema.save_schema(hbase_table_name)
            hbase_registry.register(new_table_name, hbase_table_name, True)

        return hbase_table


    def __get_column_statistics_filenames(self, column, check_file_existance=True):
        if column.profile and column.profile.data_intervals:
            md5sum = hashlib.md5(column.profile.get_interval_groups_as_str()).hexdigest()
            column_file_identifier = column.name + '_' + md5sum
        else:
            column_file_identifier = column.name

        hist_file = '%s_%s_histogram' % (self.table_name, column_file_identifier)
        hist_all_file = '%s_%s_histogram' % (self.table_name, column.name)
        stat_file = '%s_%s_stats' % (self.table_name, column.name)

        files_exist, can_rebuild_cache = False, False
        if check_file_existance and \
           os.path.isfile(hist_file) and os.path.isfile(stat_file):
             files_exist = True
        if check_file_existance and \
           os.path.isfile(hist_all_file):
             can_rebuild_cache = True
        return hist_file, stat_file, files_exist, hist_all_file, can_rebuild_cache

    def __plot_column_distribution(self, column_name, hist_file, text_file):

        return histogram.plot_histogram(hist_file,
                           column_name, 'frequency',
                           'Column Statistics - %s' % (column_name),
                           text_file)

    def __create_hist_stat_file_from_all_data(self, hist_all_file, hist_file, stat_file, feature_type, intervals):
        with open(hist_all_file) as h:
            hlines = [x.strip() for x in h.readlines()]
        stats,data_x,data_y = [],[],[]

        pig_to_python_type = {"int" : "int", "float" : "float", "long" : "long",
                              "double" : "float", "chararray" : "str", "bytearray" : "str"}
        python_feature_type = pig_to_python_type[feature_type]

        for i in range(len(hlines)):
            t = hlines[i].split('\t')
            if len(t) == 1:
                stats.append('missing_values=' + t[0])
            else:
                data_x.append(eval(python_feature_type)(t[0]))
                data_y.append(int(t[1]))


        if feature_type in ['int', 'float', 'long', 'double']:
            stats.append('max=%f' % max(data_x))
            stats.append('min=%f' % min(data_x))
            #http://stackoverflow.com/questions/2413522/weighted-standard-deviation-in-numpy
            average = numpy.average(data_x,weights=data_y)
            variance = numpy.average((data_x-average)**2, weights=data_y)
            stdev = math.sqrt(variance) 
            stats.append('avg=%f' % average)
            stats.append('var=%f' % variance)
            stats.append('stdev=%f' % stdev)

        stats.append('unique_values=%d' % len(data_x))
        with open(stat_file, 'w') as f:
            f.write("\n".join(stats))

        if intervals:
            interval_dict = {}
            for j in intervals:
                interval_dict[str(j)] = 0
 
            for i,x in enumerate(data_x):
                for j in intervals:
                    if x in j:
                        interval_dict[str(j)] += data_y[i]

            with open(hist_file, 'w') as f:
                for key,value in interval_dict.iteritems():
                    f.write("%s %d\n" % (key,value))
            

    def get_column_statistics(self, column_list, force_recomputation):
        """
        Parameters
        ----------
        column_list: List of BigColumn instances
            BigColumns for which statistics need to be computed
        force_recomputation: Boolean
            force recomputation of all statistics - recreate cache

        Returns
        -------
        result: List
            list of statistics for each BigColumn
        """

        result = []
        ColumnStat = collections.namedtuple('ColumnStat','names types intervals inmemory hist_files stat_files hist_all_files interval_list')

        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        
        recompute_columns = False

        def erase_cache(files):
            for file in files:
                if exists(file):
                    rmr(file)
                if os.path.isfile(file):
                    os.remove(file)
        
        for i in column_list:
            hfile,sfile,use_cache,hist_all_file,rebuild_cache = self.__get_column_statistics_filenames(i, not force_recomputation)
            if not use_cache:
                if not force_recomputation and rebuild_cache:
                    erase_cache([sfile])
                    self.__create_hist_stat_file_from_all_data(hist_all_file, hfile, sfile, 
                                                               etl_schema.get_feature_type(i.name), i.profile.data_intervals if i.profile else "")
                    result.append(self.__plot_column_distribution(i.name, hfile, sfile))
                else:
                    recompute_columns = ColumnStat([],[],[],[],[],[],[],[]) if not recompute_columns else recompute_columns
                    recompute_columns.names.append(i.name)
                    recompute_columns.types.append(etl_schema.get_feature_type(i.name))
                    recompute_columns.intervals.append(i.profile.get_interval_groups_as_str() if i.profile else "")
                    recompute_columns.inmemory.append(str(i.profile.in_memory_computation) if i.profile else str(True))
                    recompute_columns.hist_files.append(hfile)
                    recompute_columns.stat_files.append(sfile)
                    recompute_columns.hist_all_files.append(hist_all_file)
                    recompute_columns.interval_list.append(i.profile.data_intervals if i.profile else False)
                    erase_cache([hfile,sfile,hist_all_file])
            else:
               result.append(self.__plot_column_distribution(i.name, hfile, sfile))
            
        # Send to Pig only columns which need recomputation
        if recompute_columns:

            feature_names_as_str = ",".join(recompute_columns.names)
            feature_types_as_str = ",".join(recompute_columns.types)
            # Using # as delimiter as string representation of interval contains commas
            feature_data_groups_as_str = "#".join(recompute_columns.intervals)
            in_memory_optimization = ",".join(recompute_columns.inmemory)

            args = get_pig_args('pig_column_stats.py')
            args.extend(['-i', self.table_name, 
                    '-n', feature_names_as_str,
                    '-t', feature_types_as_str,
                    '-g', feature_data_groups_as_str,
                    '-o', in_memory_optimization])
    
            return_code = call(args, report_strategy = etl_report_strategy())
            if return_code:
                raise HBaseTableException('Could not generate statistics')

               
            for i in range(len(recompute_columns.hist_files)):
                all_file = recompute_columns.hist_all_files[i]
                hfile,sfile =  recompute_columns.hist_files[i], recompute_columns.stat_files[i]
                if eval(recompute_columns.inmemory[i]):
                    if exists('%s' % (all_file)):
                        update_cached_files(all_file)
                    self.__create_hist_stat_file_from_all_data(all_file,
                                                               hfile,
                                                               sfile,
                                                               recompute_columns.types[i],
                                                               recompute_columns.interval_list[i])

                if recompute_columns.interval_list[i] and exists('%s' % (hfile)):
                    update_cached_files(hfile)
                if exists('%s' % (sfile)):
                    update_cached_files(sfile)
                result.append(self.__plot_column_distribution(recompute_columns.names[i], hfile, sfile))


        return result 


    def drop_columns(self, columns):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        list_columns_to_drop = columns.split(',')
        bad_columns = "; ".join(["Column '{0}' not in frame"
                                 .format(c) for c in list_columns_to_drop
                                 if c not in etl_schema.feature_names])
        if len(bad_columns) > 0:
            raise Exception(bad_columns)

        args = []

        args += ['hadoop',
                 'jar',
                 global_config['intel_analytics_jar'],
                 global_config['column_dropper_class'],
                 '-t', self.table_name,
                 '-n', columns,
                 '-f', re.sub(':', '', global_config['hbase_column_family'])
                 ]

        return_code = call(args, report_strategy=MapOnlyProgressReportStrategy())
        if return_code:
            raise HBaseTableException('Could not drop columns from the table')

        # save the schema for the new table
        new_feature_names = []
        new_feature_types = []

        for feature in etl_schema.feature_names:
            if feature not in list_columns_to_drop:
                new_feature_names.append(feature)
                new_feature_types.append(etl_schema.feature_types[etl_schema.feature_names.index(feature)])

        etl_schema.feature_names = new_feature_names
        etl_schema.feature_types = new_feature_types
        etl_schema.save_schema(self.table_name)

    def _peek(self, n):

        if n < 0:
            raise HBaseTableException('A range smaller than 0 is specified')

        if n == 0:
            return []

        first_N_rows = []

        with ETLHBaseClient() as hbase_client:
           table = hbase_client.connection.table(self.table_name)
           nrows_read = 0
           for key, data in table.scan():
               orderedData = collections.OrderedDict(sorted(data.items()))
               first_N_rows.append(orderedData)
               nrows_read+=1
               if nrows_read >= n:
                   break
        return first_N_rows

    def inspect(self, n=10):

        first_N_rows = self._peek(n)
        schema = self.get_schema()
        columns = schema.keys()
        column_array = []
        print("--------------------------------------------------------------------")
        for i, column in enumerate(columns):
            header = re.sub("^" + config['hbase_column_family'],'',column)
            column_array.append(header)

        print "\t".join(column_array)
        print("--------------------------------------------------------------------")

        for orderedData in first_N_rows:
           data = []
           for col in column_array:
               col = config['hbase_column_family'] + col
               if col in orderedData and orderedData[col] != '' and orderedData[col] is not None:
                   data.append(orderedData[col])
               else:
                   data.append("NA")

           print "  |  ".join(data)

    def inspect_as_html(self, nRows=10):
        first_N_rows = self._peek(nRows)
        html_table='<table border="1">'

        schema = self.get_schema()
        columns = schema.keys()
        column_array = []
        html_table+='<tr>'
        for i, column in enumerate(columns):
            header = re.sub("^" + config['hbase_column_family'],'',column)
            column_array.append(header)
            html_table+='<th>%s</th>' % header
        html_table+='</tr>'

        for orderedData in first_N_rows:
           html_table+='<tr>'
           for col in column_array:
               col = config['hbase_column_family'] + col
               if col in orderedData and orderedData[col] != '' and orderedData[col] is not None:
                   html_table+=("<td>%s</td>" % (orderedData[col]))
               else:
                   html_table+='<td>NA</td>'

           html_table+='</tr>'

        html_table+='</table>'
        return html_table

    def __drop(self, output_table, column_name=None, how=None, replace_with=None):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()

        if column_name and (column_name not in etl_schema.feature_names):
            raise HBaseTableException("Column %s does not exist" % (column_name))

        args = get_pig_args('pig_clean.py')

        args += ['-i', self.table_name, '-o', output_table,
                 '-n', feature_names_as_str, '-t', feature_types_as_str]

        if replace_with:
            args += [ '-r' , replace_with]

        if column_name:
            args += ['-f', column_name]
        else:
            if not how:
                raise HBaseTableException('Please specify a cleaning strategy with the how argument')
            args += ['-s', how]

        # need to delete/create output table so that we can write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(output_table,
                                           [config['hbase_column_family']])

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not clean the dataset')

        hbase_registry.replace_value(self.table_name, output_table)

        self.table_name = output_table  # update table_name
        etl_schema.save_schema(self.table_name)  # save schema for new table

    def dropna(self, how='any', column_name=None, inplace=True):
        if inplace:
            columns = []
            etl_schema = ETLSchema()
            etl_schema.load_schema(self.table_name)

            def get_drop_stmt_for_column(col):
                return "%s == ''" % (col) if etl_schema.get_feature_type(col) in ["chararray","bytearray"] else "%s is null" % (col)
            if column_name:
                columns.append(column_name)
            else:
                columns.extend(etl_schema.get_feature_names_as_CSV().split(','))

            drop_stmts = [get_drop_stmt_for_column(x) for x in columns]
            drop_statement = ''
            if how == 'all':
                drop_statement= " AND ".join(drop_stmts)
            else:
                drop_statement = " OR ".join(drop_stmts)

            self.drop(drop_statement, '', False, True, '')
        else:
            frame_name = hbase_registry.get_key(self.table_name)
            output_table = _create_table_name(frame_name, True)
            self.__drop(output_table, column_name=column_name, how=how, replace_with=None)

    def fillna(self, column_name, value):
        frame_name = hbase_registry.get_key(self.table_name)
        output_table = _create_table_name(frame_name, True)
        self.__drop(output_table, column_name=column_name, how=None, replace_with=value)

    def impute(self, column_name, how):
        frame_name = hbase_registry.get_key(self.table_name)
        output_table = _create_table_name(frame_name, True)
        if how not in available_imputations:
            raise HBaseTableException('Please specify a support imputation method. %s is not supported' % (str(how)))
        self.__drop(output_table, column_name=column_name, how=None, replace_with=Imputation.to_string(how))

    def get_schema(self):
        """
        Returns the list of column names/types
        """
        columns = {}
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        for i, column_name in enumerate(etl_schema.feature_names):
            columns[column_name] = etl_schema.feature_types[i]
        return columns

    def join(self,
             right=None,
             how='left',
             left_on=None,
             right_on=None,
             suffixes=None,
             join_frame_name='',
             overwrite=False):

        """
        Perform SQL JOIN on the given list of HBaseTable(s), similar to pandas.DataFrame.join

        Parameters
        ----------
        right : List
            List of HBaseTable(s) to be joined, can be itself
        left_on : String
            String of columnes from left table, space or comma separated
            e.g., 'c1,c2' or 'b2 b3'
        right_on : List
            List of strings, each of which is in comma separated indicating
            columns to be joined corresponding to the list of tables as 
            the 'right', e.g., ['c1,c2', 'b2 b3']
        how : String
            The type of join, INNER, OUTER, LEFT, RIGHT
        suffixes : List
            List of strings, each of which is used as suffix to the column
            names from left and right of the join, e.g. ['_x', '_y1', '_y2'].
            Note the first one is always for the left
        join_frame_name : String
            Output BigDataFrame name

        Returns
        -------
        frame : BigDataFrame

        """

        if not (right and isinstance(right, list) and \
                all(isinstance(ht, HBaseTable) for ht in right)):
            raise HBaseTableException("Error! Invalid input 'right' %s, type %s!" \
                                      % (right, type(right)))

        # allowed join types: python outer is actually full
        if not how.lower() in ['inner', 'outer', 'left', 'right']:
            raise HBaseTableException("Error! Invalid input 'how' %s, type %s!" \
                                      % (how, type(how)))

        if not left_on:
            raise HBaseTableException("Error! Invalid input 'left_on' %s, type %s!" \
                                      % (left_on, type(left_on)))

        if not (right_on and isinstance(right_on, list) and \
                (len(right_on) == len(right))):
            raise HBaseTableException("Error! Invalid input 'right_on' %s, type %s!" \
                                      % (right_on, type(right_on)))

        if not (suffixes and isinstance(suffixes, list) and \
                (len(suffixes) == (len(right) + 1))):
            raise HBaseTableException("Error! Invalid input 'suffixes' %s, type %s!" \
                                      % (suffixes, type(suffixes)))

        # delete/create output table to write the joined features
        if not join_frame_name:
            raise HBaseTableException('In-place join is currently not supported')

        # in-place?
        join_table_name = _create_table_name(join_frame_name, overwrite=overwrite)
        try:
            with ETLHBaseClient() as hbase_client:
                hbase_client.drop_create_table(join_table_name, [config['hbase_column_family']])
        except KeyError:
            raise KeyError("Could not create output table for '" + output + "'")

        # prepare the script
        tables = [self.table_name]
        tables.extend([x.table_name for x in right])
        on = [left_on]
        on.extend(right_on)
        pig_builder = PigScriptBuilder()
        join_pig_script, join_pig_schema = pig_builder.get_join_statement(ETLSchema(),      \
                                                                          tables=tables,    \
                                                                          how=how.lower(),  \
                                                                          on=on,            \
                                                                          suffixes=suffixes,\
                                                                          join_table_name=join_table_name)

        # FIXME: move the script name, path to a class container instead of hardcoding it
        args = get_pig_args('pig_execute.py')
        args += ['-s', join_pig_script]

        try:
            join_pig_report = PigJobReportStrategy();
            return_code = call(args, report_strategy=[etl_report_strategy(), join_pig_report])
            if return_code:
                raise HBaseTableException('Failed to join data.')
        except:
            raise HBaseTableException('Could not join frame')

        # the schema is populated now
        join_table_properties = {}
        join_table_properties[MAX_ROW_KEY] = join_pig_report.content['input_count']
        join_etl_schema = ETLSchema()
        join_etl_schema.populate_schema(join_pig_schema)
        join_etl_schema.save_schema(join_table_name)
        save_table_properties_from_pig_report(join_etl_schema, join_pig_report, join_table_name)

        # save the table name
        hbase_registry.register(join_frame_name, join_table_name, overwrite=overwrite)

        # file name is fake, for information purpose only
        join_file_name = 'joined from ' + ', '.join(tables)
        return BigDataFrame(join_frame_name, HBaseTable(join_table_name, join_file_name))

    def flatten(self, column_name, new_frame_name, string_split_options=StringSplitOptions()):
        """
        Flatten a column with a list of values into multiple rows

        For example,

          | Input:
          |    1 a,b,c
          |    2 b
          |    3 c
          |
          | "Flattened" Output:
          |    1 a
          |    1 b
          |    1 c
          |    2 b
          |    3 c


        Parameters
        ----------
        column_name : String
            The column containing delimited values.
        new_frame_name : String
            The name of the new frame to be created. If this frame already exists, it will be overwritten.
        string_split_options : StringSplitOptions, optional
            The options for how to split the delimited values.  Default is comma delimited and trim whitespace.

        Returns
        -------
        BigDataFrame
            The newly created frame.

        Examples
        --------
        >>> string_split_options = StringSplitOptions()
        >>> string_split_options.delimiter = '|'
        >>> string_split_options.trim_whitespace = False
        >>>
        >>> flattened_frame = frame.flatten('column_to_flatten', 'new_frame_name', string_split_options)
        """
        if not column_name:
            raise HBaseTableException("column_name can't be empty")
        if not new_frame_name:
            raise HBaseTableException("new_frame_name can't be empty")

        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        if not column_name in etl_schema.feature_names:
            raise HBaseTableException("column name was not found: " + column_name)

        frame_name = hbase_registry.get_key(self.table_name)
        output_table = _create_table_name(frame_name, True)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(output_table,
                                           [config['hbase_column_family']])

        etl_schema.save_schema(output_table)

        pig_builder = FlattenScriptBuilder()
        script = pig_builder.build_script(self.table_name, output_table, etl_schema, column_name, string_split_options)

        cmd = get_pig_args('pig_execute.py')
        cmd += ['-s', script]

        return_code = call(cmd, report_strategy=etl_report_strategy())

        if return_code:
            try:
                with ETLHBaseClient() as hbase_client:
                    hbase_client.delete_table(output_table)
            except:
                logger.error("Could not delete table: " + output_table)
            raise Exception('Could not flatten frame ' + return_code)

        hbase_registry.register(new_frame_name, output_table, True)

        return BigDataFrame(new_frame_name, HBaseTable(output_table, self.file_name))


    @classmethod
    def delete_table(cls, victim_table_name):
        with ETLHBaseClient() as hbase_client:
            hbase_client.delete_table(victim_table_name)
            #clean the schema entry used by the old table
            schema_table = config['hbase_schema_table']
            row = hbase_client.get(schema_table, victim_table_name)
            if len(row) > 0:
                hbase_client.delete(schema_table, victim_table_name)

class HBaseRegistry(Registry):
    """
    Registry to map HBase table names and also handle garbage collection
    """

    def __init__(self, filename):
        super(HBaseRegistry, self).__init__(filename)

    def register(self, key, table_name, overwrite=False, append=False, delete_table=False):
        """
        Registers an HBaseTable name with key and does table garbage collection

        If append=True, don't delete anything.
        Otherwise:

        If key is already being used in the registry:
            If overwrite=True, then currently registered table is deleted from
                                    HBase and the new key-name is registered
            If overwrite=False, exception is raised

        If table_name is already being used in the registry:
            If delete_table=True, then table is deleted from HBase
        """
        if not append:
            # test if reusing key
            try:
                tmp = self.get_value(key)
            except:
                pass
            else:
                if not overwrite:
                    raise Exception("Big item '" + key + "' already exists.")
                HBaseTable.delete_table(tmp)
            # test if reusing table_name
            if delete_table and self.has_value(table_name):
                HBaseTable.delete_table(table_name)

        super(HBaseRegistry, self).register(key, table_name)

    def unregister_key(self, key, delete_table=False):
        name = self.get_value(key)
        if name and delete_table:
            HBaseTable.delete_table(name)
        super(HBaseRegistry, self).unregister_key(key)

    def unregister_value(self, value, delete_table=False):
        key = self.get_key(value)
        if key and delete_table:
            HBaseTable.delete_table(value)
        super(HBaseRegistry, self).unregister_value(value)

    def replace_value(self, victim, replacement, delete_table=False):
        key = self.get_key(victim)
        if not key:
            raise("Internal error: no key found for big item")
        HBaseTable.delete_table(victim)
        if delete_table and self.get_key(replacement):
            HBaseTable.delete_table(replacement)
        super(HBaseRegistry, self).replace_value(victim, replacement)


class HBaseFrameBuilder(FrameBuilder):
    def copy_data_frame(self, data_frame, new_frame_name, overwrite=False):
        """
        Create a new data frame and copy data from source data frame to the new data frame
        Parameters
        ----------
        data_frame : BigDataFrame
            source data frame
        new_frame_name : String
            name for the new data frame
        overwrite : Boolean
            overwrite existing big data frame

        Examples
        --------
        >>> new_frame = fb.copy_data_frame(source_data_frame, "new_data_frame")

        """

        new_table_name = _create_table_name(new_frame_name, overwrite)
        # need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])

        etl_schema = ETLSchema()
        etl_schema.load_schema(data_frame._table.table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()
        new_table = data_frame._table.copy(new_table_name, feature_names_as_str, feature_types_as_str)
        etl_schema.save_schema(new_table_name)
        hbase_registry.register(new_frame_name, new_table_name, overwrite)
        return BigDataFrame(new_frame_name, new_table)


    def project(self, data_frame, new_frame_name, features_to_project, rename=None, overwrite=False):

        if not rename:
            rename = {}

        new_table_name = _create_table_name(new_frame_name, overwrite)
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])

        etl_schema = ETLSchema()
        etl_schema.load_schema(data_frame._table.table_name)

        non_found = []
        for target_feature in features_to_project:
            if target_feature not in etl_schema.feature_names:
                non_found.append('ERROR: feature ' + target_feature + ' is invalid')

        for target_feature in rename:
            if target_feature not in etl_schema.feature_names:
                non_found.append('ERROR: feature ' + target_feature + ' is invalid')

        if len(non_found) > 0:
            raise Exception('\n'.join(non_found))

        feature_names_types_mapping = {}
        for i in range(0, len(etl_schema.feature_names)):
            feature_names_types_mapping[etl_schema.feature_names[i]] = etl_schema.feature_types[i]

        feature_to_project_types = []
        for i in range(0, len(features_to_project)):
            feature_to_project_types.append(feature_names_types_mapping[features_to_project[i]])

        renamed_feature_names = [rename.get(name) or name for name in features_to_project]

        new_table = data_frame._table.project(new_table_name, features_to_project, feature_to_project_types, renamed_feature_names)

        new_table_schema = ETLSchema()
        new_table_schema.feature_names = renamed_feature_names
        new_table_schema.feature_types = feature_to_project_types
        new_table_schema.save_schema(new_table_name)

        hbase_registry.register(new_frame_name, new_table_name, overwrite)
        return BigDataFrame(new_frame_name, new_table)




    #-------------------------------------------------------------------------
    # Create BigDataFrames
    #-------------------------------------------------------------------------
    def _get_file_name_string_for_import(self, file_name):
        if not isinstance(file_name, basestring):
            file_name = ','.join(file_name)
        return file_name

    def check_error_info(self, pig_report):
        if '2118' in pig_report.error_codes:
            raise Exception('ERROR: Some of the specified file expressions have no matching files')



    def build_from_csv(self, frame_name, file_names, schema,
                       skip_header=False, overwrite=False):
        self._validate_exists(file_names)

        table_name = _create_table_name(frame_name, overwrite)

        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()
        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        args = get_pig_args('pig_import_csv.py')

        args += ['-i', file_names_as_csv, '-o', table_name,
                 '-f', feature_names_as_str, '-t', feature_types_as_str]

        if skip_header:
            args += ['-k']

        args += ['-m', '0']

        logger.debug(' '.join(args))
        # need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])

        pig_report = PigJobReportStrategy();
        return_code = call(args, report_strategy=[etl_report_strategy(), pig_report])

        self.check_error_info(pig_report)
        
        if return_code:
            raise Exception('Could not import CSV file')

        save_table_properties_from_pig_report(etl_schema, pig_report, table_name)

        hbase_table = HBaseTable(table_name, file_names_as_csv)
        hbase_registry.register(frame_name, table_name, overwrite)
        return BigDataFrame(frame_name, hbase_table)

    def append_from_csv(self, data_frame, file_names, schema, skip_header=False):
        self._validate_exists(file_names)
        new_data_etl_schema = ETLSchema()
        new_data_etl_schema.populate_schema(schema)
        new_data_feature_names_as_str = new_data_etl_schema.get_feature_names_as_CSV()
        new_data_feature_types_as_str = new_data_etl_schema.get_feature_types_as_CSV()

        args = get_pig_args('pig_import_csv.py')

        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        table_name = data_frame._table.table_name
        args += ['-i', file_names_as_csv, '-o', table_name,
                 '-f', new_data_feature_names_as_str, '-t', new_data_feature_types_as_str]

        if skip_header:
            args += ['-k']

        logger.debug(' '.join(args))
        # need to delete/create output table to write the transformed features
        try:
            self._append_data(args, new_data_etl_schema, table_name)
        except DataAppendException:
            raise Exception('Could not import CSV file')

        existing_etl_schema = ETLSchema()
        existing_etl_schema.load_schema(table_name)

        merged_schema = merge_schema([existing_etl_schema, new_data_etl_schema])
        merged_schema.save_schema(table_name)


    def build_from_json(self, frame_name, file_names, overwrite=False):
        self._validate_exists(file_names)

        #create some random table name
        #we currently don't bother the user to specify table names
        table_name = _create_table_name(frame_name, overwrite)
        hbase_table = HBaseTable(table_name, file_names)
        new_frame = BigDataFrame(frame_name, hbase_table)

        schema='json:chararray'#dump all records as chararray
        
        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)

        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        args = get_pig_args('pig_import_json.py')

        args += ['-i', file_names_as_csv, '-o', table_name]

        logger.debug(args)

#         need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])

        pig_report = PigJobReportStrategy();
        return_code = call(args, report_strategy=[etl_report_strategy(), pig_report])
        self.check_error_info(pig_report)
        
        if return_code:
            raise Exception('Could not import JSON file')

        save_table_properties_from_pig_report(etl_schema, pig_report, table_name)

        hbase_registry.register(frame_name, table_name, overwrite)
        return new_frame
            
    def append_from_json(self, data_frame, file_names):
        #create some random table name
        #we currently don't bother the user to specify table names

        #save the schema of the dataset to import
        self._validate_exists(file_names)
        etl_schema = ETLSchema()

        args = get_pig_args('pig_import_json.py')

        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        table_name = data_frame._table.table_name
        args += ['-i', file_names_as_csv, '-o', table_name]
        try:
            self._append_data(args, etl_schema, table_name)
        except DataAppendException:
            raise Exception('Could not import JSON file')


    def build_from_xml(self, frame_name, file_names, tag_name, overwrite=False):
        self._validate_exists(file_names)

        #create some random table name
        #we currently don't bother the user to specify table names
        table_name = _create_table_name(frame_name, overwrite)
        hbase_table = HBaseTable(table_name, file_names)
        new_frame = BigDataFrame(frame_name, hbase_table)

        schema='xml:chararray'#dump all records as chararray

        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)

        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        args = get_pig_args('pig_import_xml.py')

        args += ['-i', file_names_as_csv, '-o', table_name, '-tag', tag_name]

        logger.debug(args)

#         need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])

        pig_report = PigJobReportStrategy();
        return_code = call(args, report_strategy=[etl_report_strategy(), pig_report])
        self.check_error_info(pig_report)
        
        if return_code:
            raise Exception('Could not import XML file')

        save_table_properties_from_pig_report(etl_schema, pig_report, table_name)

        hbase_registry.register(frame_name, table_name, overwrite)

        return new_frame

    def append_from_xml(self, data_frame, file_names, tag_name):
        self._validate_exists(file_names)
        args = get_pig_args('pig_import_xml.py')

        file_names_as_csv = self._get_file_name_string_for_import(file_names)

        table_name = data_frame._table.table_name
        args += ['-i', file_names_as_csv, '-o', table_name, '-tag', tag_name]

        logger.debug(args)
        etl_schema = ETLSchema()
        try:
            self._append_data(args, etl_schema, table_name)
        except DataAppendException:
            raise Exception('Could not import XML file')

    def append_from_data_frame(self, target_data_frame, source_data_frames):

        source_names = []
        schemas = []
        for source_frame in source_data_frames:
            source_schema = ETLSchema()
            source_schema.load_schema(source_frame._table.table_name)
            schemas.append(source_schema)
            source_names.append(source_frame._table.table_name)

        merged_schema = merge_schema(schemas)

        pig_builder = PigScriptBuilder()
        target_table_name = target_data_frame._table.table_name
        script = pig_builder.get_append_tables_statement(ETLSchema(), target_table_name, source_names)

        args = get_pig_args('pig_execute.py')
        args += ['-s', script]

        try:
            self._append_data(args, merged_schema, target_table_name, is_args_final = True)
        except DataAppendException:
            raise Exception('Could not append to data frame')
        merged_schema.save_schema(target_table_name)

    def _append_data(self, args, etl_schema, table_name, is_args_final = False):
        properties = etl_schema.get_table_properties(table_name)
        original_max_row_key = properties[MAX_ROW_KEY]
        if not is_args_final:
            args += ['-m', original_max_row_key]

        pig_report = PigJobReportStrategy()
        return_code = call(args, report_strategy=[etl_report_strategy(), pig_report])
        self.check_error_info(pig_report)

        if return_code:
            raise DataAppendException('Failed to append data.')

        properties[MAX_ROW_KEY] = str(long(original_max_row_key) + long(pig_report.content['input_count']))
        etl_schema.save_table_properties(table_name, properties)

    def _validate_exists(self, file_names):
        """
        Check if a file exists either in HDFS, or locally, if is_local_run()

        Raise exception if file does NOT exist.
        """
        if isinstance(file_names, basestring):
            file_names = [file_names]

        not_found = []
        for name in file_names:
            if '*' in name:
                continue

            if is_local_run():
                if not os.path.isfile(name):
                    not_found.append('ERROR: File ' + name + ' does NOT exist locally')
            elif not exists_hdfs(name):
                    not_found.append('ERROR: File ' + name + ' does NOT exist in HDFS')

        if len(not_found) > 0:
            raise Exception('\n'.join(not_found))

    def join_data_frame(self, left, right, how, left_on, right_on, suffixes, join_frame_name, overwrite=False):
        """
        Joins a left BigDataFrame with a list of (right) BigDataFrame(s)
        """
        return left.join(right,     \
                         how=how,   \
                         left_on=left_on,   \
                         right_on=right_on, \
                         suffixes=suffixes, \
                         join_frame_name=join_frame_name, \
                         overwrite=overwrite);

# Move files to local filesystem for caching/plotting purposes
def update_cached_files(file):
    try:
        import pydoop.hadut as hadooputils
        g = lambda val: ['-getmerge', '%s' % (val), '%s' % (val)]
        hadooputils.dfs(g(file))
    except Exception as e:
        raise Exception('Error: pydoop failed to connect to HDFS ' + e.message)


def exists_hdfs(file_name):
    try:
        from pydoop.hdfs.path import exists
        return exists(file_name)
    except Exception as e:
        raise Exception('ERROR: Python unable to check HDFS: ' + e.message)

def get_pig_type(type):
    if type == "Integer":
        return "int"
    elif type == "Float":
        return "float"
    elif type == "Double":
        return "double"
    elif type == "Boolean":
        return "boolean"
    elif type == "Long":
        return "long"
    elif type == "String":
        return "chararray"
    else:
        return "bytearray"


class HBaseFrameBuilderFactory(object):
    def __init__(self):
        super(HBaseFrameBuilderFactory, self).__init__()

    def get_frame_builder(self):
        return HBaseFrameBuilder()

    def get_frame(self, frame_name):
        try:
            hbase_table_name = hbase_registry[frame_name]
        except KeyError:
            raise KeyError("Could not stored table for '" + frame_name + "'")
        return self._get_frame(frame_name, hbase_table_name)

    def get_frame_names(self):
        return hbase_registry.keys()

    def _get_frame(self, frame_name, hbase_table_name):
        hbase_table = HBaseTable(hbase_table_name, ': from database')
        new_frame = BigDataFrame(frame_name, hbase_table)
        return new_frame

    @staticmethod
    def get_instance():
        global hbase_frame_builder_factory
        return hbase_frame_builder_factory


#global singleton instance
hbase_frame_builder_factory = HBaseFrameBuilderFactory()
hbase_registry = HBaseRegistry(os.path.join(config['conf_folder'],
                                       config['hbase_names_file']))


def _create_table_name(name, overwrite):
    try:
        hbase_registry[name]
    except KeyError:
        pass
    else:
        if not overwrite:
            raise Exception("Big item '" + name  + "' already exists.")
    return name + get_time_str()

def save_table_properties_from_pig_report(etl_schema, pig_report, table_name):
    properties = {};
    properties[MAX_ROW_KEY] = pig_report.content['input_count']
    etl_schema.save_table_properties(table_name, properties)
