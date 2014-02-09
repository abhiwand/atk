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
import os
import re
import sys
import collections

from intel_analytics.config import Registry, \
    global_config as config, get_time_str, global_config
from intel_analytics.table.bigdataframe import BigDataFrame, FrameBuilder
from intel_analytics.table.builtin_functions import EvalFunctions
from schema import ETLSchema
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.logger import stdout_logger as logger
from intel_analytics.subproc import call
from intel_analytics.report import MapOnlyProgressReportStrategy


try:
    from intel_analytics.pigprogressreportstrategy import PigProgressReportStrategy as etl_report_strategy#depends on ipython
except ImportError, e:
    from intel_analytics.report import PrintReportStrategy as etl_report_strategy

#for quick testing
try:
    local_run = config['local_run'].lower().strip() == 'true'
except:
    local_run = False

base_script_path = os.path.dirname(os.path.abspath(__file__))
etl_scripts_path = config['pig_py_scripts']
pig_log4j_path = os.path.join(config['conf_folder'], 'pig_log4j.properties')
logger.debug('Using %s '% pig_log4j_path)
             
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"#to get rid of Jython logging
os.environ["JYTHONPATH"] = config['pig_jython_path']#required to ship jython scripts with pig


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

def _get_pig_args():
    args=['pig']
    if local_run:
        args += ['-x', 'local']
    args += ['-4', pig_log4j_path]
    return args

def _strictly_increasing(L):
    return all(a<b for a, b in zip(L, L[1:]))

def _strictly_decreasing(L):
    return all(a>b for a, b in zip(L, L[1:]))

def _make_range_spec(range):
    result = ""
    d = "-?[0-9]+\.?[0-9]*"                                     # decimal pattern
    n = "-?[0-9]+"                                              # number pattern
    if (range == "date" or range == "week" or range == "month" or range == "year" or range == "dayofweek" or range == "monthofyear"):
        result = range
	raise Exception('Unsupported feature of specifying time series as a range')
    elif (re.match("^%s:%s:%s$" % (n,n,n), range)):             # ranges of type [min:max:stepsize] for integers
        limits = [(f.strip()) for f in range.split(':')]
        min, max, stepsize = int(limits[0]), int(limits[1]), int(limits[2])
        if ((min >= max and stepsize > 0) or (min <= max and stepsize < 0)):
            raise Exception('Illegal range %s' % (range))
        for i in xrange(min, max, stepsize):
                result += "%d," % (i)
        result += "%d" % (max)
    elif (re.match("^%s:%s:%s$" % (d,d,d), range)):             # ranges for type [min:max:stepsize] for floating points
        limits = [(f.strip()) for f in range.split(':')]
        min, max, stepsize = float(limits[0]), float(limits[1]), float(limits[2])
        if ((min >= max and stepsize > 0) or (min <= max and stepsize < 0)):
            raise Exception('Illegal range %s' % (range))
        r = min
        while r < max:
            result += "%f," %(r)
            r += stepsize
        result += "%f" %(max)
    elif (re.match("^%s(,%s)+$" % (d,d),  range)):               # ranges of type a1,a2,a3,a4 ...  -- useful in custom stepsize
	L = [int(x) for x in range.split(',')]
	if (_strictly_increasing(L) or _strictly_decreasing(L)):
            result = range
	else:
	    raise Exception('Range should be strictly increasing or decreasing')
    elif (re.match("^%s$" % (d),  range)):	                 # range specified as depth of each bucket 
	raise Exception('Unsupported feature of specifying depth as a range')
    else:
	raise Exception('Unsupported feature for specifying range')


    return result


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

	new_schema_def = ""
	if (len(group_by_columns) == 1) :
	    new_schema_def += "AggregateGroup:" + etl_schema.get_feature_type(group_by_columns[0])
	else:
            new_schema_def += "AggregateGroup:chararray"
	
	aggregation_list = []
	for i in aggregation_arguments:
	    function_name = column_to_apply = new_column_name = ""
	    if isinstance(i, tuple):
	        if (len(i) == 2):
		    function_name,column_to_apply,new_column_name = i[0],i[1],i[1]
		else:
		    function_name,column_to_apply,new_column_name = i[0],i[1],i[2]
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
	

        script_path = os.path.join(etl_scripts_path, 'pig_aggregation.py')

        args = _get_pig_args()


	new_table_name = _create_table_name(aggregate_frame_name, overwrite)
        hbase_table = HBaseTable(new_table_name, self.file_name)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])


        args += [script_path,
                '-i', self.table_name,
                '-o', new_table_name, 
		'-a', " ".join(aggregation_list),
		'-g', ",".join(group_by_columns),
                '-u', feature_names_as_str, '-r', feature_types_as_str,
                ]

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')


	new_etl_schema = ETLSchema()
	new_etl_schema.populate_schema(new_schema_def)
        new_etl_schema.save_schema(new_table_name)

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

	aggregation_list = []
	for i in aggregation_arguments:
	    function_name = column_to_apply = new_column_name = ""
	    if isinstance(i, tuple):
	        if (len(i) == 2):
		    function_name,column_to_apply,new_column_name = i[0],i[1],i[1]
		else:
		    function_name,column_to_apply,new_column_name = i[0],i[1],i[2]
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
	
        script_path = os.path.join(etl_scripts_path, 'pig_range_aggregation.py')
        args = _get_pig_args()
        _range = _make_range_spec(range)


	new_table_name = _create_table_name(aggregate_frame_name, overwrite)
        hbase_table = HBaseTable(new_table_name, self.file_name)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])


        args += [script_path,
                '-i', self.table_name,
                '-o', new_table_name, 
		'-a', " ".join(aggregation_list),
		'-g', group_by_column,
		'-l', _range,
                '-u', feature_names_as_str, '-r', feature_types_as_str,
                ]

        logger.debug(args)

        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')


	new_etl_schema = ETLSchema()
	new_etl_schema.populate_schema(new_schema_def)
        new_etl_schema.save_schema(new_table_name)

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
            cols = re.split('\+|-|\*|\/|\%', re.sub('[()]', '', column_name))
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
        elif transformation == EvalFunctions.String.CONCAT:
            cols = column_name.split(',')
            if len(cols) < 2:
                raise HBaseTableException("Concatenation needs more than 1 input")
            for col in cols:
                if ((not ('\'' == col[0] and '\'' == col[len(col)-1])) and
                    col not in etl_schema.feature_names):
                    raise HBaseTableException("Column %s in expression %s does not exist" % (col, column_name))
        # single column
        elif column_name not in etl_schema.feature_names:
            raise HBaseTableException("Column %s does not exist" % column_name)

        if not column_name:
            column_name = '' #some operations does not requires a column name.


        script_path = os.path.join(etl_scripts_path, 'pig_transform.py')

        args = _get_pig_args()

        args += [script_path,
                '-f', column_name, '-i', self.table_name,
                '-o', self.table_name, '-t', transformation_to_apply,
                '-u', feature_names_as_str, '-r', feature_types_as_str,
                '-n', new_column_name]

        if transformation_args:  # we have some args that we need to pass to the transformation function
            args += ['-a', str(transformation_args)]

        if keep_source_column:
            args += ['-k']

        logger.debug(args)


        return_code = call(args, report_strategy=etl_report_strategy())

        if return_code:
            raise HBaseTableException('Could not apply transformation')

        #need to update schema here as it's difficult to pass the updated schema info from jython to python
        if not keep_source_column:
            etl_schema.feature_names.remove(column_name)
        etl_schema.feature_names.append(new_column_name)
        
        #for now make the new feature bytearray, because all UDF's have different return types
        #and we cannot know their return types
        etl_schema.feature_types.append('bytearray')
        etl_schema.save_schema(self.table_name)

    def copy(self, new_table_name, feature_names, feature_types):
        script_path = os.path.join(etl_scripts_path, 'pig_copy_table.py')
        args = _get_pig_args()

        args += [script_path,
                 '-i', self.table_name,
                 '-o', new_table_name,
                 '-n', feature_names,
                 '-t', feature_types]

        return_code = call(args, report_strategy = etl_report_strategy())
        if return_code:
            raise HBaseTableException('Could not copy table')

        return HBaseTable(new_table_name, self.file_name)


    def drop_columns(self, columns):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
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

        list_columns_to_drop = columns.split(',')

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

        script_path = os.path.join(etl_scripts_path, 'pig_clean.py')

        args = _get_pig_args()

        args += [script_path, '-i', self.table_name,
                         '-o', output_table, '-n', feature_names_as_str,
                         '-t', feature_types_as_str]

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

    def dropna(self, how='any', column_name=None):
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

    def register(self, key, table_name, overwrite=False, delete_table=False):
        """
        Registers an HBaseTable name with key and does table garbage collection

        If key is already being used in the registry:
            If overwrite=True, then currently registered table is deleted from
                                    HBase and the new key-name is registered
            If overwrite=False, exception is raised

        If table_name is already being used in the registry:
            If delete_table=True, then table is deleted from HBase
        """
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

        new_table_name = _create_table_name(new_frame_name, overwrite)
        # need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(new_table_name,
                                           [config['hbase_column_family']])

        etl_schema = ETLSchema()
        etl_schema.load_schema(data_frame._original_table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()
        new_table = data_frame._table.copy(new_table_name, feature_names_as_str, feature_types_as_str)
        etl_schema.save_schema(new_table_name)
        hbase_registry.register(new_frame_name, new_table_name, overwrite)
        return BigDataFrame(new_frame_name, new_table)

    #-------------------------------------------------------------------------
    # Create BigDataFrames
    #-------------------------------------------------------------------------
    def build_from_csv(self, frame_name, file_name, schema,
                       skip_header=False, overwrite=False):
        table_name = _create_table_name(frame_name, overwrite)

        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()
        
        script_path = os.path.join(etl_scripts_path,'pig_import_csv.py')

        args = _get_pig_args()

        args += [script_path, '-i', file_name, '-o', table_name,
             '-f', feature_names_as_str, '-t', feature_types_as_str]

        if skip_header:
            args += ['-k']

        logger.debug(' '.join(args))
        # need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])

        return_code = call(args, report_strategy=etl_report_strategy())
        
        if return_code:
            raise Exception('Could not import CSV file')

        hbase_table = HBaseTable(table_name, file_name)
        hbase_registry.register(frame_name, table_name, overwrite)
        return BigDataFrame(frame_name, hbase_table)

    def build_from_json(self, frame_name, file_name, overwrite=False):
        #create some random table name
        #we currently don't bother the user to specify table names
        table_name = _create_table_name(frame_name, overwrite)
        hbase_table = HBaseTable(table_name, file_name)
        new_frame = BigDataFrame(frame_name, hbase_table)

        schema='json:chararray'#dump all records as chararray
        
        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)

        script_path = os.path.join(etl_scripts_path,'pig_import_json.py')

        args = _get_pig_args()

        args += [script_path, '-i', file_name, '-o', table_name]

        logger.debug(args)
        
#         need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])
            
        return_code = call(args, report_strategy=etl_report_strategy())
        
        if return_code:
            raise Exception('Could not import JSON file')

        hbase_registry.register(frame_name, table_name, overwrite)
        
        return new_frame
            
    def build_from_xml(self, frame_name, file_name, tag_name, overwrite=False):
        #create some random table name
        #we currently don't bother the user to specify table names
        table_name = _create_table_name(frame_name, overwrite)
        hbase_table = HBaseTable(table_name, file_name)
        new_frame = BigDataFrame(frame_name, hbase_table)

        schema='xml:chararray'#dump all records as chararray
        
        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(table_name)

        script_path = os.path.join(etl_scripts_path,'pig_import_xml.py')

        args = _get_pig_args()

        args += [script_path, '-i', file_name, '-o', table_name, '-tag', tag_name]

        logger.debug(args)
        
#         need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           [config['hbase_column_family']])
            
        return_code = call(args, report_strategy=etl_report_strategy())
        
        if return_code:
            raise Exception('Could not import XML file')

        hbase_registry.register(frame_name, table_name, overwrite)
        
        return new_frame


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
