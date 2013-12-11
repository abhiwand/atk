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
    global_config as config, get_time_str
from intel_analytics.table.bigdataframe import BigDataFrame, FrameBuilder
from intel_analytics.table.builtin_functions import EvalFunctions
from schema import ETLSchema
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.logger import stdout_logger as logger
from intel_analytics.subproc import call

try:
    from intel_analytics.pigprogressreportstrategy import PigProgressReportStrategy as progress_report_strategy#depends on ipython
except ImportError, e:
    from intel_analytics.report import PrintReportStrategy as progress_report_strategy
        
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

logger.debug('$JYTHONPATH %s' % os.environ["JYTHONPATH"])

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

        if column_name and (column_name not in etl_schema.feature_names):
            raise HBaseTableException("Column %s does not exist" % column_name)

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

        return_code = call(args, report_strategy=progress_report_strategy())

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

    def _get_first_N(self, n):

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
    
    def head(self, n=10):

        first_N_rows = self._get_first_N(n)
        schema = self.get_schema()
        columns = schema.keys()
        column_array = []
        sys.stdout.write("--------------------------------------------------------------------\n")
        for i, column in enumerate(columns):
            header = re.sub(config['hbase_column_family'],'',column)
            column_array.append(header)
            sys.stdout.write("%s"%(header))
            if i != len(columns)-1:
                sys.stdout.write("\t")
        sys.stdout.write("\n--------------------------------------------------------------------\n")

        for orderedData in first_N_rows:

           for col in column_array:
               if col in orderedData and orderedData[col] != '' and orderedData[col] is not None:
                   sys.stdout.write("%s"%(orderedData[col]))
               else:
                   sys.stdout.write("NA")

               if col != column_array[-1]:
                   sys.stdout.write("  |  ")

           sys.stdout.write("\n")
               
    def to_html(self, nRows=10):
        first_N_rows = self._get_first_N(nRows)
        html_table='<table border="1">'

        schema = self.get_schema()
        columns = schema.keys()
        column_array = []
        html_table+='<tr>'
        for i, column in enumerate(columns):
            header = re.sub(config['hbase_column_family'],'',column)
            column_array.append(header)
            html_table+='<th>%s</th>' % header
        html_table+='</tr>'

        for orderedData in first_N_rows:
           html_table+='<tr>'
           for col in column_array:
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
        
        return_code = call(args, report_strategy=progress_report_strategy())

        if return_code:
            raise HBaseTableException('Could not clean the dataset')

        key = hbase_frame_builder_factory.\
            name_registry.get_key(self.table_name)
            
        try:
            HBaseTable.delete_table(self.table_name)
        except:
            raise HBaseTableException('Could not clean the dataset')

        self.table_name = output_table # update the table_name
        etl_schema.save_schema(self.table_name) # save the schema for the new table
        hbase_frame_builder_factory. \
            name_registry.register(key, self.table_name)

    def dropna(self, how='any', column_name=None):
        frame_name = hbase_frame_builder_factory.\
            name_registry.get_key(self.table_name)       
        output_table = _create_table_name(frame_name, True)
        self.__drop(output_table, column_name=column_name, how=how, replace_with=None)

    def fillna(self, column_name, value):
        frame_name = hbase_frame_builder_factory.\
            name_registry.get_key(self.table_name)        
        output_table = _create_table_name(frame_name, True)
        self.__drop(output_table, column_name=column_name, how=None, replace_with=value)

    def impute(self, column_name, how):
        frame_name = hbase_frame_builder_factory.\
            name_registry.get_key(self.table_name)        
        output_table = _create_table_name(frame_name, True)
        if how not in available_imputations:
            raise HBaseTableException('Please specify a support imputation method. %d is not supported' % (how))
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
            hbase_frame_builder_factory.\
                name_registry.unregister_value(victim_table_name)
            #clean the schema entry used by the old table
            schema_table = config['hbase_schema_table']
            row = hbase_client.get(schema_table, victim_table_name)
            if len(row) > 0:
                hbase_client.delete(schema_table, victim_table_name)



class HBaseFrameBuilder(FrameBuilder):

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

        return_code = call(args, report_strategy=progress_report_strategy())
        
        if return_code:
            raise Exception('Could not import CSV file')

        hbase_table = HBaseTable(table_name, file_name)
        self._register_table_name(frame_name, table_name, overwrite)
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
            
        return_code = call(args, report_strategy=progress_report_strategy())
        
        if return_code:
            raise Exception('Could not import JSON file')

        self._register_table_name(frame_name, table_name, overwrite)
        
        return new_frame
            
    def build_from_xml(self, frame_name, file_name, schema=None):
        raise Exception("Not implemented")

    def _register_table_name(self, frame_name, table_name, overwrite):
        tmp_name =  hbase_frame_builder_factory.name_registry[frame_name]
        if tmp_name is not None:
            if overwrite:
                HBaseTable.delete_table(tmp_name)
            else:
                raise Exception("Frame '" + frame_name
                                + "' already exists.")
        hbase_frame_builder_factory. \
            name_registry.register(frame_name, table_name)
        return table_name


class HBaseFrameBuilderFactory(object):
    def __init__(self):
        super(HBaseFrameBuilderFactory, self).__init__()
        table_names_file = os.path.join(config['conf_folder'],
                                        config['hbase_names_file'])
        #print "Initializing name registry for Frame Builder Factory with " \
        #      + table_names_file
        self.name_registry = Registry(table_names_file)

    def get_frame_builder(self):
        return HBaseFrameBuilder()

    def get_frame(self, frame_name):
        try:
            hbase_table_name = self.name_registry[frame_name]
        except KeyError:
            raise KeyError("Could not stored table for '" + frame_name + "'")
        return self._get_frame(frame_name, hbase_table_name)

    def get_frame_names(self):
        return self.name_registry.keys()

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


def _create_table_name(frame_name, overwrite):
    table_name =  hbase_frame_builder_factory.name_registry[frame_name]
    if table_name is not None:
        if not overwrite:
            raise Exception("Frame '" + frame_name  + "' already exists.")
    return frame_name + get_time_str()
