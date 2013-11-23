import os
import subprocess
import re
import random
import string
import sys
import collections

from intel_analytics.config import global_config as config
from intel_analytics.table.bigdataframe import BigDataFrame
from intel_analytics.table.framebldr import FrameBuilder
from intel_analytics.table.builtin_functions import EvalFunctions
from schema import ETLSchema
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.logger import stdout_logger as logger

#for quick testing
local_run = True
DATAFRAME_NAME_PREFIX_LENGTH=5 #table name prefix length, names are generated with a rand prefix
base_script_path = os.path.dirname(os.path.abspath(__file__))
feateng_home = os.path.join(base_script_path, '../','..', 'feateng')
etl_scripts_path = config['pig_py_scripts']
pig_log4j_path = os.path.join(os.environ['SOURCE_CODE'], 'IntelAnalytics', 'conf','pig_log4j.properties')
logger.debug('Using %s '% pig_log4j_path)
             
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"#to get rid of Jython logging
os.environ["JYTHONPATH"] = os.path.join(os.environ['SOURCE_CODE'], 'IntelAnalytics', 'intel_analytics')#required to ship jython scripts with pig

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
    def __init__(self, table_name):
        """
        (internal constructor)
        Parameters
        ----------
        connection : happybase.Connection
            connection to HBase
        table_name : String
            name of table in Hbase
        """
        self.table_name = table_name
        # TODO : Hard-coded column family name must be removed later and
        #  read from Table
        #self.column_family_name = config['hbase_column_family']
        #self.connection = connection

    def transform(self,
                  column_name,
                  new_column_name,
                  transformation,
                  keep_source_column=False,
                  transformation_args=None):
        transformation_to_apply = EvalFunctions.to_string(transformation)

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

        return_code = subprocess.call(args)

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

    def head(self, n=10):
        with ETLHBaseClient() as hbase_client:
           table = hbase_client.connection.table(self.table_name)
           header_printed = False
           nrows_read = 0
           for key, data in table.scan():
               orderedData = collections.OrderedDict(sorted(data.items()))
               columns = orderedData.keys()
               items = orderedData.items()
               if not header_printed:
                   sys.stdout.write("--------------------------------------------------------------------\n")
                   for i, column in enumerate(columns):
                       sys.stdout.write("%s"%(re.sub(config['hbase_column_family'],'',column)))
                       if i != len(columns)-1:
                           sys.stdout.write("\t")
                   sys.stdout.write("\n--------------------------------------------------------------------\n")
                   header_printed = True

               for i,(column,value) in enumerate(items):
                   if value == '' or value==None:
                       sys.stdout.write("NA")
                   else:
                       sys.stdout.write("%s"%(value))
                   if i != len(items)-1:
                       sys.stdout.write("  |  ")
               sys.stdout.write("\n")
               nrows_read+=1
               if nrows_read >= n:
                   break

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
        
        return_code = subprocess.call(args)

        if return_code:
            raise HBaseTableException('Could not clean the dataset')

        #record old table name for cleaning
        old_table_name = self.table_name
        self.table_name = output_table # update the table_name
        etl_schema.save_schema(self.table_name) # save the schema for the new table
        #clean the old table
        with ETLHBaseClient() as hbase_client:
            hbase_client.delete_table(old_table_name)
            #clean the schema entry used by the old table
            schema_table = config['hbase_schema_table']
            row = hbase_client.get(schema_table, old_table_name)
            if len(row) > 0:
                hbase_client.delete(schema_table, old_table_name)

    def dropna(self, how='any', column_name=None):
        output_table = self.table_name + "_dropna"
        self.__drop(output_table, column_name=column_name, how=how, replace_with=None)

    def fillna(self, column_name, value):
        output_table = self.table_name + "_fillna"
        self.__drop(output_table, column_name=column_name, how=None, replace_with=value)

    def impute(self, column_name, how):
        output_table = self.table_name + "_impute"
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


class HBaseFrameBuilder(FrameBuilder):

    #-------------------------------------------------------------------------
    # Create BigDataFrames
    #-------------------------------------------------------------------------
    def build_from_csv(self, file, schema=None, skip_header=False):
        #create some random table name
        #we currently don't bother the user to specify table names
        df_name = file.replace("/", "_")
        df_name = df_name.replace(".", "_")
        dataframe_prefix = ''.join(random.choice(string.lowercase) for i in xrange(DATAFRAME_NAME_PREFIX_LENGTH))
        df_name = dataframe_prefix + df_name
        hbase_table = HBaseTable(df_name) #currently we support hbase, TODO: where to read table type?
        new_frame = BigDataFrame(hbase_table)

        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(df_name)
        feature_names_as_str = etl_schema.get_feature_names_as_CSV()
        feature_types_as_str = etl_schema.get_feature_types_as_CSV()
        
        script_path = os.path.join(etl_scripts_path,'pig_import_csv.py')

        args = _get_pig_args()

        args += [script_path, '-i', file, '-o', df_name,
             '-f', feature_names_as_str, '-t', feature_types_as_str]

        if skip_header:
            args += ['-k']

        logger.debug(args)
        # need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(df_name,
                                           [config['hbase_column_family']])
        print ' '.join(map(str, args))
        return_code = subprocess.call(args)
        if return_code:
            raise Exception('Could not import CSV file')

        return new_frame

    def build_from_json(self, file):
        #create some random table name
        #we currently don't bother the user to specify table names
        df_name = file.replace("/", "_")
        df_name = df_name.replace(".", "_")
        dataframe_prefix = ''.join(random.choice(string.lowercase) for i in xrange(DATAFRAME_NAME_PREFIX_LENGTH))
        df_name = dataframe_prefix + df_name
        hbase_table = HBaseTable(df_name) #currently we support hbase, TODO: where to read table type?
        new_frame = BigDataFrame(hbase_table)

        schema='json:chararray'#dump all records as chararray
        
        #save the schema of the dataset to import
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema)
        etl_schema.save_schema(df_name)

        script_path = os.path.join(etl_scripts_path,'pig_import_json.py')

        args = _get_pig_args()

        args += [script_path, '-i', file, '-o', df_name]

        logger.debug(args)
        
#         need to delete/create output table to write the transformed features
        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(df_name,
                                           [config['hbase_column_family']])
        return_code = subprocess.call(args)
        
        if return_code:
            raise Exception('Could not import CSV file')

        return new_frame
            
        raise Exception("Not implemented")
    def build_from_xml(self, file, schema=None):
        raise Exception("Not implemented")

