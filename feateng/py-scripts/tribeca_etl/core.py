import subprocess
import os
import re

from config import CONFIG_PARAMS
from hbase_client import ETLHBaseClient

__all__ = [
    'Transformations',
    'ETLOperationFailedException',
    'BigDataFrame'
]

dry_run = False

"""
Available feature transformations
"""
class Transformations:
    ABS=1
    LOG=2
    LOG10=3
    POW=4
    EXP=5

    @staticmethod
    def transformation_to_string(x):
        return {
            Transformations.ABS: 'ABS',
            Transformations.LOG: 'LOG',
            Transformations.LOG10: 'LOG10',
            Transformations.POW: 'POW',
            Transformations.EXP: 'EXP'
        }[x]
    
class ETLOperationFailedException(Exception):
    pass

class BigDataFrame:
    def __init__(self, ):
        self.degree_of_parallelism = 32#default number of reducers
        self.__pending_ETL_operations = []
        #current directory of this script, needed for locating & running the pig scripts
        self.base_script_path = os.path.dirname(os.path.abspath(__file__))
    
    """
    Can apply ETL operations to individual features: big_frame['duration'].dropna()
    """
    def __getitem__(self, filter_feature_name):
        self.filter_feature = filter_feature_name
        return self

    """
    Can apply ETL operations to individual features with assignments: big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG)
    """    
    def __setitem__(self, new_feature_name, value):
        #When the user calls: big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG)
        #we first create the transformation operation arguments except for the name of the new_feature, which is specified on the lhs
        #when we get the lhs in this method, we just execute the transformation operation
        #currently only transformation operations are executed this way
        args = value.__pending_ETL_operations.pop()
        
        args += ['-n', str(new_feature_name)]
  
        if dry_run:
            print args
            return_code = 0             
        else:
            return_code = subprocess.call(args)

        if return_code:
            raise ETLOperationFailedException('Could not apply transformation')  
    
    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        return buf
    
    """
    Import unstructured data from a given raw_data_file using a custom parser (parser_script). schema_definition
    specifies the fields in the dataset, which can then be referenced in the ETL operations
    """
    def import_data(self, raw_data_file, parser_script, df_name, schema_definition, input_delimeter_char='\\n'):
        self.raw_data_file = raw_data_file
        self.parser_script = parser_script
        self.table_name = df_name   
        self.origin_table_name = df_name #this holds the original table that we imported the data, will be used for understanding which features are derived or not             
        self.input_delimeter_char = input_delimeter_char
        
        args = ['pig', self.base_script_path + '/pig/pig_import.py', '-s', schema_definition,
                         '-l' , self.input_delimeter_char, '-i', self.raw_data_file, '-o', self.table_name,  
                         '-p', str(self.degree_of_parallelism), '-c', self.parser_script]
        
        if dry_run:
            print args
            return_code = 0             
        else:
            # need to delete/create output table so that we can write the transformed features
            with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                hbase_client.drop_create_table(self.table_name , [CONFIG_PARAMS['etl-column-family']])   
            return_code = subprocess.call(args)
                        
        if return_code:
            raise ETLOperationFailedException('Could not import dataset')
    
    """
    Import a CSV file with the given schema_definition. Note that the schema_definition should NOT be present in the data file itself as the first line 
    as Pig 0.11 doesn't handle CSV header. This will be fixed in Pig 0.12 with the new CSVExcelStorage .
    """  
    def import_csv(self, csv_file, df_name, schema_definition):
        self.table_name = df_name   
        self.origin_table_name = df_name #this holds the original table that we imported the data, will be used for understanding which features are derived or not             
        
        if not os.path.isfile(csv_file):     
            raise ETLOperationFailedException('%s does not exist' % (csv_file))
           
        args = ['pig','tribeca_etl/pig/pig_import_csv.py', '-i', csv_file, '-o', df_name, 
                '-p', str(self.degree_of_parallelism), '-s', schema_definition]
        if dry_run:
            print args
            return_code = 0             
        else:
            # need to delete/create output table so that we can write the transformed features
            with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                hbase_client.drop_create_table(df_name , [CONFIG_PARAMS['etl-column-family']])   
            return_code = subprocess.call(args)
            if return_code:
                raise ETLOperationFailedException('Could not import CSV file')            
    
    """
    Drop the missing values for a particular feature. If replace_with is specified, the missing values are replaced with that value.
    """      
    def dropna(self, replace_with=None):
        table_name = self.table_name + "_dropped_na"
        stripped_column_names = self.features()
        schema_information = ''
        for i, c in enumerate(stripped_column_names):
            schema_information += c
            if i != len(stripped_column_names) - 1:
                schema_information += ', '      
    
        args = ['pig', self.base_script_path + '/pig/pig_clean.py', '-i', self.table_name, 
                     '-o', table_name, '-p', str(self.degree_of_parallelism), '-s', schema_information]
        
        if replace_with:
            args += [ '-r' , replace_with]
            
        if hasattr(self, 'filter_feature'):
            args += ['-f', self.filter_feature]         
        else:
            args += ['-a', 'True']#drop any for all features
         
        if dry_run:
            print args
            return_code = 0             
        else:
            # need to delete/create output table so that we can write the transformed features
            with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                hbase_client.drop_create_table(table_name , [CONFIG_PARAMS['etl-column-family']])#create output table
            return_code = subprocess.call(args)

        if return_code:
            raise ETLOperationFailedException('Could not drop NAs')            
        
        self.table_name = table_name # only update the table_name field
        return self
    
    """
    Replace the missing values of a feature with the given replacement_value
    """
    def fillna(self, replacement_value):
        return self.dropna(replacement_value)
    
    """
    Apply a transformation to a particular field. Currently only the POW transformation has transformation_args
    """
    def apply(self, transformation, transformation_args=None):
        transformation_to_apply = Transformations.transformation_to_string(transformation)
        
        table_name = self.table_name + "_transformed_%s_%s" % (self.filter_feature, transformation_to_apply)
        
        if not dry_run:
            with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                hbase_client.drop_create_table(table_name , [CONFIG_PARAMS['etl-column-family']])#create output table
                    
        args = ['pig', self.base_script_path + '/pig/pig_transform.py', '-f', self.filter_feature, '-i', self.table_name,
                         '-o', table_name, '-p', str(self.degree_of_parallelism), '-t', transformation_to_apply]
        #we need the '-n', new_feature_name to be set
        if transformation_args:  # we have some args that we need to pass to the transformation function
            args += ['-a', str(transformation_args)]
         
        self.__pending_ETL_operations.append(args)
        self.table_name = table_name # only update the table_name field   
        return self
    
    """
    Returns the list of features in this big frame.
    """
    def features(self):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
#             print "will read ", self.table_name
            columns = hbase_client.get_column_names(self.table_name, [CONFIG_PARAMS['etl-column-family']])
            stripped_column_names = []
            for c in columns:
                stripped_column_names.append(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))#remove the col. family identifier
            #print "Available columns under the '%s' column family: %s" % (CONFIG_PARAMS['etl-column-family'], stripped_column_names)
            stripped_column_names.sort()
            return stripped_column_names#return sorted list of available features

    """
    Returns the list of the features that are derived from the existing features by applying transformations.
    For example, if the initial features are [x,y] and then a LOG transform is applied to create a new feature log_X (the new set of features becomes [x,y,log_X])
    calling this method returns the only derived feature [log_X]
    """
    def derived_features(self):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            columns = hbase_client.get_column_names(self.origin_table_name, [CONFIG_PARAMS['etl-column-family']])
            original_columns = set()
            for c in columns:
                original_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))
                
            columns = hbase_client.get_column_names(self.table_name, [CONFIG_PARAMS['etl-column-family']])
            current_columns = set()
            for c in columns:
                current_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))                
                
            diff_columns = list(current_columns - original_columns)
            diff_columns.sort()
            return diff_columns#return sorted list of diff features