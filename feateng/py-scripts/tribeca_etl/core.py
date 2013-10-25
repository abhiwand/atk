"""
This is just some experimental code written in sprint_3 for exploring a pandas-like ETL interface
Can use the documentation conventions of numpy https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
"""

import subprocess
import os
import re
import string
import random

from config import CONFIG_PARAMS
from hbase_client import ETLHBaseClient

__all__ = [
    'EvalFunctions',
    'ETLOperationFailedException',
    'BigDataFrame'
]

"""
Available functions that can be applied with the apply method on big data frames
"""
class EvalFunctions:
    """String functions
    """
    class String:
        LENGTH=1
        LOWER=2
        LTRIM=2
        
    """Math functions
    """        
    class Math:
        ABS=1000
        LOG=1001
        LOG10=1002
        POW=1003
        EXP=1004

    @staticmethod
    def to_string(x):
        return {
            EvalFunctions.String.LENGTH: 'org.apache.pig.piggybank.evaluation.string.LENGTH',
            EvalFunctions.String.LOWER: 'LOWER',
            EvalFunctions.String.LTRIM: 'LTRIM',
            EvalFunctions.Math.ABS: 'ABS',
            EvalFunctions.Math.LOG: 'LOG',
            EvalFunctions.Math.LOG10: 'LOG10',
            EvalFunctions.Math.POW: 'org.apache.pig.piggybank.evaluation.math.POW',
            EvalFunctions.Math.EXP: 'EXP'            
        }[x]

    
class ETLOperationFailedException(Exception):
    pass

class BigDataFrame:
    def __init__(self, ):
        self.base_script_path = os.path.dirname(os.path.abspath(__file__))
        self.DATAFRAME_NAME_PREFIX_LENGTH=5
        self.feature_names=[]
#         self.feature_types=[]
        
        #keys for etl operations used in table names
        self.TRANSFORM_OPS_KEY='t'
        self.FILTER_OPS_KEY='f'
        self.ETL_OPS_COUNT={}
        self.ETL_OPS_COUNT[self.TRANSFORM_OPS_KEY]=0
        self.ETL_OPS_COUNT[self.FILTER_OPS_KEY]=0
        
        self.__last_etl_operation=None
        
        #current directory of this script, needed for locating & running the pig scripts
        self.__dataframe_prefix = ''.join(random.choice(string.lowercase) for i in xrange(self.DATAFRAME_NAME_PREFIX_LENGTH))
        self.__dataframe_name=None#set during import
        self.__pig_statements = []
        self.__pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))
        self.__pig_statements.append("REGISTER %s; -- Tribeca ETL helper UDFs" % (CONFIG_PARAMS['tribeca-etl-jar']))
    
    """
    Can apply ETL operations to individual features: big_frame['duration'].dropna()
    """
    def __getitem__(self, source_feature_name):
        self.source_feature = source_feature_name
        return self

    """
    Can apply ETL operations to individual features with assignments: big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG)
    """    
    def __setitem__(self, new_feature_name, value):
        #When the user calls: big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG)
        #we first create the transformation operation arguments except for the name of the new_feature, which is specified on the lhs
        #when we get the lhs in this method, we just execute the transformation operation
        #currently only transformation operations are executed this way
#         args = value.__pending_ETL_operations.pop()
        self.__pig_statements.append(self.__generate_transform_statement(new_feature_name))
        pass
    
    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        buf+="\n"
        buf+="\n"
        pig_script = "\n".join(self.__pig_statements)
        buf+=pig_script
        return buf
    
    def __parse_schema(self, schema_definition):
        schema_definition = schema_definition[1:]#get rid of '('
        schema_definition = schema_definition[:len(schema_definition)-1]#get rid of ')'
        splitted = schema_definition.split(',')
        for s in splitted:
            feature_name_type = s.split(':')
            self.feature_names.append(feature_name_type[0])
#             self.feature_types.append(feature_name_type[1])
        
    """
    Import a CSV file with the given schema_definition.
    """  
    def import_csv(self, csv_file, schema_definition, skip_header=True):
        df_name = csv_file.replace("/", "_")
        df_name = df_name.replace(".", "_")
        df_name = self.__dataframe_prefix + df_name
        self.__dataframe_name = df_name       
        self.table_name = self.__dataframe_name   
        self.origin_table_name = self.__dataframe_name #this holds the original table that we imported the data, will be used for understanding which features are derived or not             
        self.__parse_schema(schema_definition)
        if skip_header:
            self.__pig_statements.append("%s = LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS %s;" % (self.__dataframe_name, csv_file, schema_definition))
        else:
            self.__pig_statements.append("%s = LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS %s;" % (self.__dataframe_name, csv_file, schema_definition))
    
    """
    Drop the missing values for a particular feature. If replace_with is specified, the missing values are replaced with that value.
    """      
    def dropna(self, replace_with=None):
        #TODO not implemented yet
        table_name = self.table_name + "_dropped_na"
        self.table_name = table_name # only update the table_name field
        return self
    
    """
    Replace the missing values of a feature with the given replacement_value
    """
    def fillna(self, replacement_value):
        #TODO not implemented yet
        return self.dropna(replacement_value)
    
    """
    Apply a transformation to a particular feature.
    Parameters
    ----------
    transformation : enum (available functions in the EvalFunctions class)
        `transformation` specifies which transformation to apply to this dataframe
    keep_base_feature : boolean
        `keep_base_feature` specifies whether the transformed feature should be kept in the resulting dataframe
    transformation_args : variable number of arguments passed to the `transformation` function
    """
    def apply(self, transformation, keep_base_feature=True, *transformation_args):
         
        transformation_to_apply = EvalFunctions.to_string(transformation)
        print "Will apply",transformation_to_apply
        
        self.ETL_OPS_COUNT[self.TRANSFORM_OPS_KEY] += 1
        table_name = self.table_name + "_%s_%s_%d" % (self.source_feature, self.TRANSFORM_OPS_KEY, self.ETL_OPS_COUNT[self.TRANSFORM_OPS_KEY])

        #wrap arguments in a tuple        
        source_relation = self.__dataframe_name
        dest_relation = table_name
        source_feature = self.source_feature
        self.__last_etl_operation = (source_relation, source_feature, dest_relation, transformation_to_apply, keep_base_feature, transformation_args)
        self.__dataframe_name = table_name#update current relation name used in pig
        return self
    
    
    """Generates a Pig transformation statement and updates the list of features in this BigDataFrame
    """
    def __generate_transform_statement(self, new_feature_name):
        #unwrap transformation-related arguments
        
        source_relation = self.__last_etl_operation[0]
        source_feature = self.__last_etl_operation[1]
        dest_relation = self.__last_etl_operation[2]
        transformation_to_apply = self.__last_etl_operation[3]
        keep_base_feature = self.__last_etl_operation[4]
        transformation_args = self.__last_etl_operation[5]
        
        transform_statement = ''
        for i, f in enumerate(self.feature_names):
            if source_feature == f:
                if keep_base_feature:#should also write the original feature to output
                    transform_statement += f 
                    transform_statement += ', ' 
            else:
                transform_statement += f
                transform_statement += ', ' 
    
        if len(transformation_args)>0:#we have some args to pass to the transformation_function
            #TODO: currently we have only 1 argument in transformation_args, need to handle more ...
            transform_statement += "%s(%s,%s) as %s" % (transformation_to_apply, source_feature, str(transformation_args[0]), new_feature_name)
        else:#PIG builtin functions without args
            transform_statement += "%s(%s) as %s" % (transformation_to_apply, source_feature, new_feature_name)
        
        self.feature_names.append(new_feature_name)#add the new feature
        
        return '%s = FOREACH %s GENERATE %s;' % (dest_relation, source_relation, transform_statement)

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