"""
This is just some experimental code written in sprint_3 for exploring a pandas-like ETL interface
Can use the documentation conventions of numpy https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
"""

import subprocess
import os
import re
import string
import random
import sys

from config import CONFIG_PARAMS
from hbase_client import ETLHBaseClient
from functions import EvalFunctions
from schema import ETLSchema

__all__ = [
    'ETLOperationFailedException',
    'BigDataFrame'
]


base_script_path = os.path.dirname(os.path.abspath(__file__))
pig_log4j_path = os.path.join(base_script_path,'..','..', 'conf','pig_log4j.properties')
print 'Using',pig_log4j_path

dry_run=False
local_run=True

class ETLFunctionContext:
    def __init__(self, args, etl_schema, feature_to_transform, keep_source_feature):
        self.args=args#command line args as a list
        self.etl_schema=etl_schema#the schema object, required after the transformation is applied to update the ETL_SCHEMA table
        self.feature_to_transform=feature_to_transform
        self.keep_source_feature=keep_source_feature
        
class ETLOperationFailedException(Exception):
    pass

class BigDataFrame:
    def __init__(self, ):
        self.DATAFRAME_NAME_PREFIX_LENGTH=5
        self.__pending_ETL_operations = []
        #current directory of this script, needed for locating & running the pig scripts
        self.feature_names=[]
        self.base_script_path = os.path.dirname(os.path.abspath(__file__))
        self.__dataframe_prefix = ''.join(random.choice(string.lowercase) for i in xrange(self.DATAFRAME_NAME_PREFIX_LENGTH))
        self.__dataframe_name=None#set during import
        self.lineage=[]
        
    """
    Can apply ETL operations to individual features: big_frame['feature_name'].dropna()
    """
    def __getitem__(self, source_feature_name):
        self.source_feature = source_feature_name
        return self

    """
    Can apply ETL operations to individual features with assignments: big_frame['new_feature_name'] = big_frame['feature_name'].apply(EvalFunctions.LOG)
    """    
    def __setitem__(self, new_feature_name, value):
        #When the user calls: big_frame['new_feature_name'] = big_frame['feature_name'].apply(EvalFunctions.LOG)
        #we first create the transformation operation arguments except for the name of the new_feature, which is specified on the lhs
        #when we get the lhs in this method, we just execute the transformation operation
        #currently only transformation operations are executed this way
        transformation_context = value.__pending_ETL_operations.pop()
        etl_schema = transformation_context.etl_schema
        
        transformation_context.args += ['-n', str(new_feature_name)]
  
        if dry_run:
            print args
            return_code = 0             
        else:
            return_code = subprocess.call(transformation_context.args)

        if return_code:
            raise ETLOperationFailedException('Could not apply transformation')  
        else:
            #need to update schema here as it's difficult to pass the updated schema info from jython to python
            if not transformation_context.keep_source_feature:
                etl_schema.feature_names.remove(transformation_context.feature_to_transform)
            etl_schema.feature_names.append(new_feature_name)
            #for now make the new feature bytearray, because all UDF's have different return types
            #and we cannot know their return types
            etl_schema.feature_types.append('bytearray')
            etl_schema.save_schema(self.table_name)
            print "updated schema of table",self.table_name
    
    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        return buf

    def __parse_schema(self, schema_definition):
        splitted = schema_definition.split(',')
        for s in splitted:
            feature_name_type = s.split(':')
            self.feature_names.append(feature_name_type[0])
    """
    Import a CSV file with the given schema_definition. 
    """  
    def import_csv(self, csv_file, schema_definition, skip_header=False):
        df_name = csv_file.replace("/", "_")
        df_name = df_name.replace(".", "_")
        df_name = self.__dataframe_prefix + df_name
        self.__dataframe_name = df_name       
        self.table_name = self.__dataframe_name   
        self.origin_table_name = self.__dataframe_name #this holds the original table that we imported the data, will be used for understanding which features are derived or not             
        self.__parse_schema(schema_definition)    
        self.lineage.append(self.table_name)    
        
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema_definition)
        etl_schema.save_schema(df_name)
        
        feature_names_as_str = ",".join(etl_schema.feature_names)
        feature_types_as_str = ",".join(etl_schema.feature_types)
        
        script_path = os.path.join(base_script_path,'pig','pig_import_csv.py')
        
        args = ['pig']

        if local_run:
            args += ['-x', 'local']
        
        args += ['-4', pig_log4j_path, script_path, '-i', csv_file, '-o', df_name, 
                '-f', feature_names_as_str, '-t', feature_types_as_str]
                       
        if skip_header:  
            args += ['-k']  
        
        if dry_run:
            print args
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
    def __drop(self, replace_with=None, in_place=False):
        if in_place:
            table_name = self.table_name#output table is the same as input
        else:
            table_name = self.table_name + "_dropped_na"
            
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        
        feature_names_as_str = ",".join(etl_schema.feature_names)
        feature_types_as_str = ",".join(etl_schema.feature_types)   
        
        if hasattr(self, 'source_feature') and self.source_feature and (self.source_feature not in etl_schema.feature_names):
                raise Exception("Feature %s does not exist" % (self.source_feature))
       
        script_path = os.path.join(base_script_path,'pig','pig_clean.py')
        
        args = ['pig']
        
        if local_run:
            args += ['-x', 'local']
        
        args += [script_path, '-i', self.table_name, 
                         '-o', table_name, '-n', feature_names_as_str,
                         '-t', feature_types_as_str]
        
        if replace_with:
            args += [ '-r' , replace_with]
             
        if hasattr(self, 'source_feature'):
            args += ['-f', self.source_feature]      
            delattr(self, 'source_feature')#remove source feature attr.   
        else:
            args += ['-a', 'True']#drop any for all features
          
        if dry_run:
            print args
            return_code = 0             
        else:
            if not in_place:#if clean is performed NOT in place, then need to create output table, otherwise we are fine
                # need to delete/create output table so that we can write the transformed features
                with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                    hbase_client.drop_create_table(table_name , [CONFIG_PARAMS['etl-column-family']])#create output table
            
            print args
            return_code = subprocess.call(args)
 
        if return_code:
            raise ETLOperationFailedException('Could not drop NAs')            
         
        self.table_name = table_name # update the table_name
        etl_schema.save_schema(self.table_name) # save the schema for the new table
        return self

    """
    drop missing values
    """
    def dropna(self):
        return self.__drop(replace_with=None, in_place=False)
                
    """
    Replace the missing values of a feature with the given replacement_value
    """
    def fillna(self, replacement_value, in_place=False):
        return self.__drop(replace_with=replacement_value, in_place=in_place)
    
    """
    Apply a transformation to a particular field. transformation_args is a list of arguments to the function.
    """
    def apply(self, transformation, keep_source_feature=False, transformation_args=None):
        transformation_to_apply = EvalFunctions.to_string(transformation)
        
        #load schema info
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        feature_names_as_str = ",".join(etl_schema.feature_names)
        feature_types_as_str = ",".join(etl_schema.feature_types)
        
        #generate some table name for transformation output
        table_name = self.table_name + "-%s" % (transformation_to_apply)
        
        if not dry_run:
            with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
                hbase_client.drop_create_table(table_name , [CONFIG_PARAMS['etl-column-family']])#create output table
        
        script_path = os.path.join(base_script_path,'pig','pig_transform.py')
        
        args = ['pig', '-4', pig_log4j_path, script_path, 
                '-f', self.source_feature, '-i', self.table_name,
                '-o', table_name, '-t', transformation_to_apply,
                '-u', feature_names_as_str, '-r', feature_types_as_str]
        
        if transformation_args:  # we have some args that we need to pass to the transformation function
            args += ['-a', str(transformation_args)]
        
        if keep_source_feature:
            args += ['-k']   
            
        transformation_context = ETLFunctionContext(args, etl_schema, self.source_feature, keep_source_feature)
        self.__pending_ETL_operations.append(transformation_context)
        self.table_name = table_name # only update the table_name field   
        self.lineage.append(self.table_name) #keep track of history so that we can collect that garbage later
        return self
    
    """
    Print the first N rows of the dataframe
    """
    def head(self, nRows=10):
     with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        print "will read ", self.table_name
        table = hbase_client.connection.table(self.table_name)
        header_printed = False
        nrows_read = 0
        for key, data in table.scan():
            columns = data.keys()
            items = data.items()
            if not header_printed:
                sys.stdout.write("--------------------------------------------------------------------\n")
                for i, column in enumerate(columns):
                    sys.stdout.write("%s"%(re.sub(CONFIG_PARAMS['etl-column-family'],'',column)))
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
                    sys.stdout.write("\t")
            sys.stdout.write("\n")
            nrows_read+=1
            if nrows_read >= nRows:
                break
            
    """
    Returns the list of features in this big frame.
    """
    def features(self):
        features=[]
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        for i, feature_name in enumerate(etl_schema.feature_names):
            features.append('%s:%s' % (feature_name, etl_schema.feature_types[i]))
        return features

    """
    Returns the list of the features that are derived from the existing features by applying transformations.
    For example, if the initial features are [x,y] and then a LOG transform is applied to create a new feature log_X (the new set of features becomes [x,y,log_X])
    calling this method returns the only derived feature [log_X]
    """
#     def derived_features(self):
#         with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
#             columns = hbase_client.get_column_names(self.origin_table_name, [CONFIG_PARAMS['etl-column-family']])
#             original_columns = set()
#             for c in columns:
#                 original_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))
#                 
#             columns = hbase_client.get_column_names(self.table_name, [CONFIG_PARAMS['etl-column-family']])
#             current_columns = set()
#             for c in columns:
#                 current_columns.add(re.sub(CONFIG_PARAMS['etl-column-family'],'',c))                
#                 
#             diff_columns = list(current_columns - original_columns)
#             diff_columns.sort()
#             return diff_columns#return sorted list of diff features