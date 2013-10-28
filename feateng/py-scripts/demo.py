"""
This is just some experimental code written for performing tests from a dummy dataframe API
"""

import os
import sys
import subprocess
from time import sleep
import commands
from tribeca_etl.functions import EvalFunctions
from tribeca_etl.hbase_client import ETLHBaseClient
from tribeca_etl.config import CONFIG_PARAMS
from tribeca_etl.core import *

#to get rid of jython logging
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"

big_frame = BigDataFrame() # create an empty data frame
commands.getoutput("hadoop fs -rmr /tmp/worldbank.csv")
commands.getoutput("hadoop fs -put ../test-data/worldbank.csv /tmp/worldbank.csv")
schema_definition = 'country:chararray,year:chararray,'+\
                    'co2_emission:double,electric_consumption:double,'+\
                    'energy_use:double,fertility:double,gni:double,'+\
                    'internet_users:double,life_expectancy:double,military_expenses:double,'+\
                    'population: double,hiv_prevelence:double'
print schema_definition
big_frame.import_csv('/tmp/worldbank.csv', schema_definition, True)
big_frame.head(100)

big_frame.features()

big_frame['country'].fillna('MISSING_COUNTRY')
big_frame.head(100)
 
big_frame['country'].dropna()
big_frame.head(100)

big_frame.dropna()#drop any
big_frame.head(100)

big_frame['internet_users'].fillna('avg')#fill with average
big_frame.head(100)

big_frame['country'].dropna()
big_frame.head(100)

big_frame['internet_users'].fillna('avg', in_place=True)#fill with average
big_frame.head(100)

# commands.getoutput("hadoop fs -rmr /tmp/us_states.csv")
# commands.getoutput("hadoop fs -put test-data/us_states.csv /tmp/us_states.csv")
# schema_definition = 'state_name:chararray'
# big_frame.import_csv('/tmp/us_states.csv', schema_definition, True)
# big_frame.head(50)
# big_frame['state_name_endswith'] = big_frame['state_name'].apply(EvalFunctions.String.ENDS_WITH, keep_source_feature=True, transformation_args=['icut'])
# big_frame.head(50)
# big_frame['state_name_equals_ignore_case'] = big_frame['state_name'].apply(EvalFunctions.String.EQUALS_IGNORE_CASE, keep_source_feature=True, transformation_args=['connecticut'])
# big_frame.head(50)
# big_frame['state_name_index_of'] = big_frame['state_name'].apply(EvalFunctions.String.INDEX_OF, keep_source_feature=True, transformation_args=['Connecticut',0])
# big_frame.head(50)
# big_frame['state_name_last_index_of'] = big_frame['state_name'].apply(EvalFunctions.String.LAST_INDEX_OF, keep_source_feature=True, transformation_args=['onnec'])
# big_frame.head(50)
# big_frame['state_name_lower'] = big_frame['state_name'].apply(EvalFunctions.String.LOWER, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_ltrim'] = big_frame['state_name'].apply(EvalFunctions.String.LTRIM, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_regex'] = big_frame['state_name'].apply(EvalFunctions.String.REGEX_EXTRACT, keep_source_feature=True, transformation_args=["\\\S*onnec\\\S*",0])
# big_frame.head(50)
# big_frame['state_name_regex_all'] = big_frame['state_name'].apply(EvalFunctions.String.REGEX_EXTRACT_ALL, keep_source_feature=True, transformation_args=["(\\\S*onn\\\S*)"])
# big_frame.head(50)
# big_frame['state_name_replaced'] = big_frame['state_name'].apply(EvalFunctions.String.REPLACE, keep_source_feature=True, transformation_args=['onnec','xxx'])
# big_frame.head(50)
# big_frame['state_name_rtrim'] = big_frame['state_name'].apply(EvalFunctions.String.RTRIM, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_starts_with'] = big_frame['state_name'].apply(EvalFunctions.String.STARTS_WITH, keep_source_feature=True, transformation_args=['Connec'])
# big_frame.head(50)
# big_frame['state_name_splitted'] = big_frame['state_name'].apply(EvalFunctions.String.STRSPLIT, keep_source_feature=True, transformation_args=[" ",2])
# big_frame.head(50)
# big_frame['state_name_substr'] = big_frame['state_name'].apply(EvalFunctions.String.SUBSTRING, keep_source_feature=True, transformation_args=[0,3])
# big_frame.head(50)
# big_frame['state_name_trim'] = big_frame['state_name'].apply(EvalFunctions.String.TRIM, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_upper'] = big_frame['state_name'].apply(EvalFunctions.String.UPPER, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_tokenized'] = big_frame['state_name'].apply(EvalFunctions.String.TOKENIZE, keep_source_feature=True)
# big_frame.head(50)
# big_frame['state_name_len'] = big_frame['state_name'].apply(EvalFunctions.String.LENGTH, keep_source_feature=True)
# big_frame.head(50)

print 'Cleaning up the temp tables %s & their schema definitions' % (big_frame.lineage)
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    for temp in big_frame.lineage:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['etl-schema-table'])
            table.delete(temp)#also remove the schema info
        except:
            pass

commands.getoutput("hadoop fs -rmr /tmp/worldbank.csv")
commands.getoutput("hadoop fs -rmr /tmp/us_states.csv")
 
#import some data
# shaw_header = 'timestamp,event_type,method,duration,item_id,src_tms,dst_tms'
# big_frame.import_data('/tmp/sample_shaw.log', '../custom-parsers/shaw_udfs.py', 'shaw_table', shaw_header)
# #TODO need a read_csv file for v 0.5
# big_frame = big_frame['duration'].dropna() # drop the missing values for the duration field
# big_frame['duration'].fillna('999')# replace with a specific value
# big_frame['duration'].fillna('avg')# replace with a specific value
#big_frame['duration^2'] = big_frame['duration'].apply(Transformations.POW, 2)
#TODO setitem type assignment should only work for transforms for v 0.5
#big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG) # apply log transformation to the duration field and name the new feature as log_duration
# print big_frame.features() # list all features
# print big_frame.schema()#TODO
# print big_frame.derived_features() # list only the features derived by applying transformations to the base features



# #only f_1 is chararray
# big_frame['f_1_len'] = big_frame['f_1'].apply(EvalFunctions.String.LENGTH)
# big_frame['f_1_lower'] = big_frame['f_1'].apply(EvalFunctions.String.LOWER)
# big_frame['f_1_trimmed'] = big_frame['f_1'].apply(EvalFunctions.String.LTRIM)
# 
# #f_4 is a double
# big_frame['log_f4_duration'] = big_frame['f_4'].apply(EvalFunctions.Math.LOG)
# print big_frame

#TODO: dropna, fillna, projection
# big_frame = big_frame['duration'].dropna() # drop the missing values for the duration field
# big_frame['duration'].fillna('999')# replace with a specific value
# big_frame['duration'].fillna('avg')# replace with a specific value
# print big_frame.features() # list all features
# print big_frame.derived_features() # list only the features derived by applying transformations to the base features