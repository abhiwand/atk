"""
This is just some experimental code written for performing tests from a dummy dataframe API
"""

import os
import sys
import subprocess
from time import sleep
import commands
from intel_analytics.etl.functions import EvalFunctions
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.core import *

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