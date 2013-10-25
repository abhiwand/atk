import os
import sys
import subprocess
from time import sleep


"""
This is just some experimental code written in sprint_3 for exploring a pandas-like ETL interface
"""
#register /home/user/nezih-experiments/pig-0.12.0/contrib/piggybank/java/piggybank.jar
#org.apache.pig.piggybank.storage.CSVExcelStorage
#A = load 'test-data/test.csv' using PigStorage(',') as (f_1:chararray,f_2:chararray,f_3:chararray,f_4:chararray);
#B = foreach A generate ENDSWITH(f_1,'t');
#B = foreach A generate org.apache.pig.piggybank.evaluation.string.LENGTH(f_1);

#first set up the environment variables
os.environ["PIG_HOME"] = '/home/user/nezih-experiments/pig-0.12.0'
os.environ["PATH"] = '/home/user/nezih-experiments/pig-0.12.0/bin' + ':' + os.environ["PATH"]
os.environ["JYTHONPATH"]  = '.' # need for shipping the scripts that we depend on to worker nodes
sys.path.append('tribeca_etl') # needed for python to find where tribeca_etl module is 

from tribeca_etl.core import *

big_frame = BigDataFrame() # create an empty data frame

#see test-data/test.csv, that csv is put under '/tmp/test.csv' @ HDFS
schema_definition = '(f_1:chararray,f_2:long,f_3:long,f_4:double)'
big_frame.import_csv('/tmp/test.csv', schema_definition)

#only f_1 is chararray
big_frame['f_1_len'] = big_frame['f_1'].apply(EvalFunctions.String.LENGTH)
big_frame['f_1_lower'] = big_frame['f_1'].apply(EvalFunctions.String.LOWER)
big_frame['f_1_trimmed'] = big_frame['f_1'].apply(EvalFunctions.String.LTRIM)

#f_4 is a double
big_frame['log_f4_duration'] = big_frame['f_4'].apply(EvalFunctions.Math.LOG)
print big_frame

#TODO: dropna, fillna, projection
# big_frame = big_frame['duration'].dropna() # drop the missing values for the duration field
# big_frame['duration'].fillna('999')# replace with a specific value
# big_frame['duration'].fillna('avg')# replace with a specific value
# print big_frame.features() # list all features
# print big_frame.derived_features() # list only the features derived by applying transformations to the base features