import os
import sys
import subprocess
from time import sleep

#first set up the environment variables
os.environ["PATH"] = '/home/user/nezih-experiments/pig-0.11.1/bin' + ':' + os.environ["PATH"]
os.environ["JYTHONPATH"]  = '.' # need for shipping the scripts that we depend on to worker nodes
sys.path.append('tribeca_etl') # needed for python to find where tribeca_etl module is 

from tribeca_etl.core import *

big_frame = BigDataFrame() # create an empty data frame

#csv_header = 'playerID,yearID,stint,teamID,lgID,G,G_batting,AB,R,H,two_B,three_B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP,G_old'
#big_frame.import_csv('/tmp/Batting.csv', 'test_df', csv_header)

#import some data
shaw_header = 'timestamp,event_type,method,duration,item_id,src_tms,dst_tms'
big_frame.import_data('/tmp/sample_shaw.log', '../custom-parsers/shaw_udfs.py', 'shaw_table', shaw_header)
# #TODO need a read_csv file for v 0.5
big_frame = big_frame['duration'].dropna() # drop the missing values for the duration field
#     #big_frame['duration'].fillna('999')# replace with a specific value
#     #big_frame['duration'].fillna('avg')# replace with a specific value
big_frame['duration^2'] = big_frame['duration'].apply(Transformations.POW, 2)
#     #TODO setitem type assignment should only work for transforms for v 0.5
#big_frame['log_duration'] = big_frame['duration'].apply(Transformations.LOG) # apply log transformation to the duration field and name the new feature as log_duration
print big_frame.features() # list all features
print big_frame.derived_features() # list only the features derived by applying transformations to the base features

#TODO: we can keep track of lineage of the ETL operations in the BigDataFrame class and can easily undo any operation.