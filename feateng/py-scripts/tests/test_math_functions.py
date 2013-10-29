import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS

TEST_TABLE='test_math'
SHOULD_IMPORT=True
TEMP_TABLES=['test_math', 'abs_table']

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    for temp in TEMP_TABLES:
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
        
        
############################################################
# FUNCTIONS TO VALIDATE THE OUTPUT OF THE MATH FUNCTIONS
############################################################
DIFF_EPSILON = 0.01#diff used for floating point comparisons

def validate_abs():
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table('abs_table')
        for key, data in t.scan():
            try:
                abs_value = math.fabs(float(data['etl-cf:f3']))
                assert (float(data['etl-cf:abs_f3']) - abs_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:abs_f3']), abs_value)
            except OverflowError:
                assert data['etl-cf:abs_f3'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:abs_f3'])
#############################################################################
    
if SHOULD_IMPORT:
    #cleanup test tables
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        hbase_client.drop_create_table(TEST_TABLE, [CONFIG_PARAMS['etl-column-family']])
                 
    print "------------------------------------TESTING IMPORT SCRIPTS------------------------------------"
    commands.getoutput('hadoop fs -rmr /tmp/test.csv')
    commands.getoutput('hadoop fs -put test-data/test.csv /tmp/test.csv')
    print "Uploaded /tmp/test.csv to HDFS:/tmp/test.csv"
      
    subprocess.call(['python', 'py-scripts/import_csv.py', '-i', '/tmp/test.csv',
                     '-o', TEST_TABLE, '-s', 'f1:chararray,f2:chararray,f3:double,f4:long', '-k'])
          
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        data_dict = hbase_client.get(TEST_TABLE,'1')#get the first row
        print "got", data_dict
        assert data_dict[CONFIG_PARAMS['etl-column-family']+'f1'] == 'test'
  
            
args = ['python', 'py-scripts/transform.py', '-i', TEST_TABLE , '-o', 'abs_table', '-f', 'f3', '-n', 'abs_f3', '-t', 'ABS', '-k']
subprocess.call(args)
validate_abs()
print 'Validated ABS'

print 'Cleaning up all the temp tables & their schema definitions'
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    for temp in TEMP_TABLES:
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