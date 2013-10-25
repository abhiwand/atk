"""
Runs all the ETL scripts and validates the output of them.
This is actually a superset of validate_transform.py
"""

import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_script_path + '/..')
from tribeca_etl.hbase_client import ETLHBaseClient
from tribeca_etl.config import CONFIG_PARAMS

print "Using", CONFIG_PARAMS
print 'Starting ...'

print 'Building feature engineering jar'
commands.getoutput("mvn clean package -DskipTests")
assert True == os.path.exists('target/TRIB-FeatureEngineering-0.0.1-SNAPSHOT.jar')
print 'Built jar'

#cleanup test tables
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    hbase_client.drop_create_table('shaw_table' , [CONFIG_PARAMS['etl-column-family']])
    hbase_client.drop_create_table('shaw_table_cleaned' , [CONFIG_PARAMS['etl-column-family']])
    hbase_client.drop_create_table('shaw_table_transformed' , [CONFIG_PARAMS['etl-column-family']])
    hbase_client.drop_create_table('dummy_table' , [CONFIG_PARAMS['etl-column-family']])
            
print "------------------------------------TESTING IMPORT SCRIPTS------------------------------------"
commands.getoutput('hadoop fs -rmr /tmp/sample_shaw.log')
commands.getoutput('hadoop fs -put test-data/sample_shaw.log /tmp/sample_shaw.log')
print "Uploaded /tmp/sample_shaw.log to HDFS:/tmp/sample_shaw.log"
 
subprocess.call(['python', 'py-scripts/import.py', '-i', '/tmp/sample_shaw.log',
                 '-o', 'shaw_table', '-l' , '\\n', '-c', 'custom-parsers/shaw_udfs.py',  
                 '-s', 'timestamp:chararray,event_type:chararray,method:chararray,duration:double,item_id:chararray,src_tms:chararray,dst_tms:chararray'])
     
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    data_dict = hbase_client.get('shaw_table','1')#get the first row
    print "got", data_dict
    expected = "{'etl-cf:timestamp': '1340497060.737806', 'etl-cf:item_id': '', 'etl-cf:method': 'user', 'etl-cf:src_tms': '1340496000_EP015402660033_219_0_34313_1_2_8837', 'etl-cf:event_type': 'transportControl', 'etl-cf:dst_tms': '1340496000_EP015402660033_219_0_34313_1_2_8837', 'etl-cf:duration': '53'}"
    print "expected", expected
    assert str(data_dict) == expected


commands.getoutput('hadoop fs -rmr /tmp/test.csv')
commands.getoutput('hadoop fs -put test-data/test.csv /tmp/test.csv')
subprocess.call(['python', 'py-scripts/import_csv.py', '-i', '/tmp/test.csv',
                 '-o', 'dummy_table', '-s', 'f1:chararray,f2:chararray,f3:double,f4:long', '-k'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    data_dict = hbase_client.get('dummy_table','1')#get the first row
    print "got", data_dict
    expected = "{'etl-cf:f4': '3.0', 'etl-cf:f2': '1', 'etl-cf:f3': '2', 'etl-cf:f1': 'test'}"
    print "expected", expected
    assert str(data_dict) == expected

print "------------------------------------DONE TESTING IMPORT SCRIPTS------------------------------------"
print "------------------------------------TESTING CLEAN SCRIPTS------------------------------------"

subprocess.call(['python', 'py-scripts/clean.py', '-i', 'shaw_table', '-o', 'shaw_cleaned',
                  '-f', 'duration'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table('shaw_cleaned')
    for key, data in t.scan():
        assert data['etl-cf:duration'] != ''
         
subprocess.call(['python', 'py-scripts/clean.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_cleaned', '-r', '999'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    data = hbase_client.get('shaw_cleaned', '3')
    assert data['etl-cf:duration'] == '999'
    t = hbase_client.connection.table('shaw_cleaned')
    for key, data in t.scan():
        assert data['etl-cf:duration'] != ''
        
subprocess.call(['python', 'py-scripts/clean.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_cleaned', '-r', 'avg'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    data = hbase_client.get('shaw_cleaned', '3')
    assert data['etl-cf:duration'] == '650566.7044057377'# the average value of non-null durations
    t = hbase_client.connection.table('shaw_cleaned')
    for key, data in t.scan():
        assert data['etl-cf:duration'] != ''
                         
print "------------------------------------DONE TESTING CLEAN SCRIPTS------------------------------------"

print "------------------------------------TESTING TRANSFORM SCRIPTS------------------------------------"

DIFF_EPSILON = 0.01#diff used for floating point comparisons

subprocess.call(['python', 'py-scripts/transform.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_table_transformed', '-t', 'EXP', '-n', 'exp_duration', '-k'])
   

# NOTE: THE EXP VALIDATION MAY FAIL BECAUSE
#  I have seen differences between Java 1.6 and Java 1.7 floating point arithmetic results
#  for example:
#  for the input 189.0
#  when we apply the exp function
#  python 2.7 on windows returns: 1.2068605179340022e+82
#  java 1.7 on windows returns:  1.2068605179340022E82
#  java 1.6 on linux returns: 1.2068605179340024E82 [NOTICE THE DIFF TOWARDS THE END !]
#  related JVM bugs: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7021568 and http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7019078
# with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
#     t = hbase_client.connection.table('shaw_table_transformed')
#     for key, data in t.scan():
#         if data['etl-cf:duration'] == '':
#                  continue
#         try:
#             exp_value = math.exp(float(data['etl-cf:duration']))
#             assert (float(data['etl-cf:exp_duration']) - exp_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:exp_duration']), exp_value)
#         except OverflowError:
#             assert data['etl-cf:exp_duration'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:exp_duration'])
      
                                
subprocess.call(['python', 'py-scripts/transform.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_table_transformed', '-t', 'ABS', '-n', 'abs_duration', '-k'])

with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table('shaw_table_transformed')
    for key, data in t.scan():
        if data['etl-cf:duration'] == '':
                 continue
        try:
            abs_value = math.fabs(float(data['etl-cf:duration']))
            assert (float(data['etl-cf:abs_duration']) - abs_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:abs_duration']), abs_value)
        except OverflowError:
            assert data['etl-cf:abs_duration'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:abs_duration'])
 
subprocess.call(['python', 'py-scripts/transform.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_table_transformed', '-t', 'LOG10', '-n', 'log10_duration', '-k'])

with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table('shaw_table_transformed')
    for key, data in t.scan():
        if data['etl-cf:duration'] == '':
                 continue
        try:
            if float(data['etl-cf:duration']) == 0.0:
                assert data['etl-cf:log10_duration'] == '-Infinity', "%s should have been -Infinity" % (data['etl-cf:log10_duration'])
            else:
                log10_value = math.log10(float(data['etl-cf:duration']))
                assert (float(data['etl-cf:log10_duration']) - log10_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:log10_duration']), log10_value)
        except OverflowError:
            assert data['etl-cf:log10_duration'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:log10_duration'])
             
 
subprocess.call(['python', 'py-scripts/transform.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_table_transformed', '-t', 'POW', '-a', '2', '-n', 'duration_squared', '-k'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table('shaw_table_transformed')
    for key, data in t.scan():
        if data['etl-cf:duration'] == '':
                 continue
        pow_value = math.pow(float(data['etl-cf:duration']), 2)
        assert (float(data['etl-cf:duration_squared']) - pow_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:duration_squared']), pow_value)
            

commands.getoutput('hadoop fs -rmr /tmp/shaw_single.log')
commands.getoutput('hadoop fs -put test-data/shaw_single.log /tmp/shaw_single.log')
print "Uploaded /tmp/shaw_single.log to HDFS:/tmp/shaw_single.log"
 
subprocess.call(['python', 'py-scripts/import.py', '-i', '/tmp/shaw_single.log',
                 '-o', 'shaw_table', '-l' , '\\n', '-c', 'custom-parsers/shaw_udfs.py',  
                 '-s', 'timestamp,event_type,method,duration,item_id,src_tms,dst_tms'])
      
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    hbase_client.drop_create_table('shaw_table_transformed' , [CONFIG_PARAMS['etl-column-family']])
     
subprocess.call(['python', 'py-scripts/transform.py', '-i', 'shaw_table', '-f', 'duration',
                 '-o', 'shaw_table_transformed', '-t', 'STND', '-n', 'normalized_duration', '-k'])

with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table('shaw_table_transformed')
    for key, data in t.scan():
        value = float(data['etl-cf:normalized_duration'])
        if data['etl-cf:duration'] == '100':
            assert data['etl-cf:normalized_duration'] == '1.3934660285832354'
        if data['etl-cf:duration'] == '1':
            assert data['etl-cf:normalized_duration'] == '-0.905752918579103'
        if data['etl-cf:duration'] == '19':
            assert data['etl-cf:normalized_duration'] == '-0.4877131100041324'                        

print "------------------------------------DONE TESTING TRANSFORM SCRIPTS------------------------------------"
print 'Done validating the ETL scripts'