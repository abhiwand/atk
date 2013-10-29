import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS

worldbank_data_csv_path = os.path.join(base_script_path, '..', '..', 'test-data/worldbank.csv')
py_scripts_path = os.path.join(base_script_path, '..')

cols = ['etl-cf:country', 'etl-cf:year', 'etl-cf:co2_emission', 'etl-cf:co2_emission', 
        'etl-cf:electric_consumption','etl-cf:energy_use','etl-cf:fertility','etl-cf:gni',
        'etl-cf:internet_users','etl-cf:life_expectancy','etl-cf:military_expenses','etl-cf:population','etl-cf:hiv_prevelence']

TEST_TABLE='worldbank_csv'
TEST_TRANSFORM_TABLE='worldbank_csv_transformed' 

# commands.getoutput("hadoop fs -rmr /tmp/worldbank.csv")
# commands.getoutput("hadoop fs -put %s /tmp/worldbank.csv" % (worldbank_data_csv_path))
#  
# schema_definition = 'country:chararray,year:chararray,'+\
#                     'co2_emission:double,electric_consumption:double,'+\
#                     'energy_use:double,fertility:double,gni:double,'+\
#                     'internet_users:double,life_expectancy:double,military_expenses:double,'+\
#                     'population: double,hiv_prevelence:double'
#                      
# subprocess.call(['python', os.path.join(py_scripts_path, 'import_csv.py'), '-i', '/tmp/worldbank.csv',
#                  '-o', TEST_TABLE, '-s', schema_definition, '-k'])


print "------------------------------------TESTING TRANSFORM SCRIPTS------------------------------------"
 
DIFF_EPSILON = 0.01#diff used for floating point comparisons
#  
# subprocess.call(['python', os.path.join(py_scripts_path, 'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
#                  '-o', TEST_TRANSFORM_TABLE, '-t', 'EXP', '-n', 'exp_internet_users', '-k'])


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
#     t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
#     for key, data in t.scan():
#         if data['etl-cf:internet_users'] == '':
#                  continue
#         try:
#             exp_value = math.exp(float(data['etl-cf:internet_users']))
#             assert (float(data['etl-cf:internet_users']) - exp_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:exp_internet_users']), exp_value)
#         except OverflowError:
#             assert data['etl-cf:exp_internet_users'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:exp_internet_users'])
# 
# #cleanup transform tables
# with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
#     hbase_client.drop_create_table(TEST_TRANSFORM_TABLE, [CONFIG_PARAMS['etl-column-family']])
#                                     
# subprocess.call(['python', os.path.join(py_scripts_path, 'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
#                  '-o', TEST_TRANSFORM_TABLE, '-t', 'ABS', '-n', 'abs_internet_users', '-k'])
#  
# with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
#     t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
#     for key, data in t.scan():
#         if data['etl-cf:internet_users'] == '':
#                  continue
#         try:
#             abs_value = math.fabs(float(data['etl-cf:internet_users']))
#             assert (float(data['etl-cf:abs_internet_users']) - abs_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:abs_internet_users']), abs_value)
#         except OverflowError:
#             assert data['etl-cf:abs_internet_users'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:abs_internet_users'])

#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    hbase_client.drop_create_table(TEST_TRANSFORM_TABLE, [CONFIG_PARAMS['etl-column-family']])
      
subprocess.call(['python', os.path.join(py_scripts_path, 'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'LOG10', '-n', 'log10_internet_users', '-k'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:internet_users'] == '':
                 continue
        try:
            if float(data['etl-cf:internet_users']) == 0.0:
                assert data['etl-cf:log10_internet_users'] == '-Infinity', "%s should have been -Infinity" % (data['etl-cf:log10_internet_users'])
            else:
                log10_value = math.log10(float(data['etl-cf:internet_users']))
                assert (float(data['etl-cf:log10_internet_users']) - log10_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:log10_internet_users']), log10_value)
        except OverflowError:
            assert data['etl-cf:log10_internet_users'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:log10_internet_users'])
              


#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    hbase_client.drop_create_table(TEST_TRANSFORM_TABLE, [CONFIG_PARAMS['etl-column-family']])
    
subprocess.call(['python', os.path.join(py_scripts_path, 'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'org.apache.pig.piggybank.evaluation.math.POW', '-a', '[2]', '-n', 'internet_users_squared', '-k'])
 
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:internet_users'] == '':
                 continue
        pow_value = math.pow(float(data['etl-cf:internet_users']), 2)
        assert (float(data['etl-cf:internet_users_squared']) - pow_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:internet_users_squared']), pow_value)
            
print "------------------------------------DONE TESTING TRANSFORM SCRIPTS------------------------------------"

#cleanup test tables
with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
    hbase_client.drop_create_table(TEST_TABLE , [CONFIG_PARAMS['etl-column-family']])
    hbase_client.drop_create_table(TEST_TRANSFORM_TABLE, [CONFIG_PARAMS['etl-column-family']])
    