##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..//'))
os.environ['PYTHONPATH'] = ':'.join(sys.path)#python scripts that call our pig scripts need this
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.config import global_config as CONFIG_PARAMS

worldbank_data_csv_path = os.path.join(base_script_path, '..//', '..', 'feateng', 'test-data/worldbank.csv')
test_standardization_dataset_csv_path = os.path.join(base_script_path, '..//', '..', 'feateng', 'test-data/test_standardization_dataset.csv')
os.environ['INTEL_ANALYTICS_ETL_RUN_LOCAL'] = 'true'

py_scripts_path = os.path.join(base_script_path, '..//', 'intel_analytics', 'table' , 'hbase', 'scripts')

cols = ['etl-cf:country', 'etl-cf:year', 'etl-cf:co2_emission', 'etl-cf:co2_emission', 
        'etl-cf:electric_consumption','etl-cf:energy_use','etl-cf:fertility','etl-cf:gni',
        'etl-cf:internet_users','etl-cf:life_expectancy','etl-cf:military_expenses','etl-cf:population','etl-cf:hiv_prevelence']

TEST_TABLE='worldbank_csv'
TEST_TRANSFORM_TABLE='worldbank_csv_transformed' 
TEST_STND_TABLE='test_standardization_dataset_csv'
TEST_STND_TRANSFORM_TABLE='test_standardization_dataset_csv_transformed'

TEST_TABLES = []
TEST_TABLES.append(TEST_TABLE)
TEST_TABLES.append(TEST_TRANSFORM_TABLE)
TEST_TABLES.append(TEST_STND_TABLE)
TEST_TABLES.append(TEST_STND_TRANSFORM_TABLE)

print '###########################'
print 'Validating Transform Functions'
print '###########################'

print "Importing %s for testing transform scripts"%(worldbank_data_csv_path)                                                            
commands.getoutput("cp %s /tmp/worldbank.csv" % (worldbank_data_csv_path))# WE ARE IN LOCAL MODE, DON'T FORGET!
  
schema_definition = 'country:chararray,year:chararray,'+\
                    'co2_emission:double,electric_consumption:double,'+\
                    'energy_use:double,fertility:double,gni:double,'+\
                    'internet_users:double,life_expectancy:double,military_expenses:double,'+\
                    'population: double,hiv_prevelence:double'
                      
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'import_csv.py'), '-i', '/tmp/worldbank.csv',
                 '-o', TEST_TABLE, '-s', schema_definition, '-k'])

if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
    
DIFF_EPSILON = 0.01#diff used for floating point comparisons

print "Testing the EXP transform"
 
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'EXP', '-n', 'exp_internet_users', '-k'])
 
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
# NOTE: THE EXP VALIDATION MAY FAIL BECAUSE
#  I have seen differences between Java 1.6 and Java 1.7 floating point arithmetic results
#  for example:
#  for the input 189.0
#  when we apply the exp function
#  python 2.7 on windows returns: 1.2068605179340022e+82
#  java 1.7 on windows returns:  1.2068605179340022E82
#  java 1.6 on linux returns: 1.2068605179340024E82 [NOTICE THE DIFF TOWARDS THE END !]
#  related JVM bugs: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7021568 and http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7019078
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:internet_users'] == '':
                 continue
        try:
            exp_value = math.exp(float(data['etl-cf:internet_users']))
            assert (float(data['etl-cf:internet_users']) - exp_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:exp_internet_users']), exp_value)
        except OverflowError:
            assert data['etl-cf:exp_internet_users'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:exp_internet_users'])
print "Validated the EXP transform"
 
#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    hbase_client.connection.delete_table(TEST_TRANSFORM_TABLE, disable=True)
  
print "Testing the ABS transform"
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'ABS', '-n', 'abs_internet_users', '-k'])
    
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
  
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:internet_users'] == '':
                 continue
        try:
            abs_value = math.fabs(float(data['etl-cf:internet_users']))
            assert (float(data['etl-cf:abs_internet_users']) - abs_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:abs_internet_users']), abs_value)
        except OverflowError:
            assert data['etl-cf:abs_internet_users'] == 'Infinity', "%s should have been Infinity" % (data['etl-cf:abs_internet_users'])
print "Validated the ABS transform"
  
#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    hbase_client.connection.delete_table(TEST_TRANSFORM_TABLE, disable=True)
  
print "Testing the LOG10 transform"      
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'LOG10', '-n', 'log10_internet_users', '-k'])
  
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
  
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
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
                
print "Validated the LOG10 transform"
  
#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    hbase_client.connection.delete_table(TEST_TRANSFORM_TABLE, disable=True)
  
print "Testing the org.apache.pig.piggybank.evaluation.math.POW transform"          
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'transform.py'), '-i', TEST_TABLE, '-f', 'internet_users',
                 '-o', TEST_TRANSFORM_TABLE, '-t', 'org.apache.pig.piggybank.evaluation.math.POW', '-a', '[2]', '-n', 'internet_users_squared', '-k'])
  
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
   
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    t = hbase_client.connection.table(TEST_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:internet_users'] == '':
                 continue
        pow_value = math.pow(float(data['etl-cf:internet_users']), 2)
        assert (float(data['etl-cf:internet_users_squared']) - pow_value) < DIFF_EPSILON, "%f vs. %f" % (float(data['etl-cf:internet_users_squared']), pow_value)
              
print "Validated the org.apache.pig.piggybank.evaluation.math.POW transform"            
  
print "Importing %s for testing STND transform"%(test_standardization_dataset_csv_path)  
  
commands.getoutput("cp %s /tmp/test_standardization_dataset.csv" % (test_standardization_dataset_csv_path))
schema_definition = 'value:double'
                         
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'import_csv.py'), '-i', '/tmp/test_standardization_dataset.csv',
                 '-o', TEST_STND_TABLE, '-s', schema_definition, '-k'])
  
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
  
#cleanup transform tables
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    try:
        hbase_client.connection.delete_table(TEST_STND_TRANSFORM_TABLE, disable=True)
    except:
        pass
 
print "Testing the STND transform"  
return_code = subprocess.call(['python', os.path.join(py_scripts_path,
                                                      'transform.py'), '-i', TEST_STND_TABLE, '-f', 'value',
                 '-o', TEST_STND_TRANSFORM_TABLE, '-t', 'STND', '-n', 'stnd_value', '-k'])
 
if return_code:
    raise Exception("Transform script failed!")
    sys.exit(1)
 
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    t = hbase_client.connection.table(TEST_STND_TRANSFORM_TABLE)
    for key, data in t.scan():
        if data['etl-cf:value'] == '-5.0':
            assert data['etl-cf:stnd_value'] == '-1.7419230835145583', "data['etl-cf:stnd_value']=%s must have been -1.7419230835145583" % (data['etl-cf:stnd_value'])
        if data['etl-cf:value'] == '6.0':
            assert data['etl-cf:stnd_value'] == '0.5948030041269223', "data['etl-cf:stnd_value']=%s must have been 0.5948030041269223" % (data['etl-cf:stnd_value'])
        if data['etl-cf:value'] == '9.0':
            assert data['etl-cf:stnd_value'] == '1.2320919371200534', "data['etl-cf:stnd_value']=%s must have been 1.2320919371200534" % (data['etl-cf:stnd_value'])
        if data['etl-cf:value'] == '2.0':
            assert data['etl-cf:stnd_value'] == '-0.2549155731972525', "data['etl-cf:stnd_value']=%s must have been -0.2549155731972525" % (data['etl-cf:stnd_value'])
        if data['etl-cf:value'] == '4.0':
            assert data['etl-cf:stnd_value'] == '0.16994371546483492', "data['etl-cf:stnd_value']=%s must have been 0.16994371546483492" % (data['etl-cf:stnd_value'])
             
print "Validated the STND transform"  
                        
#cleanup test tables
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    for temp in TEST_TABLES:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
            table.delete(temp)#also remove the schema info
            print 'deleted schema information for table',temp
        except:
	    print "got ecxception"
	    print sys.exc_info()[0]		
            pass
                    
print '###########################'
print 'DONE Validating Transform Functions'
print '###########################'    
