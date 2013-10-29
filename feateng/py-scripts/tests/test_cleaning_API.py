"""
This is just some experimental code written for performing tests from a dummy dataframe API
"""

import os
import sys
import subprocess
from time import sleep
import commands
import csv
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_script_path + '/..')

from intel_analytics.etl.functions import EvalFunctions
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.core import *

print "Using", CONFIG_PARAMS
print 'Starting ...'

#first set up the environment variables
os.environ["PATH"] = '/home/user/pig-0.12.0/bin' + ':' + os.environ["PATH"]
os.environ["JYTHONPATH"]  = os.getcwd() + '/py-scripts/' # need for shipping the scripts that we depend on to worker nodes

print ">> JYTHONPATH",os.environ["JYTHONPATH"]

#to get rid of jython logging
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"

worldbak_data_csv_path = os.path.join(base_script_path, '..', '..', 'test-data/worldbank.csv')
cols = ['etl-cf:country', 'etl-cf:year', 'etl-cf:co2_emission', 'etl-cf:co2_emission', 
        'etl-cf:electric_consumption','etl-cf:energy_use','etl-cf:fertility','etl-cf:gni',
        'etl-cf:internet_users','etl-cf:life_expectancy','etl-cf:military_expenses','etl-cf:population','etl-cf:hiv_prevelence']
############################################################
# FUNCTIONS TO VALIDATE THE API CALLS
############################################################
def validate_imported(big_frame):
    print 'validate_imported, table_name: %s' % (big_frame.table_name)
    csv_records = []
    with open(worldbak_data_csv_path, 'rb') as f:
        reader = csv.reader(f)
        header_read = False
        for row in reader:
            if not header_read:
                header_read = True
            else:
                csv_records.append(row)
                
    print 'Read %d records from %s' % (len(csv_records), worldbak_data_csv_path)
       
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        i = 0
        for row_key in range(1,len(csv_records)+1):
            data = hbase_client.get(big_frame.table_name, str(row_key))
            csv_record = csv_records[i]
            
            assert data['etl-cf:country']==csv_record[0]
            assert data['etl-cf:year']==csv_record[1]
            
            if data['etl-cf:co2_emission'] == '':
                assert csv_record[2] == ''
            else:
                assert float(data['etl-cf:co2_emission'])==float(csv_record[2])
            
            if data['etl-cf:electric_consumption'] == '':
                assert csv_record[3] == ''
            else:    
                assert float(data['etl-cf:electric_consumption'])==float(csv_record[3])
                
            if data['etl-cf:energy_use'] == '':
                assert csv_record[4] == ''
            else:    
                assert float(data['etl-cf:energy_use'])==float(csv_record[4])
                                
            if data['etl-cf:fertility'] == '':
                assert csv_record[5] == ''
            else:    
                assert float(data['etl-cf:fertility'])==float(csv_record[5])
                            
            if data['etl-cf:gni'] == '':
                assert csv_record[6] == ''
            else:    
                assert float(data['etl-cf:gni'])==float(csv_record[6])
                            
            if data['etl-cf:internet_users'] == '':
                assert csv_record[7] == ''
            else:    
                assert float(data['etl-cf:internet_users'])==float(csv_record[7])
                
            if data['etl-cf:life_expectancy'] == '':
                assert csv_record[8] == ''
            else:    
                assert float(data['etl-cf:life_expectancy'])==float(csv_record[8])
                                            
            if data['etl-cf:military_expenses'] == '':
                assert csv_record[9] == ''
            else:    
                assert float(data['etl-cf:military_expenses'])==float(csv_record[9])
                            
            if data['etl-cf:population'] == '':
                assert csv_record[10] == ''
            else:    
                assert float(data['etl-cf:population'])==float(csv_record[10])
           
            if data['etl-cf:hiv_prevelence'] == '':
                assert csv_record[11] == ''
            else:    
                assert float(data['etl-cf:hiv_prevelence'])==float(csv_record[11])
            i+=1

def validate_country_fillna(big_frame):
    print 'validate_country_fillna, table_name: %s' % (big_frame.table_name)
    missing_country_records = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table(big_frame.table_name)
        for key, data in t.scan():
            assert data['etl-cf:country'] != ''
            if data['etl-cf:country'] == 'MISSING_COUNTRY':
                missing_country_records+=1
    assert missing_country_records == 6, 'missing_country_records=%d should be 6'%missing_country_records#should be 6 missing countries
                    
def validate_country_dropna(big_frame):
    print 'validate_country_dropna, table_name: %s' % (big_frame.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table(big_frame.table_name)
        for key, data in t.scan():
            assert data['etl-cf:country'] != ''    
            nRecords+=1
    assert nRecords == 1356, 'nRecords=%d should be 1356'%nRecords
    
def validate_dropna_any(big_frame):
    print 'validate_dropna_any, table_name: %s' % (big_frame.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table(big_frame.table_name)
        for key, data in t.scan():
            for col in cols:
                assert data[col] != ''    
            nRecords+=1
    assert nRecords == 49, 'nRecords=%d should be 49'%nRecords

def validate_internet_fillna_avg(big_frame):
    print 'validate_internet_fillna_avg, table_name: %s' % (big_frame.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        t = hbase_client.connection.table(big_frame.table_name)
        for key, data in t.scan():
            assert data['etl-cf:internet_users'] != ''
            nRecords+=1
    assert nRecords == 1362, 'nRecords=%d should be 1362'%nRecords

##############################################################################
big_frame = BigDataFrame() # create an empty data frame
commands.getoutput("hadoop fs -rmr /tmp/worldbank.csv")
commands.getoutput("hadoop fs -put %s /tmp/worldbank.csv" % (worldbak_data_csv_path))
schema_definition = 'country:chararray,year:chararray,'+\
                    'co2_emission:double,electric_consumption:double,'+\
                    'energy_use:double,fertility:double,gni:double,'+\
                    'internet_users:double,life_expectancy:double,military_expenses:double,'+\
                    'population: double,hiv_prevelence:double'
big_frame.import_csv('/tmp/worldbank.csv', schema_definition, True)
initial_table = big_frame.table_name
validate_imported(big_frame)
print "DONE validate_imported"

big_frame['country'].fillna('MISSING_COUNTRY')
validate_country_fillna(big_frame)
print "DONE validate_country_fillna"

big_frame.table_name=initial_table#point to the initial table
big_frame['country'].dropna()
validate_country_dropna(big_frame)
print "DONE validate_country_dropna"
 
big_frame.table_name=initial_table#point to the initial table
big_frame.dropna()#drop any
validate_dropna_any(big_frame)
print "DONE validate_dropna_any"

big_frame.table_name=initial_table#point to the initial table
big_frame['internet_users'].fillna('avg')#fill with average
validate_internet_fillna_avg(big_frame)
print "DONE validate_internet_fillna_avg"
 
big_frame.table_name=initial_table#point to the initial table
big_frame['internet_users'].fillna('avg', in_place=True)#fill with average
validate_internet_fillna_avg(big_frame)
print "DONE validate_internet_fillna_avg, in_place=True"
 
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