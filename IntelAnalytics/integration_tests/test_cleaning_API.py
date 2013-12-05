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
##############################################################################import os
import sys
import subprocess
import commands
import math
import csv
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..//'))
from intel_analytics.table.bigdataframe import BigDataFrame
from intel_analytics.table.bigdataframe import BigDataFrameException
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.hbase.table import HBaseFrameBuilder
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.hbase.table import Imputation
from intel_analytics.config import global_config as CONFIG_PARAMS

worldbank_data_csv_path = os.path.join(base_script_path, '..//', '..', 'feateng', 'test-data/worldbank.csv')
cols = ['etl-cf:country', 'etl-cf:year', 'etl-cf:co2_emission', 'etl-cf:co2_emission',
        'etl-cf:electric_consumption','etl-cf:energy_use','etl-cf:fertility','etl-cf:gni',
        'etl-cf:internet_users','etl-cf:life_expectancy','etl-cf:military_expenses','etl-cf:population','etl-cf:hiv_prevelence']
############################################################
# FUNCTIONS TO VALIDATE THE API CALLS
############################################################
def validate_imported(big_frame):
    print 'validate_imported, table_name: %s' % (big_frame._table.table_name)
    csv_records = []
    with open(worldbank_data_csv_path, 'rb') as f:
        reader = csv.reader(f)
        header_read = False
        for row in reader:
            if not header_read:
                header_read = True
            else:
                csv_records.append(row)

    print 'Read %d records from %s' % (len(csv_records), worldbank_data_csv_path)

    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        i = 0
        for row_key in range(1,len(csv_records)+1):
            data = hbase_client.get(big_frame._table.table_name, str(row_key))
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
    print 'validate_country_fillna, table_name: %s' % (big_frame._table.table_name)
    missing_country_records = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(big_frame._table.table_name)
        for key, data in t.scan():
            assert data['etl-cf:country'] != ''
            if data['etl-cf:country'] == 'MISSING_COUNTRY':
                missing_country_records+=1
    assert missing_country_records == 6, 'missing_country_records=%d should be 6'%missing_country_records#should be 6 missing countries

def validate_country_dropna(big_frame):
    print 'validate_country_dropna, table_name: %s' % (big_frame._table.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(big_frame._table.table_name)
        for key, data in t.scan():
            assert data['etl-cf:country'] != ''
            nRecords+=1
    assert nRecords == 1356, 'nRecords=%d should be 1356'%nRecords

def validate_dropna_any(big_frame):
    print 'validate_dropna_any, table_name: %s' % (big_frame._table.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(big_frame._table.table_name)
        for key, data in t.scan():
            for col in cols:
                assert data[col] != ''
            nRecords+=1
    assert nRecords == 49, 'nRecords=%d should be 49'%nRecords

def validate_dropna_all(big_frame):
    print 'validate_dropna_all, table_name: %s' % (big_frame._table.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(big_frame._table.table_name)
        for key, data in t.scan():
            all_features_missing = True
            for col in cols:
                if data[col] != '':
                    all_features_missing = False
                    break
            assert not all_features_missing, 'Found a record with all features null %s' % (data)
            nRecords+=1
    assert nRecords == 1356, 'nRecords=%d should be 1356'%nRecords


def validate_internet_fillna_avg(big_frame):
    print 'validate_internet_fillna_avg, table_name: %s' % (big_frame._table.table_name)
    nRecords = 0
    with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
        t = hbase_client.connection.table(big_frame._table.table_name)
        for key, data in t.scan():
            assert data['etl-cf:internet_users'] != ''
            nRecords+=1
    assert nRecords == 1362, 'nRecords=%d should be 1362'%nRecords

##############################################################################
print '###########################'
print 'Validating Clean API'
print '###########################'

temp_tables=[]

commands.getoutput("cp %s /tmp/worldbank.csv" % (worldbank_data_csv_path))
schema_definition = 'country:chararray,year:chararray,'+\
                    'co2_emission:double,electric_consumption:double,'+\
                    'energy_use:double,fertility:double,gni:double,'+\
                    'internet_users:double,life_expectancy:double,military_expenses:double,'+\
                    'population: double,hiv_prevelence:double'
big_frame = HBaseFrameBuilder().build_from_csv('test_import_worldbank', '/tmp/worldbank.csv', schema_definition, True)
validate_imported(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_imported"


big_frame = HBaseFrameBuilder().build_from_csv('test_worldbank_fillna_country', '/tmp/worldbank.csv', schema_definition, True, overwrite=True)
big_frame.fillna('country', 'MISSING_COUNTRY')
validate_country_fillna(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_country_fillna"

big_frame = HBaseFrameBuilder().build_from_csv('test_worldbank_dropna_country', '/tmp/worldbank.csv', schema_definition, True, overwrite=True)
big_frame.dropna(column_name='country')
validate_country_dropna(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_country_dropna"

big_frame = HBaseFrameBuilder().build_from_csv('test_worldbank_dropna_any', '/tmp/worldbank.csv', schema_definition, True, overwrite=True)
big_frame.dropna(how='any')#drop if any of the features is missing
validate_dropna_any(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_dropna_any"

big_frame = HBaseFrameBuilder().build_from_csv('test_worldbank_dropna_all', '/tmp/worldbank.csv', schema_definition, True, overwrite=True)
big_frame.dropna(how='all')#drop if all features are missing
validate_dropna_all(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_dropna_all"

big_frame = HBaseFrameBuilder().build_from_csv('test_worldbank_impute', '/tmp/worldbank.csv', schema_definition, True, overwrite=True)
big_frame.impute('internet_users', Imputation.MEAN)
validate_internet_fillna_avg(big_frame)
temp_tables.extend(big_frame.lineage)
print "DONE validate_internet_fillna_avg"

print 'Cleaning up the temp tables %s & their schema definitions' % (temp_tables)
num_valid_table = 0
with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
    for temp in temp_tables:
        try:
            hbase_client.connection.delete_table(temp, disable=True)
            num_valid_table += 1
            print 'deleted table',temp
        except:
            pass
        try:
            table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
            table.delete(temp)#also remove the schema info
        except:
            pass

assert num_valid_table == 6
print "DONE validate_clean_temporary_table"


print '###########################'
print 'DONE Validating Clean API'
print '###########################'        