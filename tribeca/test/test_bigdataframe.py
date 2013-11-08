import unittest
import os
import random
import sys
import string
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
sys.path.append(os.path.join(base_script_path, '..', '..', 'feateng' ,'py-scripts'))

from tribeca.core import BigDataFrame
from tribeca.core import read_csv
from tribeca.core import Imputation
from tribeca.builtin_functions import EvalFunctions
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS
from tribeca.core import BigDataFrameException

class BigDataFrameTest(unittest.TestCase):
    
    def validate_nonnull(self, table, col_to_check):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               self.assertNotEqual(data[CONFIG_PARAMS['etl-column-family'] + col_to_check], '')
               
    def validate_all_nonnull(self, table):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               for k in data.keys():
                   self.assertNotEqual(data[k], '')
                   
    def validate_no_allnull(self, table):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               all_null = true
               for k in data.keys():
                   if data[k] != '':
                       all_null = false
                       break
               self.assertEqual(all_null, False)                          
                   
    def test(self):
        temp_tables = []
        
        data_set = ['name,age,salary',
        'john,23,100',
        'molly,,',
        'david,,100',
        'test,12,',
        ',,']
        
        fp = open('/tmp/clean_test.csv', 'w')
        for data in data_set:
            fp.write(data)
        fp.close()

        schema = 'name:chararray,age:int,salary:int'
        big_frame = read_csv('/tmp/clean_test.csv', schema, True) 
        big_frame.dropna(column_name='age')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(column_name='salary')
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='any')
        self.validate_all_nonnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='all')
        self.validate_no_allnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.fillna('age', '9999')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.impute('salary', Imputation.MEAN)
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)
        
        #failure cases
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        with self.assertRaises(BigDataFrameException):
            big_frame.dropna(column_name='col_doesnt_exist')#should throw BigDataFrameException
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='any', column_name='age')#should ignore the how parameter and clean age
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = read_csv('/tmp/clean_test.csv', schema, True)
        with self.assertRaises(BigDataFrameException):
            big_frame.fillna('col_doesnt_exist', '{}\"sss')#should throw BigDataFrameException
        temp_tables.extend(big_frame.lineage)  

        print 'Cleaning up the temp tables %s & their schema definitions' % (big_frame.lineage)
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            for temp in temp_tables:
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

if __name__ == '__main__':
    unittest.main()        