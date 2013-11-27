import unittest
import os
import random
import sys
import string
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))

from intel_analytics.table.bigdataframe import BigDataFrame
from intel_analytics.table.bigdataframe import BigDataFrameException
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.hbase.table import HBaseFrameBuilder
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.hbase.table import Imputation
from intel_analytics.config import global_config as CONFIG_PARAMS


class BigDataFrameTest(unittest.TestCase):
    
    def validate_nonnull(self, table, col_to_check):
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               self.assertNotEqual(data[CONFIG_PARAMS['hbase_column_family'] + col_to_check], '')
               
    def validate_all_nonnull(self, table):
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               for k in data.keys():
                   self.assertNotEqual(data[k], '')
                   
    def validate_no_allnull(self, table):
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
           table = hbase_client.connection.table(table)
           for key, data in table.scan():
               all_null = True
               for k in data.keys():
                   if data[k] != '':
                       all_null = False
                       break
               self.assertEqual(all_null, False)       
               
    def validate_json_extract(self, table):
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
            table = hbase_client.connection.table(table)
            for key, data in table.scan():#we only have a single row, validate all columns
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_book_author'], 'Nigel Rees', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_book_empty_field'], '', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_books_price'], '8.95', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_books_integer_field'], '2', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_books_boolean_field'], 'true', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_price_data_greater_than_10'], '12.99', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'category'], 'fiction', '')
                   
    def test(self):
        print '###########################'
        print 'Validating BigDataFrame API'
        print '###########################'
        
        temp_tables = []
        
        #first validate json import
        test_json='{ "store": {"book": [{ "category": "reference", "empty_field":"", "boolean_field": true, "null_field": null, "integer_field": 2,"author": "Nigel Rees","title": "Sayings of the Century", "price": 8.95},{ "category": "fiction","author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99,"isbn": "0-553-21311-3"}],"bicycle": {"color": "red","price": 19.95}}}'
        fp = open('/tmp/test.json', 'w')
        fp.write(test_json)
        fp.close()        
        
        big_frame = HBaseFrameBuilder().build_from_json('/tmp/test.json')
        big_frame.head()
        
        big_frame.transform('json', 'first_book_author', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[0].author"])
        big_frame.head()
        
        big_frame.transform('json', 'first_book_empty_field', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[0].empty_field"])
        
        big_frame.transform('json', 'first_books_price', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[0].price"])
        big_frame.head()
        
        big_frame.transform('json', 'first_books_integer_field', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[0].integer_field"])
        big_frame.head()
        
        big_frame.transform('json', 'first_books_boolean_field', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[0].boolean_field"])
        big_frame.head()
        
        big_frame.transform('json', 'first_price_data_greater_than_10', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book.findAll{book -> book.price>10}[0].price"])
        big_frame.head()
        
        big_frame.transform('json', 'category', EvalFunctions.Json.EXTRACT_FIELD, transformation_args=["store.book[1].category"])
        
        self.validate_json_extract(big_frame._table.table_name)
        
        data_set = ['name,age,salary',
        'john,23,100',
        'molly,,',
        'david,,100',
        'test,12,',
        ',,']
        
        fp = open('/tmp/clean_test.csv', 'w')
        for data in data_set:
            fp.write(data)
            fp.write('\n')
        fp.close()

        schema = 'name:chararray,age:int,salary:int'
        
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(column_name='age')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
         
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(column_name='salary')
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)
         
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='any')
        self.validate_all_nonnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)
         
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='all')
        self.validate_no_allnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)
         
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.fillna('age', '9999')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
         
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.impute('salary', Imputation.MEAN)
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)
        
        #failure cases
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        self.assertRaises(BigDataFrameException,big_frame.dropna, column_name='col_doesnt_exist')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        big_frame.dropna(how='any', column_name='age')#should ignore the how parameter and clean age
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)
        
        big_frame = HBaseFrameBuilder().build_from_csv('/tmp/clean_test.csv', schema, True)
        self.assertRaises(BigDataFrameException, big_frame.fillna, 'col_doesnt_exist', '{}\"sss')
        temp_tables.extend(big_frame.lineage)  

        print 'Cleaning up the temp tables %s & their schema definitions' % (big_frame.lineage)
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
            for temp in temp_tables:
                try:
                    hbase_client.connection.delete_table(temp, disable=True)
                    print 'deleted table',temp
                except:
                    pass
                try:
                    table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
                    table.delete(temp)#also remove the schema info
                except:
                    pass
        print '###########################'
        print 'DONE Validating BigDataFrame API'
        print '###########################'
                        

if __name__ == '__main__':
    unittest.main()        