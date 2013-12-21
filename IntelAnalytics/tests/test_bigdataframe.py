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
import unittest
import os
import sys
from intel_analytics.table.hbase.schema import ETLSchema

base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))

from intel_analytics.table.bigdataframe import BigDataFrame
from intel_analytics.table.bigdataframe import BigDataFrameException
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.hbase.table import HBaseFrameBuilder, HBaseTable
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.hbase.table import Imputation
from intel_analytics.config import global_config as CONFIG_PARAMS
from mock import MagicMock, patch


class BigDataFrameTest(unittest.TestCase):

    def create_mock_etl_object(self, result_holder):

        object = ETLSchema()
        object.load_schema = MagicMock()

        def etl_effect(arg):
            result_holder["table_name"] = arg
            result_holder["feature_names"] = object.feature_names
            result_holder["feature_types"] = object.feature_types

        save_action = MagicMock()
        save_action.side_effect = etl_effect
        object.save_schema = save_action
        object.feature_names = ["col1", "col2", "col3"]
        object.feature_types = ["long", "chararray", "long"]
        return object

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
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'first_price_data_greater_than_10'],
                                 '12.99', '')
                self.assertEqual(data[CONFIG_PARAMS['hbase_column_family'] + 'category'], 'fiction', '')

    def test_data_frame_get_schema(self):
        hbase_table = HBaseTable("test_table", "test_file")
        schema_dict = {'dest': 'long',
                       'edge_type': 'chararray',
                       'src': 'long',
                       'vertex_type': 'chararray',
                       'weight': 'long'}
        hbase_table.get_schema = MagicMock(return_value=schema_dict)
        data_frame = BigDataFrame("test_frame", hbase_table)
        schema = data_frame.get_schema()
        self.assertDictEqual(schema_dict, schema)


    def test_data_frame_string_representation(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        string_representation = str(data_frame)
        self.assertTrue(string_representation.__contains__("name:test_frame"))
        self.assertTrue(string_representation.__contains__("lineage:['test_table']"))
        self.assertTrue(string_representation.__contains__("source_file:test_file"))


    def test_data_frame_get_csv(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.to_csv, "test_output_file")

    def test_data_frame_get_json(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.to_json, "test_output_file")


    def test_data_frame_get_xml(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.to_xml, "test_output_file")


    def test_data_frame_get_html(self):
        hbase_table = HBaseTable("test_table", "test_file")
        hbase_table.inspect_as_html = MagicMock(return_value="<table></table>")
        data_frame = BigDataFrame("test_frame", hbase_table)
        self.assertEqual("<table></table>", data_frame.inspect_as_html(10))

    def test_data_frame_append(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)

        other_hbase_table = HBaseTable("test_table2", "test_file")
        other_data_frame = BigDataFrame("test_frame", other_hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.append, other_data_frame)

    def test_data_frame_join(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)

        other_hbase_table = HBaseTable("test_table2", "test_file")
        other_data_frame = BigDataFrame("test_frame", other_hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.join, other_data_frame)

    def test_data_frame_merge(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)

        other_hbase_table = HBaseTable("test_table2", "test_file")
        other_data_frame = BigDataFrame("test_frame", other_hbase_table)
        self.assertRaises(BigDataFrameException, data_frame.merge, other_data_frame)


    def test_data_frame_transform(self):
        hbase_table = HBaseTable("test_table", "test_file")
        hbase_table.transform = MagicMock()
        data_frame = BigDataFrame("test_frame", hbase_table)

        old_lineage_length = len(data_frame.lineage)
        data_frame.transform("original_col", "new_col", EvalFunctions.String.ENDS_WITH, "end_string")
        new_lineage_length = len(data_frame.lineage)
        self.assertEqual(old_lineage_length + 1, new_lineage_length)

    def test_data_frame_dropna(self):
        hbase_table = HBaseTable("test_table", "test_file")
        hbase_table.dropna = MagicMock()
        data_frame = BigDataFrame("test_frame", hbase_table)

        old_lineage_length = len(data_frame.lineage)
        data_frame.dropna()
        new_lineage_length = len(data_frame.lineage)
        self.assertEqual(old_lineage_length + 1, new_lineage_length)

    def test_data_frame_fillna(self):
        hbase_table = HBaseTable("test_table", "test_file")
        hbase_table.fillna = MagicMock()
        data_frame = BigDataFrame("test_frame", hbase_table)

        old_lineage_length = len(data_frame.lineage)
        data_frame.fillna("column_name", "dummy value")
        new_lineage_length = len(data_frame.lineage)
        self.assertEqual(old_lineage_length + 1, new_lineage_length)

    def test_data_frame_impute(self):
        hbase_table = HBaseTable("test_table", "test_file")
        hbase_table.impute = MagicMock()
        data_frame = BigDataFrame("test_frame", hbase_table)

        old_lineage_length = len(data_frame.lineage)
        data_frame.impute("column_name", Imputation.MEAN)
        new_lineage_length = len(data_frame.lineage)
        self.assertEqual(old_lineage_length + 1, new_lineage_length)

    @patch('intel_analytics.table.hbase.table._create_table_name')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_copy(self, hbase_frame_builder_factory, etl_schema_class, etl_base_client_class, _create_table_name):
        result_holder = {}

        hbase_client = MagicMock()
        hbase_client.__exit__ = MagicMock()
        hbase_client.__enter__ = MagicMock(return_value=hbase_client)
        hbase_client.drop_create_table = MagicMock()

        _create_table_name.return_value = "test_new_table"

        etl_base_client_class.return_value = hbase_client
        mock_etl_obj = self.create_mock_etl_object(result_holder)
        etl_schema_class.return_value = mock_etl_obj

        hbase_frame_builder_factory.name_registry.__getitem__ = MagicMock(return_value = None)
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        def copy_side_effect(name, feature_name, feature_types):
            return HBaseTable(name, "")

        data_frame._table.copy_data_frame = MagicMock(side_effect = copy_side_effect)
        new_frame_name = "test_frame_11"
        new_data_frame = HBaseFrameBuilder().copy_data_frame(data_frame, new_frame_name)
        self.assertEqual(new_data_frame.name, new_frame_name)
        self.assertNotEqual(data_frame._original_table_name, new_data_frame._original_table_name)
        self.assertEqual(new_data_frame._original_table_name, new_data_frame._table.table_name)
        mock_etl_obj.save_schema.assert_called_once_with(new_data_frame._table.table_name)
        hbase_client.drop_create_table.assert_called_once_with("test_new_table", [CONFIG_PARAMS['hbase_column_family']])

    def test_drop_columns(self):
        hbase_table = HBaseTable("test_table", "test_file")
        data_frame = BigDataFrame("test_frame", hbase_table)
        drop_action = MagicMock()
        data_frame._table.drop_columns = drop_action

        columns_to_drop = "f1,f2"
        data_frame.drop_columns(columns_to_drop)
        drop_action.assert_called_once_with(columns_to_drop)


    # @patch('intel_analytics.table.hbase.table.call')
    # @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory.name_registry.get_key')
    # def test_mock(self, call_method, get_by_key):
    #
    #     frame_name = "test_frame"
    #     table_name = _create_table_name(frame_name, False)
    #     hbase_table = HBaseTable(table_name, "test_file")
    #     data_frame = BigDataFrame(frame_name, hbase_table)
    #     hbase_frame_builder_factory.name_registry.register(table_name, frame_name)
    #     call_method = Mock()
    #     get_by_key = Mock(return_value = frame_name)
    #     old_lineage_length = len(data_frame.lineage)
    #     data_frame.impute("column_name", Imputation.MEAN)
    #     new_lineage_length = len(data_frame.lineage)
    #     self.assertEqual(old_lineage_length + 1, new_lineage_length)



    def test(self):
        print '###########################'
        print 'Validating BigDataFrame API'
        print '###########################'

        temp_tables = []

        #first validate json import
        test_json = '{ "store": {"book": [{ "category": "reference", "empty_field":"", "boolean_field": true, "null_field": null, "integer_field": 2,"author": "Nigel Rees","title": "Sayings of the Century", "price": 8.95},{ "category": "fiction","author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99,"isbn": "0-553-21311-3"}],"bicycle": {"color": "red","price": 19.95}}}'
        fp = open('/tmp/test.json', 'w')
        fp.write(test_json)
        fp.close()

        big_frame = HBaseFrameBuilder().build_from_json('test_json', '/tmp/test.json')
        big_frame.inspect()

        big_frame.transform('json', 'first_book_author', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[0].author"])
        big_frame.inspect()

        big_frame.transform('json', 'first_book_empty_field', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[0].empty_field"])

        big_frame.transform('json', 'first_books_price', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[0].price"])
        big_frame.inspect()

        big_frame.transform('json', 'first_books_integer_field', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[0].integer_field"])
        big_frame.inspect()

        big_frame.transform('json', 'first_books_boolean_field', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[0].boolean_field"])
        big_frame.inspect()

        big_frame.transform('json', 'first_price_data_greater_than_10', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book.findAll{book -> book.price>10}[0].price"])
        big_frame.inspect()

        big_frame.transform('json', 'category', EvalFunctions.Json.EXTRACT_FIELD,
                            transformation_args=["store.book[1].category"])

        self.validate_json_extract(big_frame._table.table_name)

        temp_tables.extend(big_frame.lineage)

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

        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_age', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.dropna(column_name='age')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_salary', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.dropna(column_name='salary')
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_any', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.dropna(how='any')
        self.validate_all_nonnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_all', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.dropna(how='all')
        self.validate_no_allnull(big_frame._table.table_name)
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_fillna_age', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.fillna('age', '9999')
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_impute_salary', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.impute('salary', Imputation.MEAN)
        self.validate_nonnull(big_frame._table.table_name, 'salary')
        temp_tables.extend(big_frame.lineage)

        #failure cases
        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_col_doesnt_exist', '/tmp/clean_test.csv', schema,
                                                       True, overwrite=True)
        self.assertRaises(BigDataFrameException, big_frame.dropna, column_name='col_doesnt_exist')
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_dropna_any_ignore', '/tmp/clean_test.csv', schema, True,
                                                       overwrite=True)
        big_frame.dropna(how='any', column_name='age')#should ignore the how parameter and clean age
        self.validate_nonnull(big_frame._table.table_name, 'age')
        temp_tables.extend(big_frame.lineage)

        big_frame = HBaseFrameBuilder().build_from_csv('test_fillna_col_doesnt_exist', '/tmp/clean_test.csv', schema,
                                                       True, overwrite=True)
        self.assertRaises(BigDataFrameException, big_frame.fillna, 'col_doesnt_exist', '{}\"sss')
        temp_tables.extend(big_frame.lineage)

        print 'Cleaning up the temp tables %s & their schema definitions' % (big_frame.lineage)
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
            for temp in temp_tables:
                try:
                    hbase_client.connection.delete_table(temp, disable=True)
                    print 'deleted table', temp
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