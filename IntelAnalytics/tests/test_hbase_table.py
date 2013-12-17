import os
import unittest
import sys

curdir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(curdir, os.pardir)))

from intel_analytics.config import global_config as config
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.table.hbase.table import HBaseTable, Imputation, HBaseTableException
from tests.mock import patch, Mock, MagicMock


class HbaseTableTest(unittest.TestCase):

    def create_mock_etl_object(self, result_holder):

        object = ETLSchema()
        object.load_schema = Mock()

        def etl_effect(arg):
            result_holder["feature_names"] = object.feature_names
            result_holder["feature_types"] = object.feature_types

        save_action = Mock()
        save_action.side_effect = etl_effect
        object.save_schema = save_action
        object.feature_names = ["col1", "col2", "col3"]
        object.feature_types = ["long", "chararray", "long"]
        return object

    def create_mock_hbase_client(self, get_result):
        object = Mock()
        mock_hbase_table = Mock()

        mock_hbase_table.scan = Mock(return_value=get_result())
        object.connection.table = MagicMock(return_value=mock_hbase_table)
        object.__exit__ = MagicMock()
        object.__enter__ = MagicMock(return_value=object)
        return object

    def create_mock_hbase_client_same_columns_in_rows(self):
        def get_result():
            yield "row1", {"name": "A", "address": "1234 xyz st"}
            yield "row2", {"name": "B", "address": "5678 def ave"}

        return self.create_mock_hbase_client(get_result)

    def create_mock_hbase_client_different_columns_in_rows(self):
        def get_result():
            yield "row1", {"name": "A", "address": "1234 xyz st"}
            yield "row2", {"name": "B", "office": "5678 def ave"}

        return self.create_mock_hbase_client(get_result)

    def create_mock_hbase_client_20_rows_in_table_scan(self):
        def get_result():
            yield "row1", {"name": "A", "address": "1234 xyz st"}
            yield "row2", {"name": "B", "address": "5678 def ave"}
            yield "row3", {"name": "A", "address": "1234 xyz st"}
            yield "row4", {"name": "B", "address": "5678 def ave"}
            yield "row5", {"name": "A", "address": "1234 xyz st"}
            yield "row6", {"name": "B", "address": "5678 def ave"}
            yield "row7", {"name": "A", "address": "1234 xyz st"}
            yield "row8", {"name": "B", "address": "5678 def ave"}
            yield "row9", {"name": "A", "address": "1234 xyz st"}
            yield "row10", {"name": "B", "address": "5678 def ave"}
            yield "row11", {"name": "A", "address": "1234 xyz st"}
            yield "row12", {"name": "B", "address": "5678 def ave"}
            yield "row13", {"name": "A", "address": "1234 xyz st"}
            yield "row14", {"name": "B", "address": "5678 def ave"}
            yield "row15", {"name": "A", "address": "1234 xyz st"}
            yield "row16", {"name": "B", "address": "5678 def ave"}
            yield "row17", {"name": "A", "address": "1234 xyz st"}
            yield "row18", {"name": "B", "address": "5678 def ave"}
            yield "row19", {"name": "A", "address": "1234 xyz st"}
            yield "row20", {"name": "B", "address": "5678 def ave"}

        return self.create_mock_hbase_client(get_result)

    def create_mock_drop_action(self, result_holder):
        def drop_side_effect(output_table, column_name=None, how=None, replace_with=None):
            result_holder["output_table"] = output_table
            result_holder["column_name"] = column_name
            result_holder["how"] = how
            result_holder["replace_with"] = replace_with

        drop_action_mock = Mock(side_effect=drop_side_effect)
        return drop_action_mock

    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_transformation(self, etl_schema_class, call_method):

        result_holder = {}
        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.transform("col1", "new_col1", EvalFunctions.Math.ABS, transformation_args = "transform_args")
        self.assertEqual("new_col1", result_holder["feature_names"][-1])
        self.assertEqual("bytearray", result_holder["feature_types"][-1])

        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual('col1', result_holder["call_args"][result_holder["call_args"].index('-f') + 1])
        self.assertEqual('test_table', result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual('test_table', result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('ABS', result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('col1,col2,col3', result_holder["call_args"][result_holder["call_args"].index('-u') + 1])
        self.assertEqual('long,chararray,long', result_holder["call_args"][result_holder["call_args"].index('-r') + 1])
        self.assertEqual('new_col1', result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual('transform_args', result_holder["call_args"][result_holder["call_args"].index('-a') + 1])

        script_path = os.path.join(config['pig_py_scripts'], 'pig_transform.py')
        self.assertTrue(script_path in result_holder["call_args"])

    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_transform_with_random_column_name(self, etl_schema_class):

        result_holder = {}
        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table.transform, "random_column", "new_col1", EvalFunctions.Math.ABS)

    def test_transform_random_evaulation_function(self):
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table.transform, "random_column", "new_col1", "something random")

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_same_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_same_columns_in_rows()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._get_first_N(10)

        first_row = n_rows[0]
        second_row = n_rows[1]

        self.assertEqual(2, len(n_rows))
        self.assertEqual('1234 xyz st', first_row['address'])
        self.assertEqual('A', first_row['name'])

        self.assertEqual('5678 def ave', second_row['address'])
        self.assertEqual('B', second_row['name'])


    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_same_columns_more_rows_than_specified_range(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_20_rows_in_table_scan()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._get_first_N(10)
        self.assertEqual(10, len(n_rows))

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_different_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_different_columns_in_rows()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._get_first_N(10)

        first_row = n_rows[0]
        second_row = n_rows[1]

        self.assertEqual(2, len(n_rows))
        self.assertEqual('1234 xyz st', first_row['address'])
        self.assertEqual('A', first_row['name'])

        self.assertEqual('5678 def ave', second_row['office'])
        self.assertEqual('B', second_row['name'])

    @patch('intel_analytics.table.hbase.table.sys.stdout')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate(self, etl_base_client_class, stdout):

        etl_base_client_class.return_value = etl_base_client_class.return_value = self.create_mock_hbase_client_same_columns_in_rows()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        table.illustrate()

        #column section starting line
        self.assertEqual('--------------------------------------------------------------------', write_queue[0])
        self.assertEqual('\n', write_queue[1])

        #column names
        self.assertEqual('name\taddress', write_queue[2])
        self.assertEqual('\n', write_queue[3])

        #column section end line
        self.assertEqual('--------------------------------------------------------------------', write_queue[4])
        self.assertEqual('\n', write_queue[5])

        #first row
        self.assertEqual('A  |  1234 xyz st', write_queue[6])


        #row seperator
        self.assertEqual('\n', write_queue[7])

        #second row
        self.assertEqual('B  |  5678 def ave', write_queue[8])
        self.assertEqual('\n', write_queue[9])

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_invalid_range(self, etl_base_client_class):

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table.illustrate, -1)


    @patch('intel_analytics.table.hbase.table.sys.stdout')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_no_result(self, etl_base_client_class, stdout):

        etl_base_client_class.return_value = etl_base_client_class.return_value = self.create_mock_hbase_client_same_columns_in_rows()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        table.illustrate(0)

        #column section starting line
        self.assertEqual('--------------------------------------------------------------------', write_queue[0])
        self.assertEqual('\n', write_queue[1])
        #column names
        self.assertEqual('name\taddress', write_queue[2])


        #column section end line
        self.assertEqual('\n', write_queue[3])
        self.assertEqual('--------------------------------------------------------------------', write_queue[4])
        self.assertEqual('\n', write_queue[5])


        #check printout ends here
        self.assertEqual(6, len(write_queue))

    @patch('intel_analytics.table.hbase.table.sys.stdout')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_rows_with_different_columns(self, etl_base_client_class, stdout):

        etl_base_client_class.return_value = etl_base_client_class.return_value = self.create_mock_hbase_client_different_columns_in_rows()
        write_queue = []
        def write_action(args):
            write_queue.append(args)

        stdout.write = Mock(side_effect = write_action)
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        table.illustrate()

        #column section starting line
        self.assertEqual('--------------------------------------------------------------------', write_queue[0])
        self.assertEqual('\n', write_queue[1])

        #column names
        self.assertEqual('name\taddress', write_queue[2])

        #column section end line
        self.assertEqual('\n', write_queue[3])
        self.assertEqual('--------------------------------------------------------------------', write_queue[4])
        self.assertEqual('\n', write_queue[5])

        #first row
        self.assertEqual('A  |  1234 xyz st', write_queue[6])
        self.assertEqual('\n', write_queue[7])

        #second row
        self.assertEqual('B  |  NA', write_queue[8])
        self.assertEqual('\n', write_queue[9])



    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_as_html(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_same_columns_in_rows()

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        html = table.illustrate_as_html()
        expected = '<table border="1"><tr><th>name</th><th>address</th></tr><tr><td>A</td><td>1234 xyz st</td></tr><tr><td>B</td><td>5678 def ave</td></tr></table>'
        self.assertEqual(expected, html)


    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_as_html_rows_with_different_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_different_columns_in_rows()

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        html = table.illustrate_as_html()
        expected = '<table border="1"><tr><th>name</th><th>address</th></tr><tr><td>A</td><td>1234 xyz st</td></tr><tr><td>B</td><td>NA</td></tr></table>'
        self.assertEqual(expected, html)


    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop(self, etl_schema_class, call_method, etl_base_client_class, hbase_frame_builder_factory):

        result_holder = {}
        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        def register_side_effect(key, table_name):
            result_holder["key"] = key
            result_holder["output_table"] = table_name

        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        frame_name = "test_frame"
        hbase_frame_builder_factory.name_registry.register = Mock(side_effect = register_side_effect)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = frame_name)

        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        table._HBaseTable__drop(output_table, 'col1', replace_with="replace")

        self.assertEqual(frame_name, result_holder["key"])
        self.assertEqual(output_table, result_holder["output_table"])

        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(output_table, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('col1,col2,col3', result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual('long,chararray,long', result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('col1', result_holder["call_args"][result_holder["call_args"].index('-f') + 1])


    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop_specify_how(self, etl_schema_class, call_method, etl_base_client_class, hbase_frame_builder_factory):

        result_holder = {}
        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        def register_side_effect(key, table_name):
            result_holder["key"] = key
            result_holder["output_table"] = table_name

        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        frame_name = "test_frame"
        hbase_frame_builder_factory.name_registry.register = Mock(side_effect = register_side_effect)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = frame_name)

        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        table._HBaseTable__drop(output_table, how="any", replace_with="replace")

        self.assertEqual(frame_name, result_holder["key"])
        self.assertEqual(output_table, result_holder["output_table"])

        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(output_table, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('col1,col2,col3', result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual('long,chararray,long', result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('any', result_holder["call_args"][result_holder["call_args"].index('-s') + 1])

    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop_specify_not_specify_column_and_how(self, etl_schema_class):

        result_holder = {}
        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table._HBaseTable__drop, None, None, None, "replace")

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop_failed_call(self, etl_schema_class, call_method, etl_base_client_class):

        result_holder = {}
        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        call_method.return_value = 1
        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table._HBaseTable__drop, output_table, 'col1')



    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop_invalid_column(self, etl_schema_class):

        result_holder = {}
        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)
        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        self.assertRaises(HBaseTableException, table._HBaseTable__drop, output_table, 'random_col')

    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_dropna(self, hbase_frame_builder_factory):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = "test_frame")

        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.dropna(column_name = column_to_clean)
        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual("any", result_holder["how"])

    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_fillna(self, hbase_frame_builder_factory):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = "test_frame")

        column_to_clean = "col1"
        replace_with = "N/A"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.fillna(column_to_clean, replace_with)

        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual(replace_with, result_holder["replace_with"])
        self.assertEqual(None, result_holder["how"])


    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_impute(self, hbase_frame_builder_factory):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = "test_frame")
        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.impute(column_to_clean, Imputation.MEAN)

        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual('avg', result_holder["replace_with"])
        self.assertEqual(None, result_holder["how"])


    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_impute_random_method_string(self, hbase_frame_builder_factory):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = "test_frame")
        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        self.assertRaises(HBaseTableException, table.impute, column_to_clean, "random method")

    @patch('intel_analytics.table.hbase.table.hbase_frame_builder_factory')
    def test_impute_random_method_int(self, hbase_frame_builder_factory):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_frame_builder_factory.name_registry.get_key = Mock(return_value = "test_frame")
        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        self.assertRaises(HBaseTableException, table.impute, column_to_clean, 10000)

    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_get_schema(self, etl_schema_class):

        result_holder= {}
        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        schema = table.get_schema()

        self.assertEqual('long', schema['col1'])
        self.assertEqual('chararray', schema['col2'])
        self.assertEqual('long', schema['col3'])


if __name__ == '__main__':
    unittest.main()
