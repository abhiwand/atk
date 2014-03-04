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
import unittest
import sys
from intel_analytics.graph.titan.graph import BulbsGraphWrapper

curdir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(curdir, os.pardir)))

if 'intel_analytics.config' in sys.modules:
    del sys.modules['intel_analytics.config']    #this  is done to verify that the global config is not patched by previous test scripts in the test runner.

from intel_analytics.config import global_config as config, global_config
from intel_analytics.table.builtin_functions import EvalFunctions
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.table.hbase.table import HBaseTable, Imputation, HBaseTableException, HBaseFrameBuilder
from mock import patch, Mock, MagicMock

config['hbase_column_family'] = "etl-cf:"

class HbaseTableTest(unittest.TestCase):

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

    def create_mock_hbase_client(self, get_result):
        object = MagicMock()
        mock_hbase_table = MagicMock()

        mock_hbase_table.scan = MagicMock(return_value=get_result())
        object.connection.table = MagicMock(return_value=mock_hbase_table)
        object.__exit__ = MagicMock()
        object.__enter__ = MagicMock(return_value=object)
        return object

    def create_mock_hbase_client_same_columns_in_rows(self):
        def get_result():
            yield "row1", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row2", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}

        return self.create_mock_hbase_client(get_result)

    def create_mock_hbase_client_different_columns_in_rows(self):
        def get_result():
            yield "row1", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row2", {"etl-cf:name": "B", "etl-cf:office": "5678 def ave"}

        return self.create_mock_hbase_client(get_result)

    def create_mock_hbase_client_20_rows_in_table_scan(self):
        def get_result():
            yield "row1", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row2", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row3", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row4", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row5", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row6", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row7", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row8", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row9", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row10", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row11", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row12", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row13", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row14", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row15", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row16", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row17", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row18", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}
            yield "row19", {"etl-cf:name": "A", "etl-cf:address": "1234 xyz st"}
            yield "row20", {"etl-cf:name": "B", "etl-cf:address": "5678 def ave"}

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

    @patch('intel_analytics.table.hbase.table.call')
    def test_copy_table(self, call_method):
        result_holder = {}

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        new_table_name = "test_output_table"
        new_table = table.copy(new_table_name, 'f1,f2,f3', 't1,t2,t3')
        self.assertEqual(new_table.table_name, new_table_name)
        # make sure the original table is not affected at all
        self.assertEqual(table_name, table.table_name)
        # check call arguments
        self.assertEqual('test_table', result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(new_table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('f1,f2,f3', result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual('t1,t2,t3', result_holder["call_args"][result_holder["call_args"].index('-t') + 1])

    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_drop_column(self, etl_schema_class, call_method):

        result_holder = {}
        mock_etl_obj = self.create_mock_etl_object(result_holder)

        etl_schema_class.return_value = mock_etl_obj

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect


        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        columns_to_drop = "col1,col2"
        table.drop_columns(columns_to_drop)

        mock_etl_obj.save_schema.assert_called_once_with(table_name)
        self.assertEqual(result_holder["feature_names"], ["col3"])
        self.assertEqual(result_holder["feature_types"], ["long"])


        call_args = result_holder["call_args"]
        self.assertEqual("hadoop", call_args[0])
        self.assertEqual("jar", call_args[1])
        self.assertEqual(global_config['intel_analytics_jar'], call_args[2])
        self.assertEqual(global_config['column_dropper_class'], call_args[3])

        # check call arguments
        # check table name
        self.assertEqual(table_name, call_args[call_args.index('-t') + 1])
        # check column names
        self.assertEqual(columns_to_drop, call_args[call_args.index('-n') + 1])

        # check column family
        self.assertEqual('etl-cf', call_args[call_args.index('-f') + 1])

    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_drop_column(self, etl_schema_class):
        etl_schema_class.return_value = self.create_mock_etl_object({})
        table = HBaseTable("test_table", "test_file")
        try:
            table.drop_columns("bogus,col2,garbage")
            self.fail()
        except Exception as e:
            self.assertEqual("Column 'bogus' not in frame; Column 'garbage' not in frame", e.message)

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_same_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_same_columns_in_rows()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._peek(10)

        first_row = n_rows[0]
        second_row = n_rows[1]

        self.assertEqual(2, len(n_rows))
        self.assertEqual('1234 xyz st', first_row['etl-cf:address'])
        self.assertEqual('A', first_row['etl-cf:name'])

        self.assertEqual('5678 def ave', second_row['etl-cf:address'])
        self.assertEqual('B', second_row['etl-cf:name'])


    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_same_columns_more_rows_than_specified_range(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_20_rows_in_table_scan()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._peek(10)
        self.assertEqual(10, len(n_rows))

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_get_first_N_different_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_different_columns_in_rows()
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        n_rows = table._peek(10)

        first_row = n_rows[0]
        second_row = n_rows[1]

        self.assertEqual(2, len(n_rows))
        self.assertEqual('1234 xyz st', first_row['etl-cf:address'])
        self.assertEqual('A', first_row['etl-cf:name'])

        self.assertEqual('5678 def ave', second_row['etl-cf:office'])
        self.assertEqual('B', second_row['etl-cf:name'])

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
        table.get_schema = Mock(return_value={config['hbase_column_family'] + 'name':'chararray', config['hbase_column_family'] + 'address':'chararray'})
        table.inspect()

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
        self.assertRaises(HBaseTableException, table.inspect, -1)


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
        table.inspect(0)

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
        table.inspect()

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
        html = table.inspect_as_html()
        expected = '<table border="1"><tr><th>name</th><th>address</th></tr><tr><td>A</td><td>1234 xyz st</td></tr><tr><td>B</td><td>5678 def ave</td></tr></table>'
        self.assertEqual(expected, html)


    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_illustrate_as_html_rows_with_different_columns(self, etl_base_client_class):

        etl_base_client_class.return_value = self.create_mock_hbase_client_different_columns_in_rows()

        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        table.get_schema = Mock(return_value={'name':'chararray', 'address':'chararray'})
        html = table.inspect_as_html()
        expected = '<table border="1"><tr><th>name</th><th>address</th></tr><tr><td>A</td><td>1234 xyz st</td></tr><tr><td>B</td><td>NA</td></tr></table>'
        self.assertEqual(expected, html)


    @patch('intel_analytics.table.hbase.table.hbase_registry')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop(self, etl_schema_class, call_method, etl_base_client_class, hbase_registry):

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

        hbase_registry.register = Mock(side_effect = register_side_effect)
        hbase_registry.get_key = Mock(return_value = frame_name)

        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        table._HBaseTable__drop(output_table, 'col1', replace_with="replace")

        #self.assertEqual(frame_name, result_holder["key"])  This is never added to the registry during the drop call
        self.assertEqual(output_table, result_holder["table_name"])  #table name is replaced with value of output table during drop function

        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(output_table, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('col1,col2,col3', result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual('long,chararray,long', result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('col1', result_holder["call_args"][result_holder["call_args"].index('-f') + 1])


    @patch('intel_analytics.table.hbase.table.hbase_registry')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test__drop_specify_how(self, etl_schema_class, call_method, etl_base_client_class, hbase_registry):

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


        hbase_registry.register = Mock(side_effect = register_side_effect)
        hbase_registry.get_key = Mock(return_value = frame_name)

        table_name = "test_table"
        file_name = "test_file"
        output_table = "output_table"
        table = HBaseTable(table_name, file_name)
        table._HBaseTable__drop(output_table, how="any", replace_with="replace")


        #self.assertEqual(frame_name, result_holder["key"]) This is never added to the registry during the drop call
        self.assertEqual(output_table, result_holder["table_name"])  #table name is replaced with value of output table during drop function

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

    @patch('intel_analytics.table.hbase.table.hbase_registry')
    def test_dropna(self, hbase_registry):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_registry.get_key = Mock(return_value = "test_frame")

        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.dropna(column_name = column_to_clean)
        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual("any", result_holder["how"])

    @patch('intel_analytics.table.hbase.table.hbase_registry')
    def test_fillna(self, hbase_registry):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_registry.get_key = Mock(return_value = "test_frame")

        column_to_clean = "col1"
        replace_with = "N/A"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.fillna(column_to_clean, replace_with)

        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual(replace_with, result_holder["replace_with"])
        self.assertEqual(None, result_holder["how"])


    @patch('intel_analytics.table.hbase.table.hbase_registry')
    def test_impute(self, hbase_registry):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_registry.get_key = Mock(return_value = "test_frame")
        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        table.impute(column_to_clean, Imputation.MEAN)

        self.assertEqual(column_to_clean, result_holder["column_name"])
        self.assertEqual('avg', result_holder["replace_with"])
        self.assertEqual(None, result_holder["how"])


    @patch('intel_analytics.table.hbase.table.hbase_registry')
    def test_impute_random_method_string(self, hbase_registry):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_registry.get_key = Mock(return_value = "test_frame")
        column_to_clean = "col1"
        table._HBaseTable__drop = self.create_mock_drop_action(result_holder)
        self.assertRaises(HBaseTableException, table.impute, column_to_clean, "random method")

    @patch('intel_analytics.table.hbase.table.hbase_registry')
    def test_impute_random_method_int(self, hbase_registry):

        result_holder = {}
        table_name = "test_table"
        file_name = "test_file"
        table = HBaseTable(table_name, file_name)
        hbase_registry.get_key = Mock(return_value = "test_frame")
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

    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def transform_with_multiple_columns(self, featin, featout, featop, etl_schema_class, call_method):

        table_name = "cross_column_test_table"
        file_name = "cross_column_test_file"
        feat_name = ["long1", "long2", "double1", "double2", "str1", "str2", "str3"]
        feat_type = ["long",  "long", "double",  "double",  "chararray", "chararray", "chararray"]
        result_holder = {}

        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)
        etl_schema_class.return_value.feature_names = feat_name
        etl_schema_class.return_value.feature_types = feat_type
        etl_schema_class.return_value.save_schema(table_name)

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        featname = ','.join(feat_name)
        feattype = ','.join(feat_type)
        featfunc = EvalFunctions.to_string(featop)

        table = HBaseTable(table_name, file_name)
        table.transform(featin, featout, featop, transformation_args = "transform_args")

        self.assertEqual('pig', str(result_holder["call_args"][0]))
        self.assertEqual(featin, result_holder["call_args"][result_holder["call_args"].index('-f') + 1])
        self.assertEqual(featout, result_holder["call_args"][result_holder["call_args"].index('-n') + 1])
        self.assertEqual(featname, result_holder["call_args"][result_holder["call_args"].index('-u') + 1])
        self.assertEqual(feattype, result_holder["call_args"][result_holder["call_args"].index('-r') + 1])
        self.assertEqual(featfunc, result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual('transform_args', result_holder["call_args"][result_holder["call_args"].index('-a') + 1])

        script_path = os.path.join(config['pig_py_scripts'], 'pig_transform.py')
        self.assertTrue(script_path in result_holder["call_args"])

    def test_arithmetics(self):
        self.transform_with_multiple_columns("long1+long2", "long3", EvalFunctions.Math.ARITHMETIC)
        self.transform_with_multiple_columns("double1+double2", "double3", EvalFunctions.Math.ARITHMETIC)
        self.transform_with_multiple_columns("double1-double2+4/5*6", "double4", EvalFunctions.Math.ARITHMETIC)

    def test_concatentaion(self):
        self.transform_with_multiple_columns("str1,str2", "strout1", EvalFunctions.String.CONCAT)
        self.transform_with_multiple_columns("str1,str2,str3", "strout2", EvalFunctions.String.CONCAT)
        self.transform_with_multiple_columns("str1,str2,\'MyString1\',str3,\'MyString2\'", "strout3", EvalFunctions.String.CONCAT)

    def test_concatentaion_with_random_columns(self):
        test_expression="fakecol1,fakecol2"
        msg="exception on invalid column inputs for CONCAT:' %s '" % test_expression
        try:
            self.transform_with_multiple_columns(test_expression, "fakeout3", EvalFunctions.String.CONCAT)
            self.assertTrue(False, "Failed to catch %s" % msg)
        except HBaseTableException, e:
            print "Caught %s" % msg

    def test_arithmetics_with_random_columns(self):
        test_expression="fakecol1+fakecol2"
        msg="exception on invalid column inputs for ARITHMETIC:' %s '" % test_expression
        try:
            self.transform_with_multiple_columns(test_expression, "fakeout3", EvalFunctions.Math.ARITHMETIC)
            self.assertTrue(False, "Failed to catch %s" % msg)
        except HBaseTableException, e:
            print "Caught %s" % msg

    def test_arithmetics_with_random_spaces(self):
        test_expression="long1 + long2 * double1 - double2"
        self.transform_with_multiple_columns(test_expression, "fout", EvalFunctions.Math.ARITHMETIC)

    def test_arithmetics_with_random_parentheses(self):
        test_expression="long1((+long2*(double1/)((double4("
        msg="exception on given ARITHMETIC expression with parentheses:' %s '" % test_expression
        try:
            self.transform_with_multiple_columns(test_expression, "fout", EvalFunctions.Math.ARITHMETIC)
            self.assertTrue(False, "Failed to catch %s" % msg)
        except HBaseTableException, e:
            print "Caught %s" % msg

    @patch('intel_analytics.graph.titan.graph.call')
    def test_export_as_graphml(self, call_method):

        result_holder = {}

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect
        graph = MagicMock()
        graph.vertices = MagicMock()
        graph.edges = MagicMock()
        wrapper = BulbsGraphWrapper(graph)
        wrapper.titan_table_name = "test_table"
        statements = ["g.V('_gb_ID','11').out"]
        wrapper.export_as_graphml(statements, "output.xml")
        self.assertEqual("\"<query><statement>g.V('_gb_ID','11').out.transform('{[it,it.map()]}')</statement></query>\"", result_holder["call_args"][result_holder["call_args"].index('-q') + 1])


    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def aggregate(self, groupby, aggregations, etl_schema_class, call_method):

        table_name = "original_table"
        file_name = "original_file"
	aggregated_table_name = "aggregated_table"
        feat_name = ["long1", "long2", "double1", "double2", "str1", "str2", "str3"]
        feat_type = ["long",  "long", "double",  "double",  "chararray", "chararray", "chararray"]
        result_holder = {}

        etl_schema_class.return_value = self.create_mock_etl_object(result_holder)
        etl_schema_class.return_value.feature_names = feat_name
        etl_schema_class.return_value.feature_types = feat_type
        etl_schema_class.return_value.save_schema(table_name)

        featname = ','.join(feat_name)
        feattype = ','.join(feat_type)

        table = HBaseTable(table_name, file_name)
	
        table.aggregate(aggregated_table_name, groupby, aggregations)

        self.assertEqual('pig', str(result_holder["call_args"][0]))
        self.assertEqual(",".join(groupby), result_holder["call_args"][result_holder["call_args"].index('-g') + 1])
        self.assertEqual(featname, result_holder["call_args"][result_holder["call_args"].index('-u') + 1])
        self.assertEqual(feattype, result_holder["call_args"][result_holder["call_args"].index('-r') + 1])
        self.assertEqual(table_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(aggregated_table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])

        script_path = os.path.join(config['pig_py_scripts'], 'pig_aggregation.py')
        self.assertTrue(script_path in result_holder["call_args"])


    def test_aggregate_max(self):
	try:
	    self.aggregate("long1", [(EvalFunctions.Aggregation.MAX, "long2", "maxlong2")])
	except:
	    print "Caught exception on aggregation"

    def test_aggregate_on_multicolumns(self):
	try:
	    self.aggregate("str1,str2", [(EvalFunctions.Aggregation.AVG, "long2", "maxlong2")])
	except:
	    print "Caught exception on aggregation multiple columns"

    def test_aggregate_multiple(self):
	try:
	    self.aggregate("str1,str2", [(EvalFunctions.Aggregation.AVG, "long2", "maxlong2"), [EvalFunctions.Aggregation.SUM, "double1", "totaldouble1"]])
	except:
	    print "Caught exception on aggregation multiple columns"

class HBaseFrameBuilderTest(unittest.TestCase):

    @patch("intel_analytics.table.hbase.table.exists_hdfs")
    def test_validate_exists_hdfs(self, exists):
        exists.return_value = True

        builder = HBaseFrameBuilder()
        builder._validate_exists('/mock/location/that/exists')

    @patch("intel_analytics.table.hbase.table.exists_hdfs")
    def test_validate_exists_not_found_in_hdfs(self, exists):
        exists.return_value = False

        builder = HBaseFrameBuilder()
        with self.assertRaises(Exception):
            builder._validate_exists('/mock/location/that/does/not/exist')

    @patch("intel_analytics.table.hbase.table.is_local_run")
    @patch("intel_analytics.table.hbase.table.os.path.isfile")
    def test_validate_exists_locally(self, is_file, is_local_run):
        is_file.return_value = True
        is_local_run.return_value = True

        builder = HBaseFrameBuilder()
        builder._validate_exists('/mock/location/that/exists')

    @patch("intel_analytics.table.hbase.table.is_local_run")
    def test_validate_exists_not_found_locally(self, is_local_run):
        is_local_run.return_value = True

        builder = HBaseFrameBuilder()
        with self.assertRaises(Exception):
            builder._validate_exists('/some/real/path/that/does/not/exist')

if __name__ == '__main__':
    unittest.main()
