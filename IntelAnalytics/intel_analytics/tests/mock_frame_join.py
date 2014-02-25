import os
import unittest
import sys

from intel_analytics.table.bigdataframe import BigDataFrame
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.table.hbase.table import MAX_ROW_KEY, HBaseTable
from intel_analytics.config import global_config as config, global_config
from mock import patch, Mock, MagicMock

curdir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(curdir, os.pardir)))

if 'intel_analytics.config' in sys.modules:
    del sys.modules['intel_analytics.config']    #this  is done to verify that the global config is not patched by previous test scripts in the test runner.

config['hbase_column_family'] = "etl-cf:"


class MockFrameJoin(unittest.TestCase):
    def create_mock_hbase_client(self, get_result):
        object = MagicMock()
        mock_hbase_table = MagicMock()

        mock_hbase_table.scan = MagicMock(return_value=get_result())
        object.connection.table = MagicMock(return_value=mock_hbase_table)
        object.__exit__ = MagicMock()
        object.__enter__ = MagicMock(return_value=object)
        return object

    def create_mock_hbase_client_for_join(self):
        def get_result():
            yield "row1", {"etl-cf:id": "100", "etl-cf:ratio": "0.3434565", "etl-cf:desc":"test row 1"}
            yield "row2", {"etl-cf:id": "200", "etl-cf:ratio": "0.2387412", "etl-cf:desc":"test row 2"}
        return self.create_mock_hbase_client(get_result)

    def create_mock_table(self, table_name, file_name, schema):
        table = HBaseTable(table_name, file_name)
        table.get_schema = MagicMock(return_value=schema)
        return table

    def create_mock_etl_object(self, result_holder):

        object = ETLSchema()
        object.load_schema = MagicMock()

        def save_schema_side_effect(table_name):
            pass

        def populate_schema_side_effect(schema_string):
            pass

        def save_table_properties_side_effect(table_name, properties):
            pass

        object.feature_names = ["col1", "col2", "col3"]
        object.feature_types = ["long", "chararray", "long"]
        object.populate_schema = MagicMock(side_effect = populate_schema_side_effect)
        object.save_schema = MagicMock(side_effect = save_schema_side_effect)
        object.save_table_properties = MagicMock(side_effect = save_table_properties_side_effect)

        return object

    def join_test_invalid_right(self, tL, tR, info):
        try:
             tJ = tL.join(tR,\
                         how='inner', \
                         left_on='lcol1', \
                         right_on='rcol1', \
                         suffixes=['_l', '_r'], \
                         join_frame_name='join_frame')
        except Exception, e:
            print "%s: caught exception %s" %(info, str(e))

    def join_test_invalid_right_on(self, tL, tR, rigth_on, info):
        try:
             tJ = tL.join([tR],\
                         how='inner', \
                         left_on='lcol1', \
                         right_on=right_on, \
                         suffixes=['_l', '_r'], \
                         join_frame_name='join_frame')
        except Exception, e:
            print "%s: caught exception %s" %(info, str(e))

    def join_test_invalid_suffixes(self, tL, tR, suffixes, info):
        try:
             tJ = tL.join([tR],\
                         how='inner', \
                         left_on='lcol1', \
                         right_on=['rcol1'], \
                         suffixes=suffixes, \
                         join_frame_name='join_frame')
        except Exception, e:
            print "%s: caught exception %s" %(info, str(e))

    def join_test_invalid_columns(self, tL, tR, columns, info):
        try:
             tJ = tL.join([tR],\
                         how='inner', \
                         left_on='lcol1', \
                         right_on=columns, \
                         suffixes=['_l', '_r'], \
                         join_frame_name='join_frame')
        except Exception, e:
            print "%s: caught exception %s" %(info, str(e))


    def join_test_invalid_join_type(self, tL, tR, join_type, info):
        try:
             tJ = tL.join([tR],\
                         how=join_type, \
                         left_on='lcol1', \
                         right_on=['rcol1'], \
                         suffixes=['_l', '_r'], \
                         join_frame_name='join_frame')
        except Exception, e:
            print "%s: caught exception %s" %(info, str(e))

    def join_test_single_column(self, tL, tR, info, result_holder):
        for how in ['inner', 'outer', 'left', 'right']:
            join_name = 'join_single_' + how
            # mock the joined table
            tJ = tL.join([tR], how=how,
                         left_on='lcol1', \
                         right_on=['rcol1'], \
                         suffixes=['_l', '_r'], \
                         join_frame_name=join_name)

            args = result_holder["call_args"]
            print "%s: args: d: %s" %(info, '\t'.join(args))

    def join_test_multiple_column(self, tL, tR, info, result_holder):
        for how in ['inner', 'outer', 'left', 'right']:
            join_name = 'join_multiple_' + how
            # mock the joined table
            tJ = tL.join([tR], how=how,
                         left_on='lcol1, lcol2', \
                         right_on=['rcol1,rcol2'], \
                         suffixes=['_l', '_r'], \
                         join_frame_name=join_name)

            args = result_holder["call_args"]
            print "%s: args: d: %s" %(info, '\t'.join(args))

    def join_test_self(self, tL, info, result_holder):
        for how in ['inner', 'outer', 'left', 'right']:
            join_name = 'join_self_' + how
            # mock the joined table
            tJ = tL.join([tL], how=how,
                         left_on='lcol1, lcol2', \
                         right_on=['lcol1,lcol2'], \
                         suffixes=['_1', '_2'], \
                         join_frame_name=join_name)

            args = result_holder["call_args"]
            print "%s: args: d: %s" %(info, '\t'.join(args))

    def join_test_multiple_tables(self, tL, tR1, tR2, info, result_holder):
        for how in ['inner', 'outer', 'left', 'right']:
            join_name = 'join_self_' + how
            # mock the joined table
            tJ = tL.join([tR1, tR2], how=how,
                         left_on='lcol1, lcol2', \
                         right_on=['r1c1,r1c2', 'r2c1,r2c2'], \
                         suffixes=['_x', '_y1', '_y2'], \
                         join_frame_name=join_name)

            args = result_holder["call_args"]
            print "%s: args: d: %s" %(info, '\t'.join(args))

    @patch('intel_analytics.table.hbase.table.PigJobReportStrategy')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch('intel_analytics.table.hbase.table.HBaseRegistry.register')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.PigScriptBuilder.get_join_statement')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient.drop_create_table')
    @patch('intel_analytics.table.hbase.table._create_table_name')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_join(self, \
                  etl_schema_class, \
                  create_table_name_method, \
                  drop_create_table_method,  \
                  get_join_statement_method, \
                  call_method, \
                  register_method, \
                  etl_base_client_class, \
                  pig_report_strategy_class):

        def create_table_name_side_effect(name, overwrite):
            return "join_table"

        def drop_create_table_side_effect(table_name, column_family_descriptors):
            pass

        def get_join_statement_side_effect(etl_schema, tables, on, how, suffixes, join_table_name):
            join_pig_script = 'mock test script'
            join_pig_schema = 'mock test schema'
            return join_pig_script, join_pig_schema

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg
            return 0

        def register_side_effect(key, table_name, overwrite=False, append=False, delete_table=False):
            pass

        # output
        result_holder = {}

        #lefte and rigth mock tables
        tL = self.create_mock_table('tL', 'table_left', {'lcol1':'long', 'lcol2':'double', 'lcol3':'chararray'})
        tR = self.create_mock_table('tR', 'table_right', {'rcol1':'long', 'rcol2':'double', 'rcol3':'chararray'})
        tRR = self.create_mock_table('tRR', 'table_right_right', {'rcol1':'long', 'rcol2':'double', 'rcol3':'chararray'})

        create_table_name_method.side_effect = create_table_name_side_effect
        drop_create_table_method.side_effect = drop_create_table_side_effect
        get_join_statement_method.side_effect = get_join_statement_side_effect
        call_method.side_effect = call_side_effect
        register_method.side_effect = register_side_effect

        etl_result_holder = {}
        etl_object = self.create_mock_etl_object(etl_result_holder)
        etl_object.return_value = etl_object

        # all negative cases for inputs validation
        self.join_test_invalid_right(tL, tR, 'Test:right must be a list')
        self.join_test_invalid_right_on(tL, tR, 'dummy_right_col', 'Test:right_on must be a list')
        self.join_test_invalid_suffixes(tL, tR, 'dummy_suffixes', 'Test:suffixes must match')
        self.join_test_invalid_columns(tL, tR, ['col1', 'col2_fake'], 'Test:right_on size must match')
        self.join_test_invalid_join_type(tL, tR, 'dummy_join_type', 'Test:type unknown')

        # mock the following for the actual join operations
        etl_base_client_class.return_value = self.create_mock_hbase_client_for_join()

        pig_report_strategy = MagicMock()
        report_content = {}
        report_content['input_count'] = '1500'
        pig_report_strategy.content = report_content
        pig_report_strategy_class.return_value = pig_report_strategy
        self.join_test_single_column(tL, tR, 'Test:single column', result_holder)
        self.join_test_multiple_column(tL, tR, 'Test:multiple columns', result_holder)
        self.join_test_self(tL, 'Test:self joihn', result_holder)
        self.join_test_multiple_tables(tL, tR, tRR, 'Test:multiple table join', result_holder)

if __name__ == '__main__':
    unittest.main()
