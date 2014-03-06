import unittest
from mock import patch, MagicMock

from intel_analytics.table.bigdataframe import get_frame_builder, BigDataFrame
from intel_analytics.table.hbase.schema import ETLSchema, merge_schema
from intel_analytics.table.hbase.table import MAX_ROW_KEY, HBaseFrameBuilder

class TestFrameBuilder(unittest.TestCase):

    def create_mock_etl_object(self, result_holder):

        object = ETLSchema()
        object.load_schema = MagicMock()

        def etl_effect(arg):
            result_holder["table_name"] = arg
            result_holder["feature_names"] = object.feature_names
            result_holder["feature_types"] = object.feature_types

        def save_properties_effect(table_name, properties):
            result_holder['properties'] = properties


        save_action = MagicMock()
        save_action.side_effect = etl_effect
        object.save_schema = save_action
        object.feature_names = ["col1", "col2", "col3"]
        object.feature_types = ["long", "chararray", "long"]

        properties = {}
        properties[MAX_ROW_KEY] = '1000'
        object.get_table_properties = MagicMock(return_value = properties)

        save_properties_action = MagicMock()
        save_properties_action.side_effect = save_properties_effect
        object.save_table_properties = save_properties_action
        return object

    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    @patch("intel_analytics.table.hbase.table.hbase_registry")
    @patch("intel_analytics.table.hbase.table._create_table_name")
    @patch('intel_analytics.table.hbase.schema.ETLSchema')
    @patch('intel_analytics.table.hbase.table.PigJobReportStrategy')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_build_from_csv_multiple_files(self, etl_schema_class, call_method, pig_report_strategy_class, etl_schema_class_schema, create_table_name_func, hbase_registry, etl_base_client_class):
        result_holder = {}
        etl_object = self.create_mock_etl_object(result_holder)
        etl_object.feature_names = []
        etl_object.feature_types = []
        etl_schema_class.return_value = etl_object
        etl_schema_class_schema.return_value = MagicMock()
        pig_report_strategy = MagicMock()
        report_content = {}
        report_content['input_count'] = '1500'
        pig_report_strategy.content = report_content
        pig_report_strategy_class.return_value = pig_report_strategy

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        etl_base_client_class.drop_create_table = MagicMock()

        create_table_name_func.return_value = "test_table"
        hbase_registry.register = MagicMock()
        table = MagicMock()
        table.table_name = 'test_table'

        fb = get_frame_builder()
        file_name = ['/test/files/dummy.csv', 'file2']
        fb._validate_exists = MagicMock()
        fb.build_from_csv("frame", file_name, 'col1:long,col2:chararray,col3:long')

        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(','.join(file_name), result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual("test_table", result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual("col1,col2,col3", result_holder["call_args"][result_holder["call_args"].index('-f') + 1])
        self.assertEqual("long,chararray,long", result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('0', result_holder["call_args"][result_holder["call_args"].index('-m') + 1])

    @patch('intel_analytics.table.hbase.schema.ETLSchema')
    @patch('intel_analytics.table.hbase.table.PigJobReportStrategy')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_append_from_csv_same_schema_multiple_files(self, etl_schema_class, call_method, pig_report_strategy_class, etl_schema_class_schema):

        result_holder = {}
        etl_object = self.create_mock_etl_object(result_holder)
        etl_object.feature_names = []
        etl_object.feature_types = []
        etl_schema_class.return_value = etl_object
        etl_schema_class_schema.return_value = MagicMock()
        pig_report_strategy = MagicMock()
        report_content = {}
        report_content['input_count'] = '1500'
        pig_report_strategy.content = report_content
        pig_report_strategy_class.return_value = pig_report_strategy

        def call_side_effect(arg, report_strategy):
            result_holder["call_args"] = arg

        call_method.return_value = None
        call_method.side_effect = call_side_effect

        table = MagicMock()
        table.table_name = 'test_table'

        frame = BigDataFrame('dummy_frame', table)
        fb = get_frame_builder()
        fb._validate_exists = MagicMock()
        file_name = ['/test/files/dummy.csv', 'file1']
        fb.append_from_csv(frame, file_name, 'col1:long,col2:chararray,col3:long')
        properties = result_holder['properties']
        self.assertEqual('2500', properties[MAX_ROW_KEY])
        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(','.join(file_name), result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(table.table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual("col1,col2,col3", result_holder["call_args"][result_holder["call_args"].index('-f') + 1])
        self.assertEqual("long,chararray,long", result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('1000', result_holder["call_args"][result_holder["call_args"].index('-m') + 1])

    def test_append_from_json(self):
        table = MagicMock()
        table.table_name = 'test_table'

        frame = BigDataFrame('dummy_frame', table)
        fb = get_frame_builder()
        file_name = ['/test/files/dummy.csv', 'file1']

        append_data_mock, result_holder = self.create_append_data_mock()

        fb._append_data = MagicMock(side_effect = append_data_mock)
        fb._validate_exists = MagicMock()
        fb.append_from_json(frame, file_name)
        self.assertEqual(table.table_name, result_holder["table_name"])
        self.assertEqual(','.join(file_name), result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(table.table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])

    def create_append_data_mock(self):
        result_holder = {}

        def _append_side_effect(args, etl_schema, table_name):
            result_holder["table_name"] = table_name
            result_holder["call_args"] = args

        append_data_mock = MagicMock(side_effect=_append_side_effect)
        return append_data_mock, result_holder

    def test_append_from_xml(self):
        table = MagicMock()
        table.table_name = 'test_table'

        frame = BigDataFrame('dummy_frame', table)
        fb = get_frame_builder()
        file_name = ['/test/files/dummy.csv', 'file1']

        append_data_mock, result_holder = self.create_append_data_mock()

        tag_name = "tag"

        fb._append_data = append_data_mock
        fb._validate_exists = MagicMock()
        fb.append_from_xml(frame, file_name, tag_name)
        self.assertEqual(table.table_name, result_holder["table_name"])
        self.assertEqual(','.join(file_name), result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(table.table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual(tag_name, result_holder["call_args"][result_holder["call_args"].index('-tag') + 1])

    @patch('intel_analytics.table.hbase.table.PigJobReportStrategy')
    @patch('intel_analytics.table.hbase.schema.ETLSchema')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_append_from_data_frame(self, etl_schema_class, call_method, etl_schema_for_merge, pig_report_strategy_class):

        pig_report_strategy = MagicMock()
        report_content = {}
        report_content['input_count'] = '1500'
        pig_report_strategy.content = report_content
        pig_report_strategy_class.return_value = pig_report_strategy

        call_method.return_value = None
        result_holder = {}
        etl_object1 = self.create_mock_etl_object(result_holder)
        etl_object1.feature_names = []
        etl_object1.feature_types = []
        etl_object2 = self.create_mock_etl_object(result_holder)
        etl_object2.feature_names = []
        etl_object2.feature_types = []
        etl_object3 = self.create_mock_etl_object(result_holder)
        etl_object3.feature_names = []
        etl_object3.feature_types = []

        def load_schema_side_effect_1(source):
            etl_object1.feature_names = ['f1','f2','f3']
            etl_object1.feature_types = ['long','float','chararray']

        def load_schema_side_effect_2(source):
            etl_object2.feature_names = ['f2','f3','f4']
            etl_object2.feature_types = ['long','float','chararray']

        etl_object1.load_schema = MagicMock(side_effect = load_schema_side_effect_1)
        etl_object2.load_schema = MagicMock(side_effect = load_schema_side_effect_2)
        etl_schema_class.side_effect = [etl_object1, etl_object2, etl_object3]

        merge_etl = self.create_mock_etl_object(result_holder)
        merge_etl.feature_names = []
        merge_etl.feature_types = []

        def save_schema_side_effect(table):
            result_holder['feature_names_merged'] = merge_etl.feature_names
            result_holder['feature_types_merged'] = merge_etl.feature_types

        merge_etl.save_schema = MagicMock(side_effect = save_schema_side_effect)
        etl_schema_for_merge.return_value = merge_etl

        table = MagicMock()
        table.table_name = 'test_table'
        table1 = MagicMock()
        table1.table_name = 'input_1'
        table2 = MagicMock()
        table2.table_name = 'input_2'

        frame = BigDataFrame('dummy_frame', table)
        frame1 = BigDataFrame('dummy_frame', table1)
        frame2 = BigDataFrame('dummy_frame', table2)

        fb = get_frame_builder()
        fb.append_from_data_frame(frame, [frame1, frame2])
        self.assertEqual(result_holder['feature_names_merged'], ['f1','f2','f3','f4'])
        properties = result_holder['properties']
        self.assertEqual('2500', properties[MAX_ROW_KEY])

    def test_merge_schema_only_one_schema(self):
        schema1 = ETLSchema()
        schema1.populate_schema('Country:chararray, Year:long, CO2_emission:float')

        merged_schema = merge_schema([schema1])
        self.assertEquals(3, len(merged_schema.feature_names))
        self.assertEquals(3, len(merged_schema.feature_types))
        self.assertTrue('Country' in merged_schema.feature_names)
        self.assertTrue('Year' in merged_schema.feature_names)
        self.assertTrue('CO2_emission' in merged_schema.feature_names)

        self.assertTrue('chararray' in merged_schema.feature_types)
        self.assertTrue('long' in merged_schema.feature_types)
        self.assertTrue('float' in merged_schema.feature_types)

    def test_merge_schema_same_schema(self):
        schema1 = ETLSchema()
        schema1.populate_schema('Country:chararray, Year:long, CO2_emission:float')
        schema2 = ETLSchema()
        schema2.populate_schema('Country:chararray, Year:long, CO2_emission:float')
        schema3 = ETLSchema()
        schema3.populate_schema('Country:chararray, Year:long, CO2_emission:float')

        merged_schema = merge_schema([schema1, schema2, schema3])
        self.assertEquals(3, len(merged_schema.feature_names))
        self.assertEquals(3, len(merged_schema.feature_types))
        self.assertTrue('Country' in merged_schema.feature_names)
        self.assertTrue('Year' in merged_schema.feature_names)
        self.assertTrue('CO2_emission' in merged_schema.feature_names)

        self.assertTrue('chararray' in merged_schema.feature_types)
        self.assertTrue('long' in merged_schema.feature_types)
        self.assertTrue('float' in merged_schema.feature_types)

    def test_merge_schema_different_schema(self):
        schema1 = ETLSchema()
        schema1.populate_schema('Country:chararray, Year:long, CO2_emission:float')
        schema2 = ETLSchema()
        schema2.populate_schema('Country:chararray, Year:long')
        schema3 = ETLSchema()
        schema3.populate_schema('energy_use:chararray, GNI:float, Internet_users:long')

        merged_schema = merge_schema([schema1, schema2, schema3])
        self.assertEquals(6, len(merged_schema.feature_names))
        self.assertEquals(6, len(merged_schema.feature_types))
        self.assertTrue('Country' in merged_schema.feature_names)
        self.assertTrue('Year' in merged_schema.feature_names)
        self.assertTrue('CO2_emission' in merged_schema.feature_names)
        self.assertTrue('energy_use' in merged_schema.feature_names)
        self.assertTrue('GNI' in merged_schema.feature_names)
        self.assertTrue('Internet_users' in merged_schema.feature_names)

    @patch('intel_analytics.table.hbase.table.hbase_registry')
    @patch('intel_analytics.table.hbase.table._create_table_name')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    @patch('intel_analytics.table.hbase.table.ETLHBaseClient')
    def test_copy_data_frame(self, etl_base_client_class, etl_object_class, _create_table_name, hbase_registry):

        hbase_registry.register = MagicMock()
        frame_builder = HBaseFrameBuilder()
        table = MagicMock()
        table.table_name = "test_table"


        _create_table_name.return_value = "new_frame_1234"


        copy_method =  MagicMock(return_value = MagicMock())
        table.copy = copy_method
        data_frame = BigDataFrame("frame", table)
        etl_base_client_class.drop_create_table = MagicMock()

        result_holder = {}
        etl_object = self.create_mock_etl_object(result_holder)
        etl_object.get_feature_names_as_CSV = MagicMock(return_value = 'f1,f2,f3')
        etl_object.get_feature_types_as_CSV = MagicMock(return_value = 'long,long,chararray')
        etl_object_class.return_value = etl_object
        frame_builder.copy_data_frame(data_frame, "new_frame")
        copy_method.assert_called_with("new_frame_1234", 'f1,f2,f3', 'long,long,chararray')




if __name__ == '__main__':
    unittest.main()
