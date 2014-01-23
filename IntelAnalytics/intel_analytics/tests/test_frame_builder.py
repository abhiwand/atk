import unittest
from mock import patch, MagicMock
from intel_analytics.table.bigdataframe import get_frame_builder, BigDataFrame
from intel_analytics.table.hbase.schema import ETLSchema, merge_schema
from intel_analytics.table.hbase.table import MAX_ROW_KEY


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

    @patch('intel_analytics.table.hbase.table.PigJobReportStrategy')
    @patch('intel_analytics.table.hbase.table.call')
    @patch('intel_analytics.table.hbase.table.ETLSchema')
    def test_append_to_append_from_csv_same_schema(self, etl_schema_class, call_method, pig_report_strategy_class):

        result_holder = {}
        etl_object = self.create_mock_etl_object(result_holder)
        etl_object.feature_names = []
        etl_object.feature_types = []
        etl_schema_class.return_value = etl_object

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
        file_name = '/test/files/dummy.csv'
        fb.append_from_csv(frame, file_name, 'col1:long,col2:chararray,col3:long')
        properties = result_holder['properties']
        self.assertEqual('2500', properties[MAX_ROW_KEY])
        # validate call arguments
        self.assertEqual("pig", result_holder["call_args"][0])
        self.assertEqual(file_name, result_holder["call_args"][result_holder["call_args"].index('-i') + 1])
        self.assertEqual(table.table_name, result_holder["call_args"][result_holder["call_args"].index('-o') + 1])
        self.assertEqual("col1,col2,col3", result_holder["call_args"][result_holder["call_args"].index('-f') + 1])
        self.assertEqual("long,chararray,long", result_holder["call_args"][result_holder["call_args"].index('-t') + 1])
        self.assertEqual('1000', result_holder["call_args"][result_holder["call_args"].index('-m') + 1])

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





if __name__ == '__main__':
    unittest.main()
