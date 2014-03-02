import unittest
from mock import MagicMock
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.table.hbase.table import MAX_ROW_KEY
from intel_analytics.table.pig.pig_script_builder import PigScriptBuilder


class TestPigScriptBuilder(unittest.TestCase):
    def test_get_append_single_table_statement(self):

        etl_schema = ETLSchema()
        etl_schema.load_schema = MagicMock()
        etl_schema.feature_names = ['f1','f2','f3']
        etl_schema.feature_types = ['long','float','chararray']

        property = {}
        property[MAX_ROW_KEY] = '1000'
        etl_schema.get_table_properties = MagicMock(return_value = property)
        pig_builder = PigScriptBuilder()
        script = pig_builder.get_append_tables_statement(etl_schema, 'target_table', ['source_table'])

        statements = script.split("\n")
        self.assertEqual(statements[0], "relation_0_in = LOAD 'hbase://source_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3') as (f1:long,f2:float,f3:chararray);")
        self.assertEqual(statements[1], "combined_relation = relation_0_in;")
        self.assertEqual(statements[2], "temp = rank combined_relation;")
        self.assertEqual(statements[3], "combined_relation_out = foreach temp generate $0 + 1000 as key, f1,f2,f3;")
        self.assertEqual(statements[4], "STORE combined_relation_out INTO 'target_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3');")

    def test_get_append_multiple_table_statement(self):

        etl_schema = ETLSchema()
        etl_schema.feature_names = ['f1','f2','f3']
        etl_schema.feature_types = ['long','float','chararray']

        def load_schema_side_effect(source):
            if source == 'input_1':
                etl_schema.feature_names = ['f1','f2','f3']
                etl_schema.feature_types = ['long','float','chararray']
            elif source == 'input_2':
                etl_schema.feature_names = ['f2','f3','f4']
                etl_schema.feature_types = ['long','float','chararray']

        etl_schema.load_schema = MagicMock(side_effect = load_schema_side_effect)


        property = {}
        property[MAX_ROW_KEY] = '1000'
        etl_schema.get_table_properties = MagicMock(return_value = property)
        pig_builder = PigScriptBuilder()
        script = pig_builder.get_append_tables_statement(etl_schema, 'target_table', ['input_1', 'input_2'])

        statements = script.split("\n")
        self.assertEqual(statements[0], "relation_0_in = LOAD 'hbase://input_1' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3') as (f1:long,f2:float,f3:chararray);")
        self.assertEqual(statements[1], "relation_1_in = LOAD 'hbase://input_2' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f2 etl-cf:f3 etl-cf:f4') as (f2:long,f3:float,f4:chararray);")
        self.assertEqual(statements[2], "combined_relation = UNION ONSCHEMA relation_0_in,relation_1_in;")
        self.assertEqual(statements[3], "temp = rank combined_relation;")
        self.assertEqual(statements[4], "combined_relation_out = foreach temp generate $0 + 1000 as key, f1,f2,f3,f4;")
        self.assertEqual(statements[5], "STORE combined_relation_out INTO 'target_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3 etl-cf:f4');")

if __name__ == '__main__':
    unittest.main()
