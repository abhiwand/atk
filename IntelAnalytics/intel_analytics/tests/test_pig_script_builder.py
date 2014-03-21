import unittest
from mock import MagicMock
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.table.hbase.table import MAX_ROW_KEY
from intel_analytics.table.pig.pig_script_builder import PigScriptBuilder, HBaseSource, HBaseLoadFunction, HBaseStoreFunction


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

    def test_get_join_statement(self):

        etl_schema = ETLSchema()
        etl_schema.feature_names = ['f1','f2','f3']
        etl_schema.feature_types = ['long','float','chararray']

        def load_schema_side_effect(source):
            if source == 'table_L':
                etl_schema.feature_names = ['lcol1','lcol2','lcol3']
                etl_schema.feature_types = ['long','float','chararray']
            elif source == 'table_R':
                etl_schema.feature_names = ['rcol2','rcol3','rcol4']
                etl_schema.feature_types = ['long','float','chararray']

        etl_schema.load_schema = MagicMock(side_effect = load_schema_side_effect)

        property = {}
        property[MAX_ROW_KEY] = '1000'
        etl_schema.get_table_properties = MagicMock(return_value = property)
        pig_builder = PigScriptBuilder()
        # the varying part is S2
        S0="L0 = LOAD 'hbase://table_L' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:lcol1 etl-cf:lcol2 etl-cf:lcol3') AS (lcol1:long,lcol2:float,lcol3:chararray);"
        S1="L1 = LOAD 'hbase://table_R' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:rcol2 etl-cf:rcol3 etl-cf:rcol4') AS (rcol2:long,rcol3:float,rcol4:chararray);"
        S2="J = JOIN L0 BY (lcol1) , L1 BY (rcol4);"
        S3="R = RANK J;"
        S4="final = R;"
        S5="STORE final INTO 'hbase://jointable' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:lcol1_l etl-cf:lcol2_l etl-cf:lcol3_l etl-cf:rcol2_r etl-cf:rcol3_r etl-cf:rcol4_r');"
        S6="lcol1_l:long,lcol2_l:float,lcol3_l:chararray, rcol2_r:long,rcol3_r:float,rcol4_r:chararray"
        expected_statements = [S0, S1, S2, S3, S4, S5]

        def join_statement_test(etl_schema, tables, how, on, suffixes, join_table_name, expected, info):
            print "INFO:%s: join type is '%s'" %(info, how)
            try:
                join_pig_script, join_pig_schema = pig_builder.get_join_statement(etl_schema, tables=tables,        \
                                                                                  how=how, on=on, suffixes=suffixes,\
                                                                                  join_table_name=join_table_name)
            except Exception, e:
                print "Caught exception on script generation %s" % str(e)

            statements = []
            # filter out comments
            for s in join_pig_script.split("\n"):
                if s[:2] != '--':
                    statements.append(s)
            statements.append(join_pig_schema)

            for x,y in zip(statements, expected):
                self.assertEqual(x, y)
            print "INFO:%s: join type is '%s'...Passed" %(info, how)

        # single
        expected_statements[2]="J = JOIN L0 BY (lcol1) , L1 BY (rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='inner', \
                            on=['lcol1', 'rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="INNER JOIN on single column" )

        expected_statements[2]="J = JOIN L0 BY (lcol1,lcol2) , L1 BY (rcol3,rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='inner', \
                            on=['lcol1,lcol2', 'rcol3,rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="INNER JOIN on multiple columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1) FULL, L1 BY (rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='outer', \
                            on=['lcol1', 'rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="OUTER JOIN on single columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1,lcol2) FULL, L1 BY (rcol3,rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='outer', \
                            on=['lcol1,lcol2', 'rcol3,rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="OUTER JOIN on multiple columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1) LEFT, L1 BY (rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='left', \
                            on=['lcol1', 'rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="LEFT JOIN on single columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1,lcol2) LEFT, L1 BY (rcol3,rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='left', \
                            on=['lcol1,lcol2', 'rcol3,rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="LEFT JOIN on multiple columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1) RIGHT, L1 BY (rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='right', \
                            on=['lcol1', 'rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="RIGHT JOIN on single columns" )

        expected_statements[2]="J = JOIN L0 BY (lcol1,lcol2) RIGHT, L1 BY (rcol3,rcol4);"
        join_statement_test(etl_schema, tables=['table_L', 'table_R'], how='right', \
                            on=['lcol1,lcol2', 'rcol3,rcol4'], suffixes=['_l', '_r'],    \
                            join_table_name='jointable', \
                            expected=expected_statements,\
                            info="RIGHT JOIN on multiple columns" )

    def test_HBaseSource(self):
        source = HBaseSource('test_table')
        self.assertEqual('hbase://test_table', source.get_data_source_as_string())

    def test_HBaseLoadFunction_load_key(self):
        func = HBaseLoadFunction(['f1', 'f2', 'f3'], True)
        self.assertEqual("org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3', '-loadKey true')", func.get_loading_function_statement())

    def test_HBaseLoadFunction_not_load_key(self):
        func = HBaseLoadFunction(['f1', 'f2', 'f3'], False)
        self.assertEqual("org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3', '-loadKey false')", func.get_loading_function_statement())


    def test_add_load_statement(self):
        pig_builder = PigScriptBuilder()
        pig_schema = 'key:chararray,f1:chararray,f2:chararray,f3:chararray'
        pig_builder.add_load_statement("original_data", HBaseSource('test_table'), HBaseLoadFunction(['f1', 'f2', 'f3'], True), pig_schema)
        self.assertEqual("original_data = LOAD 'hbase://test_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3', '-loadKey true') as (key:chararray,f1:chararray,f2:chararray,f3:chararray);", pig_builder.get_statements())

    def test_add_store_statement(self):
        pig_builder = PigScriptBuilder()
        pig_builder.add_store_statement("final_relation", HBaseSource('output_table'), HBaseStoreFunction(['f1', 'f2', 'f3']))
        self.assertEqual("store final_relation into 'hbase://output_table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('etl-cf:f1 etl-cf:f2 etl-cf:f3');", pig_builder.get_statements())

if __name__ == '__main__':
    unittest.main()
