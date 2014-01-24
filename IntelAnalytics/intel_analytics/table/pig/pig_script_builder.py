from intel_analytics import config
from intel_analytics.table.pig import pig_helpers

MAX_ROW_KEY = 'max_row_key'

class PigScriptBuilder(object):
    def get_append_tables_statement(self, etl_schema, target_table, source_tables):
        statements = []
        properties = etl_schema.get_table_properties(target_table)
        i = 0
        for source in source_tables:
            etl_schema.load_schema(source)
            pig_schema_info = pig_helpers.get_pig_schema_string(etl_schema.get_feature_names_as_CSV(), etl_schema.get_feature_types_as_CSV())
            hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(etl_schema.get_feature_names_as_CSV(), etl_schema.get_feature_types_as_CSV())
            relation = 'relation_%s' %(str(i))
            i = i + 1
            statements.append("%s = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') as (%s);" %(relation + '_in', source, hbase_constructor_args, pig_schema_info))
            statements.append('temp = rank %s;' %(relation + '_in'))
            statements.append('%s = foreach temp generate $0 + %s as key, %s;' %(relation + '_out', properties[MAX_ROW_KEY], etl_schema.get_feature_names_as_CSV()))
            statements.append("STORE %s INTO '%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" %(relation + '_out', target_table, hbase_constructor_args))
        return "\n".join(statements)