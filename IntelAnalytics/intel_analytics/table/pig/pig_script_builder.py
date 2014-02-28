from intel_analytics import config
from intel_analytics.table.pig import pig_helpers

MAX_ROW_KEY = 'max_row_key'

class PigScriptBuilder(object):
    def get_append_tables_statement(self, etl_schema, target_table, source_tables):
        statements = []
        properties = etl_schema.get_table_properties(target_table)
        i = 0
        in_relations = []
        final_cols = []
        for source in source_tables:
            etl_schema.load_schema(source)
            pig_schema_info = pig_helpers.get_pig_schema_string(etl_schema.get_feature_names_as_CSV(), etl_schema.get_feature_types_as_CSV())
            loading_hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(
                etl_schema.get_feature_names_as_CSV())
            in_relations.append('relation_%s_in' %(str(i)))
            i = i + 1
            # forming the load statements which load data to a relation.
            # The relation is the latest appended to in_relations.
            statements.append("%s = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') as (%s);" %(in_relations[-1], source, loading_hbase_constructor_args, pig_schema_info))

            in_final_list = set(final_cols)
            in_current_table = set(etl_schema.feature_names)
            diff = in_current_table - in_final_list
            final_cols.extend(list(diff))
            etl_schema.feature_names = []
            etl_schema.feature_types = []

        if len(source_tables) == 1:
            statements.append('combined_relation = %s;' %(in_relations[-1]))
        elif len(source_tables) > 1:
            statements.append('combined_relation = UNION ONSCHEMA %s;' %(','.join(in_relations)))


        statements.append('temp = rank combined_relation;')
        statements.append('combined_relation_out = foreach temp generate $0 + %s as key, %s;' %(properties[MAX_ROW_KEY], ','.join(final_cols)))
        storing_hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(','.join(final_cols))
        statements.append("STORE combined_relation_out INTO '%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" %(target_table, storing_hbase_constructor_args))
        return "\n".join(statements)