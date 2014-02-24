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

    """
    helper
    """
    def dump_statement(statements):
        for s in statements:
            sys.stderr.write(s + '\n')

    def get_join_statement(self, tables, on=[], how='inner', suffixes=[], sort=False, join_frame_name=''):
        """
        TODO:
        1. filtering
        2. sorting (RANK, ORDER BY)
        3. parallel,parititioning
        4. performance (USING skewed, etc.)
        """
    
        def _get_join_type(how='inner'):
            how = how.upper()
       
        def _get_pig_schema(features, types):
            return ','.join([x + ":" + y for x,y in zip(features, types)])

        def _get_hbase_schema(features):
            return ' '.join([config['hbase_column_family'] + x for x in features])

        # supported types
        if not how.lower() in ['inner', 'outer', 'left', 'right']:
            raise Exception('No corresponding pig join type for the requested join as ' + how)

        if not on:
            raise Exception('Empty list of columns selected to join on.')

        if len(on) != len(tables):
            raise Exception('Invalid input list of columns to join on.')

        if len(suffixes) != len(tables):
            raise Exception('Input input list of suffixes.')

        join_type = _get_join_type(how)
        if (how == 'LEFT') or (how == 'RIGHT'):
            join_type = how
        elif how == 'OUTER':
            join_type = 'FULL'
        else:
            join_type = ''
            # Outer join in pig can only do two tables a time
            if len(tables) < 2:
                raise Exception('Outer join only works on two tables')


        if not join_table_name:
            raise Exception('Must specify an output table name for join operation!')
        
        # inner join supports multiple tables
        # FIXME: do we have a max??
        statements = []
        joins = []
        loads = []
        pig_schemas = []
        hbase_schemas = []

        for table in tables:
            etl_schema = ETLSchema()
            etl_schema.load_schema(table)
            hbase_schema = _get_hbase_schema(etl_schema.feature_name)
            pig_schema = _get_pig_schema(etl_schema.feature_names, etl_schema.feature_types)
            
            i = tables.index(table)

            # LOAD
            alias = 'L%d' % i
            loads.append("%s = LOAD 'hbase://%s' USING " \
                         "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') " \
                         "AS (%s);" % (alias, table, hbase_schema, pig_schema))


            # JOIN,  i.e. '{alias BY (col,...) [LEFT|RIGHT|FULL]}'
            join = alias + ' BY ' + on[i]
            join = 'J = JOIN ' + join + ' ' + join_type if i == 0 else join
            joins.append(join)

            # prepare the schema for hbase in STORE later
            suffix = suffixes[i]
            suffixed_features = [x + suffix for x in etl_schema.feature_names]
            hbase_schemas.append(_get_hbase_schema(suffixed_features))
            pig_schemas.append(_get_pig_schema(suffixed_features, etl_schema.feature_types))

        # build the statements
        statements.append('-- Loading input tables')
        statements.append(loads)
        statements.append('-- Joining input tables')
        statements.append(', '.join(joins) + ';')

        # generate row key using rank
        statements.append('-- Use rank to generate the row key')
        statements.append('R = RANK J;')
       
        # TODO: other filtering 
        statements.append('final = R;')

        # STORE
        statements.append('-- Storing to output table')
        statements.append("STORE final INTO 'hbase://%s' USING " \
                          "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" \
                          %(join_table_name, ' '.join(hbase_schemas)))
       
        # return the pig schema to be saved 
        join_pig_schema = ', '.join(pig_schemas)
        join_script = "\n".join(statements)

        # TODO: remove the debug
        dump_statement(statements)

        return join_script, join_pig_schema
