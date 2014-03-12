import abc
import string
from intel_analytics.table.pig import pig_helpers
from intel_analytics.config import global_config as config
from intel_analytics.table.pig.pig_helpers import get_hbase_storage_schema

MAX_ROW_KEY = 'max_row_key'

class DataSource(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_data_source_as_string(self):
        pass

class HBaseSource(DataSource):

    def __init__(self, table):
        self._table = table

    def get_data_source_as_string(self):
        return 'hbase://%s' %(self._table)


class LoadFunction(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_loading_function_statement(self):
        pass

class HBaseLoadFunction(LoadFunction):
    def __init__(self, feature_name_iterable, load_key):
        self._feature_name_iterable = feature_name_iterable
        self._load_key = load_key

    def get_loading_function_statement(self):
        loading_hbase_constructor_args = get_hbase_storage_schema(self._feature_name_iterable)
        return "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey %s')" %(loading_hbase_constructor_args, string.lower(str(self._load_key)))


class StoreFunction(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_storing_function_statement(self):
        pass

class HBaseStoreFunction(StoreFunction):
    def __init__(self, feature_name_iterable):
        self._feature_name_iterable = feature_name_iterable

    def get_storing_function_statement(self):
        loading_hbase_constructor_args = get_hbase_storage_schema(self._feature_name_iterable)
        return "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s')" %(loading_hbase_constructor_args)



class PigScriptBuilder(object):

    def __init__(self):
        self.statements = []

    def add_load_statement(self, output_relation_name, data_source, load_function, pig_schema):
        """
        Add a load statement
        Parameters
        ----------
        output_relation_name : String
            name the output relation for the load operation
        data_source : DataSource
            DataSource object
        load_function : LoadFunction
            LoadFunction instance
        pig_schema : String
            pig schema for the data loading. Loader produces types specified by the schema

        """
        self.statements.append("%s = LOAD '%s' USING %s as (%s);" %(output_relation_name, data_source.get_data_source_as_string(), load_function.get_loading_function_statement(), pig_schema))

    def add_store_statement(self, relation_to_store, data_source, store_function):
        """
        Add a store statement
        Parameters
        ----------
        relation_to_store : String
            name the relation to store
        data_source : DataSource
            DataSource object
        store_function : StoreFunction
            LoadFunction instance
        """
        self.statements.append("store %s into '%s' using %s;" %(relation_to_store, data_source.get_data_source_as_string(), store_function.get_storing_function_statement()))

    def get_statements(self):
        return "\n".join(self.statements)


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

    def get_join_statement(self, etl_schema, tables, on, how='inner', suffixes=None, join_table_name=''):
        """
        Parameters
        ----------
        etl_schema: ETLSchema
            The ETL Schema object used as a container to retrive table schema
        tables: List
            List of HBase table names
        on: List
            List of columns to be joined on
        how: String
            {'inner', 'outer', 'left', 'right'}
        suffixes: List
            List of suffixes
        join_table_name: String
            Output table name
        """


        if not etl_schema:
            raise Exception('Must have a valid reference to an ETLSchema object!')

        # supported types
        if (not tables) or (len(tables) < 2):
            raise Exception('Invalid input table list.')

        if not how.lower() in ['inner', 'outer', 'left', 'right']:
            raise Exception("The requested join type '%s' is not supported." % how)

        if (not on) or (len(on) != len(tables)):
            raise Exception('Invalid columns to be joined on.')

        if not suffixes:
            suffixes = ['_x']
            suffixes.extend(['_y' + str(x) for x in range(1, len(tables))])

        if len(suffixes) != len(tables):
            raise Exception('Input list of suffixes.')

        how = how.upper()
        if (how == 'LEFT') or (how == 'RIGHT'):
            join_type = how
        elif how == 'OUTER':
            join_type = 'FULL'
        else:
            join_type = ''

        # Outer join in pig can only do two tables a time
        if join_type and len(tables) != 2:
           raise Exception('Outer join only works on two tables')

        if not join_table_name:
            raise Exception('Must specify an output table name for join operation!')

        # inner join supports multiple tables
        joins = []
        loads = []
        pig_schemas = []
        hbase_schemas = []

        for i, table in enumerate(tables):
            etl_schema.load_schema(table)
            hbase_schema = pig_helpers.get_hbase_storage_schema(etl_schema.feature_names)
            pig_schema = pig_helpers.get_pig_schema(etl_schema.feature_names, etl_schema.feature_types)

            # LOAD
            alias = 'L%d' % i
            loads.append("%s = LOAD 'hbase://%s' USING " \
                         "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s') " \
                         "AS (%s);" % (alias, table, hbase_schema, pig_schema))


            # JOIN,  i.e. '{alias BY (col,...) [LEFT|RIGHT|FULL]}'
            join_on = on[i]
            if i == 0:
                joins.append("J = JOIN %s BY (%s) %s" % (alias, join_on, join_type))
            else:
                joins.append("%s BY (%s)" % (alias, join_on))

            # prepare the schema for hbase in STORE later
            suffix = suffixes[i]
            suffixed_features = [x + suffix for x in etl_schema.feature_names]
            hbase_schemas.append(pig_helpers.get_hbase_storage_schema(suffixed_features))
            pig_schemas.append(pig_helpers.get_pig_schema(suffixed_features, etl_schema.feature_types))

        # build the statements
        statements = []
        statements.append('-- Loading input tables')
        statements.extend(loads)
        statements.append('-- Joining input tables')
        statements.append(', '.join(joins) + ';')

        # generate row key using rank
        statements.append('-- Use rank to generate the row key')
        statements.append('R = RANK J;')
        statements.append('final = R;')

        # STORE
        statements.append('-- Storing to output table')
        statements.append("STORE final INTO 'hbase://%s' USING " \
                          "org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" \
                          %(join_table_name, ' '.join(hbase_schemas)))

        # return the pig schema to be saved
        join_pig_schema = ', '.join(pig_schemas)
        join_script = "\n".join(statements)

        return join_script, join_pig_schema
