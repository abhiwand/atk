
from intel_analytics.config import global_config as config
from builtin_functions import EvalFunctions
from schema import ETLSchema
from hbase_client import ETLHBaseClient

class HBaseTableException(Exception):
    pass

class HBaseTable(Table):
    """
    Table Implementation for HBase
    """
    def __init__(self, table_name):
        """
        (internal constructor)
        Parameters
        ----------
        connection : happybase.Connection
            connection to HBase
        table_name : String
            name of table in Hbase
        """
        self.table_name = table_name
        # TODO : Hard-coded column family name must be removed later and
        #  read from Table
        #self.column_family_name = config['hbase_column_family']
        #self.connection = connection

    def transform(self,
                  column_name,
                  new_column_name,
                  transformation,
                  keep_source_column=False,
                  transformation_args=None):
        transformation_to_apply = EvalFunctions.to_string(transformation)

        #load schema info
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        feature_names_as_str = ",".join(etl_schema.feature_names)
        feature_types_as_str = ",".join(etl_schema.feature_types)

        if column_name and (column_name not in etl_schema.feature_names):
            raise HBaseTableException("Column %s does not exist" % column_name)

        #generate some table name for transformation output
        table_name = self.table_name + "-%s" % (transformation_to_apply)

        with ETLHBaseClient() as hbase_client:
            hbase_client.drop_create_table(table_name,
                                           config['hbase_column_family'])

        script_path = os.path.join(etl_scripts_path,'pig_transform.py')

        args = get_pig_args()

        args += [script_path,
                '-f', column_name, '-i', self.table_name,
                '-o', table_name, '-t', transformation_to_apply,
                '-u', feature_names_as_str, '-r', feature_types_as_str, '-n', new_column_name]

        if transformation_args:  # we have some args that we need to pass to the transformation function
            args += ['-a', str(transformation_args)]

        if keep_source_column:
            args += ['-k']

        print args

        return_code = subprocess.call(args)

        if return_code:
            raise HBaseTableException('Could not apply transformation')

        self.table_name = table_name
        #need to update schema here as it's difficult to pass the updated schema info from jython to python
        if not keep_source_column:
            etl_schema.feature_names.remove(column_name)
        etl_schema.feature_names.append(new_column_name)
        #for now make the new feature bytearray, because all UDF's have different return types
        #and we cannot know their return types
        etl_schema.feature_types.append('bytearray')
        etl_schema.save_schema(self.table_name)

    def head(self, n=10):
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
           table = hbase_client.connection.table(self.table_name)
           header_printed = False
           nrows_read = 0
           for key, data in table.scan():
               columns = data.keys()
               items = data.items()
               if not header_printed:
                   sys.stdout.write("--------------------------------------------------------------------\n")
                   for i, column in enumerate(columns):
                       sys.stdout.write("%s"%(re.sub(CONFIG_PARAMS['etl-column-family'],'',column)))
                       if i != len(columns)-1:
                           sys.stdout.write("\t")
                   sys.stdout.write("\n--------------------------------------------------------------------\n")
                   header_printed = True

               for i,(column,value) in enumerate(items):
                   if value == '' or value==None:
                       sys.stdout.write("NA")
                   else:
                       sys.stdout.write("%s"%(value))
                   if i != len(items)-1:
                       sys.stdout.write("\t")
               sys.stdout.write("\n")
               nrows_read+=1
               if nrows_read >= n:
                   break

    def __drop(self, output_table, column_name=None, how=None, replace_with=None):
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)

        feature_names_as_str = ",".join(etl_schema.feature_names)
        feature_types_as_str = ",".join(etl_schema.feature_types)

        if column_name and (column_name not in etl_schema.feature_names):
            raise HBaseTableException("Column %s does not exist" % (column_name))

        script_path = os.path.join(etl_scripts_path,'pig_clean.py')

        args = get_pig_args()

        args += [script_path, '-i', self.table_name,
                         '-o', output_table, '-n', feature_names_as_str,
                         '-t', feature_types_as_str]

        if replace_with:
            args += [ '-r' , replace_with]

        if column_name:
            args += ['-f', column_name]
        else:
            if not how:
                raise HBaseTableException('Please specify a cleaning strategy with the how argument')
            args += ['-s', how]

        # need to delete/create output table so that we can write the transformed features
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            hbase_client.drop_create_table(output_table , [CONFIG_PARAMS['etl-column-family']])#create output table

        print args
        return_code = subprocess.call(args)

        if return_code:
            raise HBaseTableException('Could not clean the dataset')

        self.table_name = output_table # update the table_name
        etl_schema.save_schema(self.table_name) # save the schema for the new table

    def dropna(self, how='any', column_name=None):
        output_table = self.table_name + "_dropna"
        self.__drop(output_table, column_name=column_name, how=how, replace_with=None)

    def fillna(self, column_name, value):
        output_table = self.table_name + "_fillna"
        self.__drop(output_table, column_name=column_name, how=None, replace_with=value)

    def impute(self, column_name, how):
        output_table = self.table_name + "_impute"
        if how not in available_imputations:
            raise HBaseTableException('Please specify a support imputation method. %d is not supported' % (how))
        self.__drop(output_table, column_name=column_name, how=None, replace_with=Imputation.to_string(how))

    def get_schema(self):
        """
        Returns the list of column names/types
        """
        columns=[]
        etl_schema = ETLSchema()
        etl_schema.load_schema(self.table_name)
        for i, column_name in enumerate(etl_schema.feature_names):
            columns.append('%s:%s' % (column_name, etl_schema.feature_types[i]))
        return columns