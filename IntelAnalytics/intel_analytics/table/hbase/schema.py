import re

from intel_analytics.config import global_config as config
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient


class ETLSchema:
    def __init__(self):
        self.feature_names=[]
        self.feature_types=[]
      
    def populate_schema(self, schema_string):
        """
        Parses the given schema string of the form:
           'x:chararray,y:double,z:int,l:long'
        """
        splitted = schema_string.split(',')
        for schema_item in splitted:
            feature_name, feature_type = schema_item.split(':')
            feature_name = feature_name.strip()
            feature_type = feature_type.strip()
            self.feature_names.append(feature_name)
            self.feature_types.append(feature_type)
    
    def load_schema(self, table_name):
        """
        Loads schema from HBase for the given table
        """
        schema_table = config['hbase_schema_table']
        with ETLHBaseClient() as hbase_client:
            assert hbase_client.table_exists(schema_table), 'Cannot read the schema from %s!' % (schema_table)
            data_dict = hbase_client.get(schema_table,table_name)
            assert len(data_dict.items())>0, 'No schema information found for table %s!' % (table_name)
            for feature_name,feature_type in data_dict.items():
                self.feature_names.append(re.sub(config['hbase_column_family'],'',feature_name))#remove the col. family identifier
                self.feature_types.append(feature_type)            
#         print "LOADED",self.feature_names,self.feature_types
    
    def save_schema(self, table_name):
        """
        Saves schema to HBase for the given table. If an entry already exists in ETL_SCHEMA for the
        given table, than that entry is overwritten.
        """
        schema_table = config['hbase_schema_table']
        with ETLHBaseClient() as hbase_client:
            #create if etl schema table doesn't exist
            if not hbase_client.table_exists(schema_table):
                hbase_client.drop_create_table(schema_table,
                                               config['hbase_column_family'])
                
            #check if an entry already exists in ETL_SCHEMA
            row = hbase_client.get(schema_table, table_name)
            if len(row) > 0:#there is an entry for this table in ETL_SCHEMA, overwrite it
#                 print "An entry already exists in %s for table %s, overwriting it" % (CONFIG_PARAMS['etl-schema-table'], table_name)
                hbase_client.delete(schema_table, table_name)

            data_dict = {}
            for i,feature_name in enumerate(self.feature_names):
                feature_type = self.feature_types[i]
                data_dict[config['hbase_column_family'] + feature_name] = feature_type
            hbase_client.put(schema_table,table_name,data_dict)

#         print "SAVED",self.feature_names,self.feature_types