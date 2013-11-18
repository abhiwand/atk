import happybase

from intel_analytics.table.hbase import Hbase
from intel_analytics.config import global_config as config


"""
Usage will be like:
with ETLHBaseClient(hbase_host) as hbase_client:
    # use hbase_client
"""
class ETLHBaseClient:
    
    def __init__(self, hbase_host=config['hbase_host']):
        self.hbase_host = hbase_host

    def is_table_readable(self, table_name):
        try:
            return self.connection.is_table_enabled(table_name)
        except:
            return False
    """
    first drops the table with the given name and then creates a new table with the given name and column families
    table_name: table name to drop & create
    column_family_descriptors: list of column descriptors
    """
    def drop_create_table(self, table_name, column_family_descriptors):
        cf_descriptors = {}
        for cfd in column_family_descriptors:
            cf_descriptors[cfd]=None#no options for the column families, can add later
        
        try:        
            self.connection.delete_table(table_name, disable=True)
        except:#output table may not exist so exception may be thrown
            pass
        
        self.connection.create_table(table_name, cf_descriptors)
        
    """
    return the name of the columns of a table assuming that all the rows have the same number of columns
    """
    def get_column_names(self, table_name, column_family_descriptors):
        table = self.connection.table(table_name)
        scanner = table.scan()
        single_row = scanner.next()
        data_dictionary = single_row[1]
        return data_dictionary.keys()
  
    def __enter__(self):
        self.connection = happybase.Connection(self.hbase_host)        
        return self
      
    def __exit__(self, type, value, traceback):
        self.connection.close()
