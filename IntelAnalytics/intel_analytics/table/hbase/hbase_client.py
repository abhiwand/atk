from intel_analytics.config import global_config as config
import happybase

class ETLHBaseClient:
    """
    Usage will be like:
    with ETLHBaseClient(hbase_host) as hbase_client:
        # use hbase_client
    """

    def __init__(self, hbase_host=config['hbase_host']):
        self.hbase_host = hbase_host

    def table_exists(self, table_name):
        return table_name in self.connection.tables()

    def put(self, table_name, row_key, data_dict):
        table = self.connection.table(table_name)
        table.put(row_key, data_dict)
        
    def get(self, table_name, row_key):
        table = self.connection.table(table_name)
        return table.row(row_key)
    
    def delete(self, table_name, row_key):
        table = self.connection.table(table_name)
        table.delete(row_key)        
    
    def drop_create_table(self, table_name, column_family_descriptors):
        """
        first drops the table with the given name and then creates a new table with the given name and column families
        table_name: table name to drop & create
        column_family_descriptors: list of column descriptors
        """
        cf_descriptors = {}
        for cfd in column_family_descriptors:
            cf_descriptors[cfd]=None#no options for the column families, can add later
        
        try:        
            self.connection.delete_table(table_name, disable=True)
        except:#output table may not exist so exception may be thrown
            pass
        
        self.connection.create_table(table_name, cf_descriptors)
        
    def get_column_names(self, table_name, column_family_descriptors):
        """
        return the name of the columns of a table assuming that all the rows have the same number of columns
        """
        table = self.connection.table(table_name)
        for key, data in table.scan():
            return data.keys()
        return None    
  
    def __enter__(self):
        self.connection = happybase.Connection(self.hbase_host)        
        return self
      
    def __exit__(self, type, value, traceback):
        self.connection.close()
