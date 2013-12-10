##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
from intel_analytics.config import global_config as config
import happybase

class ETLHBaseClient:
    """
    Usage will be similar to:
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
    
    def delete_table(self, table_name):
        if self.table_exists(table_name):
            self.connection.delete_table(table_name, disable=True)

    def drop_create_table(self, table_name, column_family_descriptors):
        """
        First, drops the table with the given name and then creates a new table with the given name and column families.
        
		+01
        column_family_descriptors: list of column descriptors
        """
        self.delete_table(table_name)

        cf_descriptors = {}
        for cfd in column_family_descriptors:
            cf_descriptors[cfd]=None#no options for the column families, can add later
        self.connection.create_table(table_name, cf_descriptors)
        
    def get_column_names(self, table_name, column_family_descriptors):
        """
        Returns the name of the columns of a table, assuming that all the rows have the same number of columns.
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
