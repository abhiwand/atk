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
import re

from intel_analytics.config import global_config as config
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.logger import stdout_logger as logger

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
        Loads schema from HBase for the given table.
        """
        schema_table = config['hbase_schema_table']
        with ETLHBaseClient() as hbase_client:
            assert hbase_client.table_exists(schema_table), 'Cannot read the schema from %s!' % (schema_table)
            data_dict = hbase_client.get(schema_table,table_name)
            assert len(data_dict.items())>0, 'No schema information found for table %s!' % (table_name)
            for feature_name,feature_type in data_dict.items():
                self.feature_names.append(re.sub(config['hbase_column_family'],'',feature_name))#remove the col. family identifier
                self.feature_types.append(feature_type)            
    
    def save_schema(self, table_name):
        """
        Saves schema to HBase for the given table. If an entry already exists in ETL_SCHEMA for the
        given table, than that entry is overwritten.
        """
        schema_table = config['hbase_schema_table']
        with ETLHBaseClient() as hbase_client:
            #create if etl schema table doesn't exist
            logger.debug('creating etl schema table ' + schema_table + " with column family: " + config['hbase_column_family']) 
            if not hbase_client.table_exists(schema_table):
                hbase_client.drop_create_table(schema_table,
                                               [config['hbase_column_family']])
            #check if an entry already exists in ETL_SCHEMA
            row = hbase_client.get(schema_table, table_name)
            if len(row) > 0:#there is an entry for this table in ETL_SCHEMA, overwrite it
                hbase_client.delete(schema_table, table_name)
                
            data_dict = {}
            for i,feature_name in enumerate(self.feature_names):
                feature_type = self.feature_types[i]
                data_dict[config['hbase_column_family'] + feature_name] = feature_type
            hbase_client.put(schema_table,table_name,data_dict)
            
    def get_feature_names_as_CSV(self):
        return ",".join(self.feature_names)
    
    def get_feature_types_as_CSV(self):
        return ",".join(self.feature_types)    
