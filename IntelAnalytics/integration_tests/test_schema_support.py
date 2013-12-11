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
import unittest
import os
import random
import sys
import string
base_script_path = os.path.dirname(os.path.abspath(__file__))
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..//'))
os.environ['PYTHONPATH'] = ':'.join(sys.path)#python scripts that call our pig scripts need this
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.table.hbase.schema import ETLSchema
from intel_analytics.config import global_config as CONFIG_PARAMS


class ETLSchemaTest(unittest.TestCase):
    def test(self):
        print '###########################'
        print 'Validating ETL Schema support'
        print '###########################'
        test_table = ''.join(random.choice(string.lowercase) for i in xrange(5))#generate random table name of length 5
        #should be able to have whitespace in between
        schema_string = 'f1:   chararray  ,   f2:chararray,f3:double,f4:long, f5:    datetime'
        print "Will save schema info %s for test table %s" % (schema_string, test_table)
        etl_schema = ETLSchema()
        etl_schema.populate_schema(schema_string)
        etl_schema.save_schema(test_table)
        print "Saved schema"
        loaded_schema = ETLSchema()
        loaded_schema.load_schema(test_table)
        print "Loaded schema:",loaded_schema.feature_names, loaded_schema.feature_types
        self.assertEqual(len(loaded_schema.feature_names), 5)
        for i, fname in enumerate(loaded_schema.feature_names):
            if fname == 'f1':
              self.assertEqual(loaded_schema.feature_types[i], 'chararray')
            elif fname == 'f2':
              self.assertEqual(loaded_schema.feature_types[i], 'chararray')  
            elif fname == 'f3':
              self.assertEqual(loaded_schema.feature_types[i], 'double')  
            elif fname == 'f4':
              self.assertEqual(loaded_schema.feature_types[i], 'long')
            elif fname == 'f5':
              self.assertEqual(loaded_schema.feature_types[i], 'datetime')              
            else:
                raise Exception("Should't reach here")  
                                                         
        print 'Cleaning up the test table %s & their schema definition' % (test_table)
        with ETLHBaseClient(CONFIG_PARAMS['hbase_host']) as hbase_client:
            try:
                table = hbase_client.connection.table(CONFIG_PARAMS['hbase_schema_table'])
                table.delete(test_table)#also remove the schema info
            except:
                pass
        print '###########################'
        print 'DONE Validating ETL Schema support'
        print '###########################'            

if __name__ == '__main__':
    try:
      rc = unittest.main()  
      sys.exit(rc)
    except Exception, e:
      print e
      sys.exit(1)
              