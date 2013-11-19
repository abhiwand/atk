import unittest
import os
import random
import sys
import string
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.schema import ETLSchema

class ETLSchemaTest(unittest.TestCase):
    def test(self):
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
        with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
            try:
                table = hbase_client.connection.table(CONFIG_PARAMS['etl-schema-table'])
                table.delete(test_table)#also remove the schema info
            except:
                pass

if __name__ == '__main__':
    unittest.main()        