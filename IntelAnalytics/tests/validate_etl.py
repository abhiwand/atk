"""
Validates all the ETL functionality.
"""

import os
import sys
import subprocess
import commands
import math
import traceback

base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
from intel_analytics.table.hbase.hbase_client import ETLHBaseClient
from intel_analytics.config import global_config as CONFIG_PARAMS

#to get rid of jython logging
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"

#to run pig as local mode
os.environ["INTEL_ANALYTICS_ETL_RUN_LOCAL"] = "true"

test_scripts_path = os.path.join(base_script_path)

print '###########################'
print 'Validating ETL scripts'
print '###########################'

try:
    # test transform functionality
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_transform_API.py')])
     
    if return_code:
        raise Exception("Transform tests failed")
     
    # test transform functionality
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_cleaning_API.py')])
     
    if return_code:
        raise Exception("Clean tests failed")    
    
    #test string functions
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_string_functions.py')])
     
    if return_code:
        raise Exception("String tests failed")    
     
    #test schema functions
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_schema_support.py')])
     
    if return_code:
        raise Exception("Schema tests failed")    
    
    #test big data frame API
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_bigdataframe.py')])
    
    if return_code:
        raise Exception("BigDataFrame API tests failed")    
except:
    traceback.print_exc()
    sys.exit(1)


print '#################################'
print 'Done validating ETL scripts'
print '#################################'