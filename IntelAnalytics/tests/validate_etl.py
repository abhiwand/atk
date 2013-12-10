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
    
    #test big data frame API
    return_code = subprocess.call(['python', os.path.join(test_scripts_path, 'test_bigdataframe.py')])
    
    if return_code:
        raise Exception("BigDataFrame API tests failed") 
    
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
   
except:
    traceback.print_exc()
    sys.exit(1)


print '#################################'
print 'Done validating ETL scripts'
print '#################################'