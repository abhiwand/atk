"""
Validates all the ETL functionality.
"""

import os
import sys
import subprocess
import commands
import math
base_script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_script_path, '..'))
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.config import CONFIG_PARAMS

print "Using", CONFIG_PARAMS
print 'Starting ...'

print 'Building feature engineering jar'
commands.getoutput("mvn clean package -DskipTests")
assert True == os.path.exists('target/Intel-FeatureEngineering-0.0.1-SNAPSHOT.jar')
print 'Built jar'

#to get rid of jython logging
os.environ["PIG_OPTS"] = "-Dpython.verbose=error"

#to run pig as local mode
os.environ["INTEL_ANALYTICS_ETL_RUN_LOCAL"] = "true"

test_scripts_path = os.path.join(base_script_path)

print '###########################'
print 'Validating ETL scripts'
print '###########################'

# test transform functionality
subprocess.call(['python', os.path.join(test_scripts_path, 'test_transform_API.py')])

#test cleaning functionality
subprocess.call(['python', os.path.join(test_scripts_path, 'test_cleaning_API.py')])
 
#test string functions
subprocess.call(['python', os.path.join(test_scripts_path, 'test_string_functions.py')])

#test math functions
subprocess.call(['python', os.path.join(test_scripts_path, 'test_math_functions.py')])
 
print '#################################'
print 'Done validating ETL scripts'
print '#################################'