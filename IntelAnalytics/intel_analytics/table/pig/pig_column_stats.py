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
import os
import ast
import sys

from intel_analytics.table.pig import pig_helpers

try:
    from org.apache.pig.scripting import Pig
except ImportError, e:
    print "Pig is either not installed or not executing through Jython. Pig is required for this module."

from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.table.builtin_functions import available_builtin_functions
from intel_analytics.table.builtin_functions import EvalFunctions


def generate_hbase_store_args(features):
    hbase_store_args = ''#for storing, we need to update the transformed field's name when storing
    cf = config['hbase_column_family']
    for i, f in enumerate(features):
        hbase_store_args += '%s ' % ((cf+f))
    
    return hbase_store_args
    
def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    #parser.add_argument('-f', '--filter', dest='filter', help='filter condition to drop rows', required=True)
    parser.add_argument('-n', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-c', '--column', dest='column', help='column to apply regex filter on')

    cmd_line_args = parser.parse_args()
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    #pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    #hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    pig_schema_info = cmd_line_args.column + ':int'
    hbase_constructor_args = config['hbase_column_family'] + cmd_line_args.column
    hbase_store_args = generate_hbase_store_args(features)
    c = cmd_line_args.column
    

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script

    datafu_jar = os.path.join(config['pig_lib'], 'datafu-0.0.10.jar')
    pig_statements.append("REGISTER %s; -- for the VAR UDF" % datafu_jar)
    pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")
    pig_statements.append("SET DEFAULT_PARALLEL %s;" % (config['pig_parallelism_factor']))
 

    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))


    pig_statements.append("grouped_data1 = GROUP hbase_data ALL;")
    pig_statements.append("max_result = FOREACH grouped_data1 GENERATE CONCAT('max=', (chararray)MAX(hbase_data.%s));" % (c))
    pig_statements.append("min_result = FOREACH grouped_data1 GENERATE CONCAT('min=', (chararray)MIN(hbase_data.%s));" % (c))
    pig_statements.append("avg_result = FOREACH grouped_data1 GENERATE CONCAT('avg=', (chararray)AVG(hbase_data.%s));" % (c))
    pig_statements.append("var_result = FOREACH grouped_data1 GENERATE CONCAT('var=', (chararray)VAR(hbase_data.%s));" % (c))
    pig_statements.append("stdev_result = FOREACH grouped_data1 GENERATE CONCAT('stdev=', (chararray)SQRT(VAR(hbase_data.%s)));" % (c))
    pig_statements.append("unique_val_count = FOREACH grouped_data1 { unique_values = DISTINCT hbase_data.%s; GENERATE CONCAT('unique_values=', (chararray)COUNT(unique_values)); }" % (c))

    pig_statements.append("filter_data1 = FOREACH hbase_data GENERATE (%s is null ? 1 : 0) as null_value:int;" % (c))
    pig_statements.append("grouped_data2 = GROUP filter_data1 ALL;")
    pig_statements.append("missing_val_count = FOREACH grouped_data2 GENERATE CONCAT('missing_values=', (chararray)SUM(filter_data1.null_value));")

    pig_statements.append("result = UNION max_result, min_result, avg_result, var_result, stdev_result, unique_val_count, missing_val_count;")


    pig_statements.append("grouped_data2 = GROUP hbase_data BY %s;" % (c))
    pig_statements.append("histogram = FOREACH grouped_data2 {distinct_values = DISTINCT hbase_data.%s; GENERATE group, COUNT(distinct_values);}" % (c))

    pig_statements.append("rmf $STATS;")
    pig_statements.append("rmf $HISTOGRAM;")

    pig_statements.append("STORE result INTO '$STATS';")
    pig_statements.append("STORE histogram INTO '$HISTOGRAM';")

    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind({'STATS':cmd_line_args.input + '_' + c + '_stats', 'HISTOGRAM':cmd_line_args.input + '_' + c + '_histogram'}).runSingle()#without binding anything Pig raises error
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
  try:
      rc = main(sys.argv)
      sys.exit(rc)
  except Exception, e:
      print e
      sys.exit(1)
