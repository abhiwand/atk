##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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


def generate_hbase_store_args(features, cmd_line_args):
    hbase_store_args = ''#for storing, we need to include output_column name when storing
    cf = config['hbase_column_family']
    for i, f in enumerate(features):
        hbase_store_args += '%s ' % ((cf+f))

    hbase_store_args += (cf+cmd_line_args.output_column)
    return hbase_store_args

def generate_split_statement(features, cmd_line_args):
    split_statement = ' '
    for i, f in enumerate(features):
        split_statement += f
        split_statement += ', '

    if cmd_line_args.test_fold_id != '0':
        # to split data for k-fold validation
        low = cmd_line_args.test_fold_id
        high = str(int(cmd_line_args.test_fold_id) + 1)
        split_statement += "(( %s <= %s and %s < %s )? '%s' : '%s') " % (low, cmd_line_args.input_column, cmd_line_args.input_column, high, cmd_line_args.split_name[0], cmd_line_args.split_name[1])
    else:
        #first split
        high = str(cmd_line_args.split_percent[0])
        split_statement += " (CASE WHEN  (%s < %s)  THEN '%s' " % (cmd_line_args.input_column, high, cmd_line_args.split_name[0])
        #middle part which is neither first nor last split
        for i in range(1,len(cmd_line_args.split_percent)-1):
            low = str(cmd_line_args.split_percent[i-1])
            high = str(cmd_line_args.split_percent[i-1] + cmd_line_args.split_percent[i])
            split_statement += " WHEN  (%s <= %s and %s < %s)  THEN '%s' " % (str(low), cmd_line_args.input_column, cmd_line_args.input_column, high, cmd_line_args.split_name[i])

        #last split
        split_statement += " ELSE '%s' " % (cmd_line_args.split_name[-1])
        split_statement += ' END) '
    print split_statement
    return split_statement
    
def main(argv):

    parser = ArgumentParser(description='split table into different buckets')
    parser.add_argument('-t', '--table', dest='input_table', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output_table', help='the output HBase table', required=True)
    parser.add_argument('-i', '--input', dest='input_column', help='the input HBase column', required=True)
    parser.add_argument('-f', '--test_fold_id', dest='test_fold_id', help='The fold ID for test', required=True)
    parser.add_argument('-p', '--split_percent', dest='split_percent', help='the percentage distribution of each split', required=True)
    parser.add_argument('-n', '--split_name', dest='split_name', help='the name for each split', required=True)
    parser.add_argument('-r', '--result', dest='output_column', help='the result HBase column', required=True)
    parser.add_argument('-fn', '--feature_names', dest='feature_names', help='the names of features', required=True)
    parser.add_argument('-ft', '--feature_types', dest='feature_types', help='the types of features', required=True)

    cmd_line_args = parser.parse_args()
    #convert the string representation of split_percent to a list
    if cmd_line_args.split_percent:
        cmd_line_args.split_percent = ast.literal_eval(cmd_line_args.split_percent)

    if cmd_line_args.split_name:
        cmd_line_args.split_name = ast.literal_eval(cmd_line_args.split_name)
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    hbase_store_args = generate_hbase_store_args(features, cmd_line_args)

    features.insert(0, 'key')
    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("SET default_parallel %s;" % (config['pig_parallelism_factor']))
    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input_table, hbase_constructor_args, pig_schema_info))
    split_statement = generate_split_statement(features, cmd_line_args)
    pig_statements.append("split_result = FOREACH hbase_data GENERATE %s AS %s;" % (split_statement, cmd_line_args.output_column))
    pig_statements.append("store split_result into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % hbase_store_args)
    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind({'OUTPUT':cmd_line_args.output_table}).runSingle()#without binding anything Pig raises error
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
  try:
      rc = main(sys.argv)
      sys.exit(rc)
  except Exception, e:
      print e
      sys.exit(1)
