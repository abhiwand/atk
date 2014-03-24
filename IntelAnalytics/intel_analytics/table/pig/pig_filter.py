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

def outputSchema(schema_str):
    def wrap(f):
        def wrapped_f(*args):
            return f(*args)
        return wrapped_f
    return wrap

@outputSchema('pattern_matched:int')
def regex_search(val, pattern):
    import re
    prog = re.compile(pattern)
    return 1 if prog.search(val) else 0

def main(argv):
    parser = ArgumentParser(description='applies feature transformations to features in a big dataset')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table', required=True)
    parser.add_argument('-f', '--filter', dest='filter', help='filter condition to drop rows', required=True)
    parser.add_argument('-n', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-p', '--inplace', dest='inplace', help='drop rows inplace')
    parser.add_argument('-c', '--column', dest='column', help='column to apply regex filter on')
    parser.add_argument('-r', '--isregex', dest='isregex', help='if filter is a regex expression')

    cmd_line_args = parser.parse_args()
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    hbase_store_args = generate_hbase_store_args(features)

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')
    filter_string = cmd_line_args.filter

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script

    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))

    if (cmd_line_args.isregex and cmd_line_args.isregex == 'True'):
	if (cmd_line_args.inplace == 'False'):
            pig_statements.append("filter_data = FILTER hbase_data BY (regex_search((chararray)%s, '%s') == 0);" % (cmd_line_args.column, filter_string))
	else:
            pig_statements.append("filter_data = FILTER hbase_data BY (regex_search((chararray)%s, '%s') == 1);" % (cmd_line_args.column, filter_string))
    else:
	if (cmd_line_args.inplace == 'False'):
            pig_statements.append("filter_data = FILTER hbase_data BY NOT(%s);" % (filter_string))
	else:
            pig_statements.append("filter_data = FILTER hbase_data BY (%s);" % (filter_string))

    if (cmd_line_args.inplace == 'False'):	# Write into new table
        pig_statements.append("store filter_data into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (hbase_store_args))
    else:					# Update the same table
	pig_statements.append("DEFINE HBaseDeleteRowsUDF com.intel.pig.udf.HBaseDeleteRowsUDF('$OUTPUT', '%s');--table to delete rows given the batch size default is 1000" % (config['hbase_delete_row_batchsize']))
	pig_statements.append("result = FOREACH filter_data GENERATE HBaseDeleteRowsUDF(*);")
	pig_statements.append("STORE result INTO '/tmp/dummy_file_wont_be_created' USING com.intel.pig.udf.store.NoOpStore();")

    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind({'OUTPUT':cmd_line_args.output}).runSingle()#without binding anything Pig raises error
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
  try:
      rc = main(sys.argv)
      sys.exit(rc)
  except Exception, e:
      print e
      sys.exit(1)
