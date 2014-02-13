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

#Coverage.py will attempt to import every python module to generate coverage statistics.
#Since Pig is only available to Jython than this will cause the coverage tool to throw errors thus breaking the build.
#This try/except block will allow us to run coverage on the Jython files.
try:
    from org.apache.pig.scripting import Pig
except:
    print("Pig is either not installed or not executing through Jython. Pig is required for this module.")

from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.table.pig import pig_helpers
from intel_analytics.table.builtin_functions import available_builtin_functions
from intel_analytics.table.builtin_functions import EvalFunctions


def generate_hbase_store_args(features, cmd_line_args):
    hbase_store_args = config['hbase_column_family'] + "AggregateGroup "
    arg_list = cmd_line_args.aggregation_function_list.split(" ")
    for i in range(0,len(arg_list), 3):
    	hbase_store_args += config['hbase_column_family']  + "%s " % (arg_list[i+2])

    return hbase_store_args

def generate_aggregation_statement(cmd_line_args):
    aggregation_statement = 'GENERATE group, group as AggregateGroup'
    arg_list = cmd_line_args.aggregation_function_list.split(" ")
    for i in range(0,len(arg_list), 3):
	if (arg_list[i] == EvalFunctions.to_string(EvalFunctions.Aggregation.DISTINCT) or
	    arg_list[i] == EvalFunctions.to_string(EvalFunctions.Aggregation.COUNT_DISTINCT)):
	    distinct_var = "DISTINCT_" + arg_list[i+1]
	    sub_statement = "%s = DISTINCT hbase_data.%s;" % (distinct_var, arg_list[i+1])
	    if (arg_list[i] == EvalFunctions.to_string(EvalFunctions.Aggregation.DISTINCT)):
	        aggregation_statement = sub_statement + aggregation_statement + ", %s as %s" % (distinct_var, arg_list[i+2])
	    else:
	        aggregation_statement = sub_statement + aggregation_statement + ", COUNT(%s) as %s" % (distinct_var, arg_list[i+2])
	else:
	    if (arg_list[i+1] == "*"):
	        aggregation_statement += ", %s(hbase_data) as %s" % (arg_list[i], arg_list[i+2])
	    else:
	        if (arg_list[i] == EvalFunctions.to_string(EvalFunctions.Aggregation.STDEV)):
	            aggregation_statement += ", SQRT(VAR((hbase_data.%s))) as %s" % (arg_list[i+1], arg_list[i+2])
		else:
	            aggregation_statement += ", %s(hbase_data.%s) as %s" % (arg_list[i], arg_list[i+1], arg_list[i+2])

    return aggregation_statement
    
def main(argv):
    parser = ArgumentParser(description='applies groupby and aggregation to features in a big dataset')
    parser.add_argument('-i', '--input', dest='input', help='the input HBase table', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output HBase table', required=True)
    parser.add_argument('-a', '--aggregation', dest='aggregation_function_list', help='Aggregation function arguments as a list', required=True)
    parser.add_argument('-u', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-r', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')    
    parser.add_argument('-g', '--groupby', dest='group_by_columns', help='Group by Columns')
    parser.add_argument('-l', '--range', dest='range', help='specify the range for group by column')

    cmd_line_args = parser.parse_args()
    
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_store_args = generate_hbase_store_args(features, cmd_line_args)

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')

    pig_statements = []
    pig_statements.append("REGISTER %s;" % (config['feat_eng_jar']))
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar; -- POW is in piggybank.jar" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script

    datafu_jar = os.path.join(config['pig_lib'], 'datafu-0.0.10.jar')
    pig_statements.append("REGISTER %s; -- for the VAR UDF" % datafu_jar)
    pig_statements.append("DEFINE VAR datafu.pig.stats.VAR();")
    pig_statements.append("SET DEFAULT_PARALLEL 20;")
        
    pig_statements.append("hbase_data = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))
    
    aggregation_statement = generate_aggregation_statement(cmd_line_args)

    pig_statements.append("grp = GROUP hbase_data BY (%s);" % (cmd_line_args.group_by_columns))
    pig_statements.append("aggregate_relation = FOREACH grp {%s;};" % (aggregation_statement))

    pig_statements.append("store aggregate_relation into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (hbase_store_args))

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
