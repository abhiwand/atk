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
    cf = config['hbase_column_family']
    hbase_store_args =  " ".join([cf+f for f in features])
    if cmd_line_args.randomize == "True":
        hbase_store_args += ' ' + (cf+cmd_line_args.input_column)
    hbase_store_args += ' ' + (cf+cmd_line_args.output_column)
    return hbase_store_args

def generate_split_statement(feature_list, cmd_line_args):
    if cmd_line_args.randomize == "True":
         feature_list += ', ' + cmd_line_args.input_column
    low = cmd_line_args.test_fold_id
    high = int(cmd_line_args.test_fold_id) + 1
    new =[
        ', ((',
        low,
        "<=",
        cmd_line_args.input_column,
        'and',
        cmd_line_args.input_column,
        "<",
        high,
        ")? '" + cmd_line_args.split_name[0] + "'",
        " : ",
        "'" + cmd_line_args.split_name[1] + "') "
    ]
    split_statement = feature_list + ' '.join(map(str, new))
    return split_statement

def main(argv):

    parser = ArgumentParser(description='split table into different buckets')
    parser.add_argument('-it', '--table', dest='input_table', help='the input HBase table', required=True)
    parser.add_argument('-ot', '--output', dest='output_table', help='the output HBase table', required=True)
    parser.add_argument('-k', '--num_fold', dest='num_fold', help='how many folds to use', required=True)
    parser.add_argument('-ic', '--input_column', dest='input_column', help='the input HBase column', required=True)
    parser.add_argument('-f', '--test_fold_id', dest='test_fold_id', help='The fold ID for test', required=True)
    parser.add_argument('-r', '--randomize', dest='randomize', help='to randomize record', required=True)
    parser.add_argument('-n', '--split_name', dest='split_name', help='the name for each split', required=True)
    parser.add_argument('-oc', '--output_column', dest='output_column', help='the result HBase column', required=True)
    parser.add_argument('-fn', '--feature_names', dest='feature_names', help='the names of features', required=True)
    parser.add_argument('-ft', '--feature_types', dest='feature_types', help='the types of features', required=True)

    cmd_line_args = parser.parse_args()
    #convert the string representation of split_name to a list
    if cmd_line_args.split_name:
        cmd_line_args.split_name = ast.literal_eval(cmd_line_args.split_name)
    features = [f.strip() for f in cmd_line_args.feature_names.split(',')]
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)
    hbase_store_args = generate_hbase_store_args(features, cmd_line_args)
    features.insert(0, 'key')
    feature_list = ", ".join(features)
    split_statement = generate_split_statement(feature_list, cmd_line_args)

    pig_load = [
                "REGISTER " + config['feat_eng_jar'] + ";",
                "SET default_parallel " + config['pig_parallelism_factor'] + ";",
                "hbase_data = LOAD 'hbase://" + cmd_line_args.input_table +
                "' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('" +
                hbase_constructor_args + "', '-loadKey true') as (key:chararray, " +
                pig_schema_info + ");",
                ]
    if cmd_line_args.randomize == "True":
        pig_compute = [
                        "randomization = FOREACH hbase_data GENERATE " + feature_list +
                        ", FLOOR( 1 + (" + cmd_line_args.num_fold + " - 1)*RANDOM()) AS " +
                        cmd_line_args.input_column + ";",
                        "split_result = FOREACH randomization GENERATE " + split_statement +
                        " AS " + cmd_line_args.output_column + ";",
                        "store split_result into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('" +
                        hbase_store_args + "');"
                      ]
    else:
        pig_compute = [
                        "split_result = FOREACH hbase_data GENERATE " + split_statement +
                        " AS " + cmd_line_args.output_column + ";",
                        "store split_result into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('" +
                        hbase_store_args + "');"
                     ]
    pig_script = "\n".join(pig_load) + "\n" + "\n".join(pig_compute)
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
