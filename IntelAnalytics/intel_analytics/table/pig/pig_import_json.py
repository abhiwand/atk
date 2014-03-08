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
import sys

#Coverage.py will attempt to import every python module to generate coverage statistics.
#Since Pig is only available to Jython than this will cause the coverage tool to throw errors thus breaking the build.
#This try/except block will allow us to run coverage on the Jython files.
from intel_analytics.table.pig.pig_helpers import get_load_statement_list, get_generate_key_statements, report_job_status

try:
    from org.apache.pig.scripting import Pig
except:
    print("Pig is either not installed or not executing through Jython. Pig is required for this module.")
from intel_analytics.config import global_config as config
from intel_analytics.table.pig.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7

def main(argv):
    """
    Currently, we don't get any schema info from the user. We just load
    the whole JSON as a possibly nested map and dump it as is to HBase.
    """
    parser = ArgumentParser(description='imports a big CSV dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-m', '--offset', dest='offset', help='current maximum row key. Will be the offset value for assigning new row keys')

    cmd_line_args = parser.parse_args()

    raw_load_statement = "LOAD '%s' USING TextLoader() as (json: chararray);"
    input_path = cmd_line_args.input
    files = input_path.split(',')
    load_statements = get_load_statement_list(files, raw_load_statement, 'json_data')

    pig_statements = []
    final_relation = 'with_unique_row_keys'
    pig_statements.extend(load_statements)

    if not cmd_line_args.offset:
        cmd_line_args.offset = 0

    key_assignment_statements = get_generate_key_statements('json_data', final_relation, 'json', long(cmd_line_args.offset))
    pig_statements.extend(key_assignment_statements)
    pig_statements.append("STORE %s INTO 'hbase://$OUTPUT' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (final_relation, config['hbase_column_family']+'json'));
    pig_script = "\n".join(pig_statements)
    compiled = Pig.compile(pig_script)
    status = compiled.bind({'OUTPUT':cmd_line_args.output}).runSingle()#without binding anything Pig raises error
    report_job_status(status)
    return 0 if status.isSuccessful() else 1

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)

