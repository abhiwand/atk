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
try:
    from org.apache.pig.scripting import Pig
except:
    print("Pig is either not installed or not executing through Jython. Pig is required for this module.")
from intel_analytics.table.pig import pig_helpers
from intel_analytics.table.pig.argparse_lib import ArgumentParser


def main(argv):
    parser = ArgumentParser(description='imports a big dataset from HDFS to HBase')
    parser.add_argument('-l', '--idelimeter', dest='input_delimeter_char', help='delimeter to use while parsing the input file', required=True)
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-c', '--custom_parser', dest='custom_parser', help='custom parser to use while parsing the raw input', required=True)
    parser.add_argument('-f', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')
    

    cmd_line_args = parser.parse_args()

    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names)

    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    
# create the field extraction statement from the features
    field_extractor = ''
    for i, fname in enumerate(features):
      field_extractor += '$0.%s' % (fname)
      if i != len(features) - 1:
          field_extractor += ', '
                    
    pig_statements = []
    pig_statements.append("REGISTER %s USING jython as custom_udf; -- register parser" % (cmd_line_args.custom_parser))
    pig_statements.append("logs = LOAD '%s' using PigStorage('%s') AS (line: chararray);" % (cmd_line_args.input, cmd_line_args.input_delimeter_char))
    pig_statements.append("parsed = FOREACH logs GENERATE custom_udf.parseRecord(*);")
    pig_statements.append("parsed_val = FOREACH parsed GENERATE %s; -- needed to extract the individual fields of the tuples" % (field_extractor))
    pig_statements.append("with_unique_keys = rank parsed_val;" )#prepends row IDs to each row 
    pig_statements.append("STORE with_unique_keys INTO 'hbase://$OUTPUT' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (hbase_constructor_args))
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
