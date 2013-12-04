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
from org.apache.pig.scripting import Pig
from intel_analytics.table.pig import pig_helpers
from intel_analytics.table.pig.argparse_lib import ArgumentParser

def main(argv):
    parser = ArgumentParser(description='cleans a big dataset')
    parser.add_argument('-f', '--feature', dest='feature_to_clean', help='the feature to clean based on missing values')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output file path (on HDFS)', required=True)
    parser.add_argument('-r', '--replace', dest='replacement_value', help='value to replace the missing values for the given feature (available special constants: avg)')
    parser.add_argument('-s', '--strategy', dest='clean_strategy', help='any: if any missing values are present drop that record, all: if all values are missing, drop that record')
    parser.add_argument('-n', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')
    
    cmd_line_args = parser.parse_args()
    
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
            
    features = [(f.strip()) for f in cmd_line_args.feature_names.split(',')]
    feature_types = [(f.strip()) for f in cmd_line_args.feature_types.split(',')]
    
    feature_type_dict = {}
    for i,feature_name in enumerate(features):
        feature_type_dict[feature_name]=feature_types[i]

    #don't forget to add the key we read from hbase, we read from hbase like .... as (key:chararray, ... remaining_features ...), see below
    features.insert(0, 'key')
    feature_type_dict['key'] = 'chararray'
    
    pig_statements = []
    pig_statements.append("parsed_val = LOAD 'hbase://%s' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s', '-loadKey true') as (key:chararray, %s);" \
                          % (cmd_line_args.input, hbase_constructor_args, pig_schema_info))
    pig_statements.append("all_records_group = GROUP parsed_val ALL;")
    
    final_relation_to_store = 'cleaned_data'
    if cmd_line_args.feature_to_clean:
        if feature_type_dict[cmd_line_args.feature_to_clean] == 'chararray' or feature_type_dict[cmd_line_args.feature_to_clean] == 'bytearray':#chararray checks are done using empty strings instead of NULLs
            pig_statements.append("cleaned_data = FILTER parsed_val BY (%s != '');" % (cmd_line_args.feature_to_clean))
            pig_statements.append("null_relation = FILTER parsed_val BY (%s == '');" % (cmd_line_args.feature_to_clean))
        else:
            pig_statements.append("cleaned_data = FILTER parsed_val BY %s is not NULL;" % (cmd_line_args.feature_to_clean))
            pig_statements.append("null_relation = FILTER parsed_val BY %s is NULL;" % (cmd_line_args.feature_to_clean))
    elif cmd_line_args.clean_strategy:
        clean_statement=''
        if cmd_line_args.clean_strategy == 'any':
            for i, feature in enumerate(features):
              if feature == 'key':#skip the hbase row key
                  continue
              if feature_type_dict[feature] == 'chararray' or feature_type_dict[feature] == 'bytearray':
                  clean_statement += "(%s != '')" % (feature)#chararray checks are done using empty strings instead of NULLs
              else:
                  clean_statement += "(%s is not NULL)" % (feature)
              if i != len(features) - 1:
                clean_statement += ' AND ' 
        elif cmd_line_args.clean_strategy == 'all':
            for i, feature in enumerate(features):
              if feature == 'key':#skip the hbase row key
                  continue                
              if feature_type_dict[feature] == 'chararray' or feature_type_dict[feature] == 'bytearray':
                  clean_statement += "(%s == '')" % (feature)#chararray checks are done using empty strings instead of NULLs
              else:
                  clean_statement += "(%s is NULL)" % (feature)
              if i != len(features) - 1:
                clean_statement += ' AND '              
            clean_statement = "NOT (%s)" % (clean_statement)
        pig_statements.append("cleaned_data = FILTER parsed_val BY %s;" % clean_statement)
            
    if cmd_line_args.replacement_value:
        generate_statement = ''
        for i, feature in enumerate(features):
          if cmd_line_args.feature_to_clean == feature:
              if cmd_line_args.replacement_value == 'avg':
                  generate_statement += 'AVG(all_records_group.parsed_val.%s) as average' % (cmd_line_args.feature_to_clean)
              else:
                  generate_statement += "(%s)TRIM('%s') as %s" % (feature_type_dict[cmd_line_args.feature_to_clean], cmd_line_args.replacement_value, cmd_line_args.feature_to_clean)#need to cast TRIM output otherwise there can be type mismatches
          else:
              generate_statement += "$%d" % i
          if i != len(features) - 1:
              generate_statement += ","
              
        final_relation_to_store = 'replaced_cleaned'
        pig_statements.append("replaced_null_relation = FOREACH null_relation GENERATE %s;" % generate_statement)
        pig_statements.append("replaced_cleaned = UNION replaced_null_relation, cleaned_data;")
    
    pig_statements.append("store %s into 'hbase://$OUTPUT' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('%s');" % (final_relation_to_store, hbase_constructor_args))
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
