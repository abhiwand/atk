import sys
import os
from org.apache.pig.scripting import Pig
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.etl import pig_helpers

def main(argv):
    parser = ArgumentParser(description='imports a big CSV dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-d', '--delimeter', dest='delimeter_char', help='delimeter to use while parsing the input file. Default value is comma')
    parser.add_argument('-f', '--features', dest='feature_names', help='name of the features as a comma separated string')
    parser.add_argument('-t', '--feature_types', dest='feature_types', help='type of the features as a comma separated string')
    parser.add_argument('-k', '--skip_header', dest='skip_header', help='skip the header line (first line) of the CSV file while loading', action='store_true', default=False)

    cmd_line_args = parser.parse_args()
    print cmd_line_args
    
    delimeter_char = ','#by default this script parses comma separated files
    if cmd_line_args.delimeter_char:
        delimeter_char = cmd_line_args.delimeter_char
    
    pig_schema_info = pig_helpers.get_pig_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)
    
    pig_statements = []
    pig_statements.append("REGISTER %s/contrib/piggybank/java/piggybank.jar;" % (os.environ.get('PIG_HOME')))#Pig binary sets the PIG_HOME env. variable when we run the script
    if cmd_line_args.skip_header:
        pig_statements.append("logs = LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage('%s', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (%s);" % (cmd_line_args.input, delimeter_char, pig_schema_info))
    else:
        pig_statements.append("logs = LOAD '%s' USING org.apache.pig.piggybank.storage.CSVExcelStorage('%s') AS (%s);" % (cmd_line_args.input, delimeter_char, pig_schema_info))
    pig_statements.append("with_unique_keys = rank logs;" )#prepends row IDs to each row 
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
