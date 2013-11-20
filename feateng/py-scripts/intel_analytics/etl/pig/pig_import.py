import sys
from org.apache.pig.scripting import Pig
from intel_analytics.etl.argparse_lib import ArgumentParser# pig supports jython (python 2.5) and so the argparse module is not there, that's why we import this open source module, which is the argparse module itself in the std python lib after v2.7
from intel_analytics.etl import pig_helpers

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
    hbase_constructor_args = pig_helpers.get_hbase_storage_schema_string(cmd_line_args.feature_names, cmd_line_args.feature_types)

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
