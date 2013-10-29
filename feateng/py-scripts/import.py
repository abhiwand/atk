import sys
import subprocess
import os
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.argparse_lib import ArgumentParser
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.schema import ETLSchema

base_script_path = os.path.dirname(os.path.abspath(__file__))

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-l', '--idelimeter', dest='input_delimeter_char', help='delimeter to use while parsing the input file', required=True)
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-c', '--custom_parser', dest='custom_parser', help='custom parser to use while parsing the raw input', required=True)
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information', required=True)

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    etl_schema = ETLSchema()
    etl_schema.populate_schema(cmd_line_args.schema_information)
    etl_schema.save_schema(cmd_line_args.output)
    
    feature_names_as_str = ",".join(etl_schema.feature_names)
    feature_types_as_str = ",".join(etl_schema.feature_types)
    
    # need to delete/create output table so that we can write the transformed features
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        hbase_client.drop_create_table(cmd_line_args.output , [CONFIG_PARAMS['etl-column-family']])        
    
    import_script_path = os.path.join(base_script_path, 'intel_analytics', 'etl', 'pig', 'pig_import.py')
       
    subprocess.call(['pig', import_script_path, '-l' , cmd_line_args.input_delimeter_char, 
                     '-i', cmd_line_args.input, '-o', cmd_line_args.output,
                     '-c', cmd_line_args.custom_parser, '-f', feature_names_as_str,
                      '-t', feature_types_as_str])

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)
