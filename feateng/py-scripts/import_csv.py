import sys
import subprocess
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.argparse_lib import ArgumentParser
from intel_analytics.etl.config import CONFIG_PARAMS
from intel_analytics.etl.schema import ETLSchema

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-d', '--delimeter', dest='input_delimeter_char', help='delimeter to use while parsing the input file. Default value is comma')
    parser.add_argument('-s', '--schema', dest='schema_information', help='schema information', required=True)
    parser.add_argument('-k', '--skip_header', dest='skip_header', help='skip the header line (first line) of the CSV file while loading', action='store_true', default=False)

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
        
    args = ['pig', 'py-scripts/intel_analytics/etl/pig/pig_import_csv.py','-i', cmd_line_args.input, 
            '-o', cmd_line_args.output, '-f', feature_names_as_str, '-t', feature_types_as_str]
    
    if cmd_line_args.skip_header:  
        args += ['-k']  
        
    if cmd_line_args.input_delimeter_char:  
        args += ['-d', cmd_line_args.input_delimeter_char]          
    
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)