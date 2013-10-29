import sys
import subprocess
import os
from intel_analytics.etl.hbase_client import ETLHBaseClient
from intel_analytics.etl.argparse_lib import ArgumentParser
from intel_analytics.etl.config import CONFIG_PARAMS

base_script_path = os.path.dirname(os.path.abspath(__file__))

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    # need to delete/create output table so that we can write the transformed features
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        hbase_client.drop_create_table(cmd_line_args.output , [CONFIG_PARAMS['etl-column-family']])        
    
    import_json_script_path = os.path.join(base_script_path, 'intel_analytics', 'etl', 'pig', 'pig_import_json.py')
    args = ['pig', import_json_script_path, '-i', cmd_line_args.input, '-o', cmd_line_args.output]
    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)