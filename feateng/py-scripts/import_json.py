import sys
import subprocess
from tribeca_etl.hbase_client import ETLHBaseClient
from tribeca_etl.argparse_lib import ArgumentParser
from tribeca_etl.config import CONFIG_PARAMS

def main(argv):
    parser = ArgumentParser(description='import.py imports a big dataset from HDFS to HBase')
    parser.add_argument('-i', '--input', dest='input', help='the input file path (on HDFS)', required=True)
    parser.add_argument('-o', '--output', dest='output', help='the output able name', required=True)
    parser.add_argument('-p', '--parallelism', dest='degree_of_parallelism', help='degree of parallelism (number of reducers in MR jobs)', required=True)

    cmd_line_args = parser.parse_args()
    print cmd_line_args

    # need to delete/create output table so that we can write the transformed features
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        hbase_client.drop_create_table(cmd_line_args.output , [CONFIG_PARAMS['etl-column-family']])        
        
    args = ['pig', 'py-scripts/tribeca_etl/pig/pig_import_json.py', '-i', cmd_line_args.input, 
            '-o', cmd_line_args.output, '-p', cmd_line_args.degree_of_parallelism]

    subprocess.call(args)

if __name__ == "__main__":
  try:
    rc = main(sys.argv)
    sys.exit(rc)
  except Exception, e:
    print e
    sys.exit(1)